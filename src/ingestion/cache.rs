use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Read;
use std::path::Path;

use chrono::{DateTime, Local};
use sha2::{Digest, Sha256};

use crate::ingestion::secrets::interpolate;
use crate::structures::Ingestor;

#[derive(Debug)]
pub enum SourceLocation {
    Local(String),
    Remote(String),
}

/// Expected on-disk format of a downloaded file, used to reject an endpoint that
/// answered with an HTML error page under HTTP 200 before the bytes poison the
/// cache under a name we would never re-download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MagicKind {
    /// GTFS / address zip: `PK\x03\x04`.
    Zip,
    /// OSM PBF: the `OSMHeader` literal appears within the leading blob.
    OsmPbf,
    /// DEM GeoTIFF: `II*\0` (little-endian) or `MM\0*` (big-endian).
    GeoTiff,
    /// No known signature; the magic check is skipped.
    Any,
}

impl MagicKind {
    pub fn of(input: &Ingestor) -> MagicKind {
        match input {
            Ingestor::OsmPbf(_) => MagicKind::OsmPbf,
            Ingestor::GtfsGeneric(_) | Ingestor::GtfsStib(_) | Ingestor::GtfsSncb(_) => {
                MagicKind::Zip
            }
            Ingestor::AddressBestAdd(_) => MagicKind::Zip,
            Ingestor::DemBelgianLambert2008(_) => MagicKind::GeoTiff,
        }
    }

    fn describe(self) -> &'static str {
        match self {
            MagicKind::Zip => "a zip archive",
            MagicKind::OsmPbf => "an OSM PBF",
            MagicKind::GeoTiff => "a GeoTIFF",
            MagicKind::Any => "the expected format",
        }
    }

    fn matches(self, head: &[u8]) -> bool {
        match self {
            MagicKind::Any => true,
            MagicKind::Zip => head.starts_with(b"PK\x03\x04"),
            MagicKind::GeoTiff => {
                head.starts_with(b"II*\0") || head.starts_with(b"MM\0*")
            }
            // A PBF starts with a 4-byte big-endian blob-header length then a
            // BlobHeader whose `type` field is the literal "OSMHeader", well
            // inside 64 B.
            MagicKind::OsmPbf => head
                .windows(b"OSMHeader".len())
                .take(64)
                .any(|w| w == b"OSMHeader"),
        }
    }
}

/// Callers MUST delete the temp file on mismatch (never publish it to cache).
fn validate_magic(head: &[u8], kind: MagicKind, label: &str) -> Result<(), String> {
    if kind.matches(head) {
        return Ok(());
    }
    Err(format!(
        "download for '{label}' is not {} (got {} leading bytes {:02x?}); the endpoint \
         likely returned an HTML error/redirect page under HTTP 200 rather than the file. \
         Check the URL/credentials; nothing was cached.",
        kind.describe(),
        head.len(),
        &head[..head.len().min(8)],
    ))
}

/// An existing cache file is reused unless `force_download` is set.
pub fn resolve_source(
    input: &Ingestor,
    cache_dir: &str,
    force_download: bool,
) -> Result<String, String> {
    match input.location()? {
        SourceLocation::Local(path) => Ok(path),
        SourceLocation::Remote(url) => {
            fs::create_dir_all(cache_dir)
                .map_err(|e| format!("failed to create cache dir '{cache_dir}': {e}"))?;
            let dest = format!("{cache_dir}/{}", input.cache_filename());
            if Path::new(&dest).exists() && !force_download {
                return Ok(dest);
            }
            download(&url, input.label(), input.headers(), &dest, MagicKind::of(input))?;
            Ok(dest)
        }
    }
}

const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

pub fn download_to(
    url: &str,
    headers: &HashMap<String, String>,
    dest: &str,
) -> Result<(), String> {
    if let Some(parent) = Path::new(dest).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create cache dir '{}': {e}", parent.display()))?;
    }
    let label = Path::new(dest)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(dest);
    download(url, label, headers, dest, MagicKind::Zip)
}

/// A ureq error's Display can embed the RESOLVED (secret-bearing) URL, which must
/// never be logged.
fn redact_ureq_error(e: &ureq::Error) -> String {
    redact_url_in(&e.to_string())
}

/// Drop everything from the first `http(s)://` token onward so a resolved
/// (secret-bearing) URL never reaches the log.
fn redact_url_in(s: &str) -> String {
    match s.find("http://").or_else(|| s.find("https://")) {
        Some(i) => format!("{}<url redacted>", &s[..i]),
        None => s.to_string(),
    }
}

/// Temp file + rename, validating against `magic` BEFORE the atomic publish so an
/// HTML/error page never poisons the cache. Neither the resolved URL nor header
/// values (nor a leaked URL in a ureq error) are ever logged.
fn download(
    url: &str,
    label: &str,
    headers: &HashMap<String, String>,
    dest: &str,
    magic: MagicKind,
) -> Result<(), String> {
    let resolved_url = interpolate(url)?;
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(DOWNLOAD_TIMEOUT)
        .timeout_read(DOWNLOAD_TIMEOUT)
        .build();
    let mut req = agent.get(&resolved_url);
    for (k, v) in headers {
        req = req.set(k, &interpolate(v)?);
    }
    tracing::info!("downloading '{label}' ({}) -> {dest}", redact_url(url));
    let resp = req
        .call()
        .map_err(|e| format!("download failed for '{label}': {}", redact_ureq_error(&e)))?;
    let tmp = format!("{dest}.tmp");
    let mut reader = resp.into_reader();
    let mut file = fs::File::create(&tmp).map_err(|e| format!("failed to create '{tmp}': {e}"))?;
    std::io::copy(&mut reader, &mut file).map_err(|e| format!("failed to write '{tmp}': {e}"))?;

    if let Err(e) = validate_downloaded_magic(&tmp, magic, label) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }

    fs::rename(&tmp, dest).map_err(|e| format!("failed to publish '{dest}': {e}"))?;
    tracing::info!("downloaded '{label}' -> {dest}");
    Ok(())
}

/// Strip the query string so a literal `?key=...` (or a `${VAR}` inside one) never
/// reaches the log.
fn redact_url(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _)) => format!("{base}?<redacted>"),
        None => url.to_string(),
    }
}

fn validate_downloaded_magic(tmp: &str, magic: MagicKind, label: &str) -> Result<(), String> {
    if magic == MagicKind::Any {
        return Ok(());
    }
    let mut f = fs::File::open(tmp).map_err(|e| format!("failed to reopen '{tmp}': {e}"))?;
    let mut head = [0u8; 64];
    let n = f
        .read(&mut head)
        .map_err(|e| format!("failed to read '{tmp}': {e}"))?;
    validate_magic(&head[..n], magic, label)
}

/// Callers MUST pass the PRE-interpolation url form so `${VAR}`/`${file:}` secrets
/// never enter the cache path.
pub fn short_hash(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    let digest = hasher.finalize();
    format!("{:x}", digest)[..8].to_string()
}

/// Digest over the zip's *decompressed* entries sorted by name, so re-zipping
/// identical content with different packaging yields the same hash.
pub fn gtfs_content_hash(zip_path: &str) -> Result<String, String> {
    let file = fs::File::open(zip_path).map_err(|e| format!("failed to open '{zip_path}': {e}"))?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| format!("failed to read zip '{zip_path}': {e}"))?;

    let mut names: Vec<String> = (0..archive.len())
        .filter_map(|i| {
            let f = archive.by_index(i).ok()?;
            if f.is_dir() {
                None
            } else {
                Some(f.name().to_string())
            }
        })
        .collect();
    names.sort();

    let mut hasher = Sha256::new();
    for name in &names {
        let mut entry = archive
            .by_name(name)
            .map_err(|e| format!("failed to read entry '{name}': {e}"))?;
        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .map_err(|e| format!("failed to decompress '{name}': {e}"))?;
        hasher.update(name.as_bytes());
        hasher.update([0u8]);
        hasher.update(&buf);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn load_feed_hashes(cache_dir: &str) -> BTreeMap<String, String> {
    let path = format!("{cache_dir}/feeds.yml");
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_yaml_ng::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_feed_hashes(cache_dir: &str, hashes: &BTreeMap<String, String>) -> Result<(), String> {
    fs::create_dir_all(cache_dir)
        .map_err(|e| format!("failed to create cache dir '{cache_dir}': {e}"))?;
    let path = format!("{cache_dir}/feeds.yml");
    let s = serde_yaml_ng::to_string(hashes).map_err(|e| format!("serialize feed hashes: {e}"))?;
    fs::write(&path, s).map_err(|e| format!("write feed hashes: {e}"))
}

/// Time of the last feed *check* (download + hash), regardless of whether the
/// content changed. Cron-gates the startup freshness catch-up.
pub fn load_last_checked(cache_dir: &str) -> Option<DateTime<Local>> {
    let path = format!("{cache_dir}/last_checked");
    let s = fs::read_to_string(&path).ok()?;
    DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|dt| dt.with_timezone(&Local))
}

pub fn save_last_checked(cache_dir: &str, when: DateTime<Local>) -> Result<(), String> {
    fs::create_dir_all(cache_dir)
        .map_err(|e| format!("failed to create cache dir '{cache_dir}': {e}"))?;
    let path = format!("{cache_dir}/last_checked");
    fs::write(&path, when.to_rfc3339()).map_err(|e| format!("write last_checked: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    fn make_zip(path: &std::path::Path, entries: &[(&str, &str)]) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = SimpleFileOptions::default();
        for (name, content) in entries {
            zip.start_file(*name, opts).unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }

    #[test]
    fn content_hash_ignores_entry_order() {
        let dir = std::env::temp_dir();
        let a = dir.join("maas_hash_a.zip");
        let b = dir.join("maas_hash_b.zip");
        make_zip(&a, &[("stops.txt", "X"), ("routes.txt", "Y")]);
        make_zip(&b, &[("routes.txt", "Y"), ("stops.txt", "X")]);
        assert_eq!(
            gtfs_content_hash(a.to_str().unwrap()).unwrap(),
            gtfs_content_hash(b.to_str().unwrap()).unwrap()
        );
    }

    #[test]
    fn content_hash_changes_with_content() {
        let dir = std::env::temp_dir();
        let a = dir.join("maas_hash_c.zip");
        let b = dir.join("maas_hash_d.zip");
        make_zip(&a, &[("stops.txt", "X")]);
        make_zip(&b, &[("stops.txt", "Z")]);
        assert_ne!(
            gtfs_content_hash(a.to_str().unwrap()).unwrap(),
            gtfs_content_hash(b.to_str().unwrap()).unwrap()
        );
    }

    #[test]
    fn last_checked_round_trip() {
        let dir = std::env::temp_dir().join("maas_last_checked_test");
        std::fs::create_dir_all(&dir).unwrap();
        let cache = dir.to_str().unwrap();
        let _ = std::fs::remove_file(format!("{cache}/last_checked"));
        assert!(load_last_checked(cache).is_none());

        let now = Local::now();
        save_last_checked(cache, now).unwrap();
        let loaded = load_last_checked(cache).unwrap();
        assert_eq!(loaded.timestamp(), now.timestamp());
    }

    #[test]
    fn magic_rejects_html_error_page() {
        let html = b"<!DOCTYPE html><html><body>404 Not Found</body></html>";
        let err = validate_magic(html, MagicKind::Zip, "luxembourg").unwrap_err();
        assert!(err.contains("luxembourg"), "error names the source: {err}");
        assert!(err.contains("HTML"), "error hints at an HTML page: {err}");
    }

    #[test]
    fn magic_accepts_zip_signature() {
        let zip = b"PK\x03\x04rest-of-zip";
        assert!(validate_magic(zip, MagicKind::Zip, "gtfs").is_ok());
    }

    #[test]
    fn magic_rejects_zip_for_non_zip_bytes() {
        assert!(validate_magic(b"not a zip", MagicKind::Zip, "gtfs").is_err());
    }

    #[test]
    fn magic_accepts_osm_pbf_header() {
        // 4-byte big-endian length, then the "OSMHeader" literal.
        let pbf = b"\x00\x00\x00\x0e\x0a\x09OSMHeader\x18";
        assert!(validate_magic(pbf, MagicKind::OsmPbf, "osm").is_ok());
    }

    #[test]
    fn magic_rejects_pbf_for_html() {
        let html = b"<!DOCTYPE html><html>error</html>";
        assert!(validate_magic(html, MagicKind::OsmPbf, "osm").is_err());
    }

    #[test]
    fn magic_accepts_geotiff_both_endian() {
        assert!(validate_magic(b"II*\0....", MagicKind::GeoTiff, "dem").is_ok());
        assert!(validate_magic(b"MM\0*....", MagicKind::GeoTiff, "dem").is_ok());
    }

    #[test]
    fn magic_rejects_geotiff_for_html() {
        assert!(validate_magic(b"<html>", MagicKind::GeoTiff, "dem").is_err());
    }

    #[test]
    fn magic_any_passes_anything() {
        assert!(validate_magic(b"<html>", MagicKind::Any, "x").is_ok());
    }

    #[test]
    fn redact_url_in_strips_resolved_url_with_key() {
        let leaked =
            "Failed to connect to https://data.example/gtfs.zip?apiKey=SECRET123: timed out";
        let s = redact_url_in(leaked);
        assert!(!s.contains("SECRET123"), "the key must not survive: {s}");
        assert!(!s.contains("https://"), "no raw URL: {s}");
        assert!(s.starts_with("Failed to connect to "), "keeps the prefix: {s}");
    }

    #[test]
    fn redact_url_in_passes_url_free_error() {
        assert_eq!(redact_url_in("connection reset"), "connection reset");
    }

    #[test]
    fn redact_url_drops_query_string() {
        assert_eq!(
            redact_url("https://data.example/gtfs.zip?apiKey=SECRET"),
            "https://data.example/gtfs.zip?<redacted>"
        );
        assert_eq!(
            redact_url("https://data.example/gtfs.zip"),
            "https://data.example/gtfs.zip"
        );
    }

    #[test]
    fn feed_hashes_round_trip() {
        let dir = std::env::temp_dir().join("maas_feeds_test");
        std::fs::create_dir_all(&dir).unwrap();
        let cache = dir.to_str().unwrap();
        let mut map = std::collections::BTreeMap::new();
        map.insert("stib".to_string(), "abc".to_string());
        save_feed_hashes(cache, &map).unwrap();
        assert_eq!(load_feed_hashes(cache), map);
    }
}
