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

/// Resolve an ingestor's source to a local file path. Remote sources are
/// downloaded into `cache_dir/<label>.zip`; an existing cache file is reused
/// unless `force_download` is set.
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
            let dest = format!("{cache_dir}/{}.zip", input.label());
            if Path::new(&dest).exists() && !force_download {
                return Ok(dest);
            }
            download(&url, input.headers(), &dest)?;
            Ok(dest)
        }
    }
}

/// Download `url` (with interpolated headers) to `dest` via a temp file + rename.
/// Neither the resolved URL nor header values are logged.
fn download(url: &str, headers: &HashMap<String, String>, dest: &str) -> Result<(), String> {
    let resolved_url = interpolate(url)?;
    let mut req = ureq::get(&resolved_url);
    for (k, v) in headers {
        req = req.set(k, &interpolate(v)?);
    }
    let resp = req
        .call()
        .map_err(|e| format!("download failed for '{dest}': {e}"))?;
    let tmp = format!("{dest}.tmp");
    let mut reader = resp.into_reader();
    let mut file = fs::File::create(&tmp).map_err(|e| format!("failed to create '{tmp}': {e}"))?;
    std::io::copy(&mut reader, &mut file).map_err(|e| format!("failed to write '{tmp}': {e}"))?;
    fs::rename(&tmp, dest).map_err(|e| format!("failed to publish '{dest}': {e}"))?;
    Ok(())
}

/// Stable digest over a GTFS zip's *decompressed* entries (sorted by name),
/// so re-zipping identical content with different packaging yields the same hash.
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

/// Per-feed content hashes persisted next to the cache. Missing/corrupt → empty.
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

/// Wall-clock time of the last successful feed *check* (download + hash),
/// regardless of whether the content changed. Used to cron-gate the startup
/// freshness catch-up. Missing/corrupt → None.
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

/// Sorted list of GTFS input labels that were active when the graph was last built.
/// Missing file → empty vec (triggers rebuild on first run or after cache wipe).
pub fn load_input_labels(cache_dir: &str) -> Vec<String> {
    let path = format!("{cache_dir}/input_labels");
    fs::read_to_string(&path)
        .map(|s| {
            s.lines()
                .filter(|l| !l.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

pub fn save_input_labels(cache_dir: &str, labels: &[String]) -> Result<(), String> {
    fs::create_dir_all(cache_dir)
        .map_err(|e| format!("failed to create cache dir '{cache_dir}': {e}"))?;
    let path = format!("{cache_dir}/input_labels");
    fs::write(&path, labels.join("\n")).map_err(|e| format!("write input_labels: {e}"))
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
