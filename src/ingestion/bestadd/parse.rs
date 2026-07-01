use std::collections::HashMap;
use std::io::{BufReader, Cursor, Read};

use quick_xml::Reader;
use quick_xml::events::{BytesText, Event};

use crate::structures::{AddressIndex, AddressIndexBuilder, Named};

use super::convert::Lambert72Converter;

/// Decode an XML text node and resolve entity references into an owned `String`.
fn decode_text(e: &BytesText) -> Result<String, String> {
    let decoded = e.decode().map_err(|err| format!("decode: {err}"))?;
    let unescaped =
        quick_xml::escape::unescape(&decoded).map_err(|err| format!("unescape: {err}"))?;
    Ok(unescaped.into_owned())
}

/// Local (namespace-stripped) name of an XML element as a `String`.
fn local_name(raw: &[u8]) -> String {
    let name = match raw.iter().rposition(|&b| b == b':') {
        Some(i) => &raw[i + 1..],
        None => raw,
    };
    String::from_utf8_lossy(name).into_owned()
}

/// Composite lookup key joining a BeST `namespace` (region) with an
/// `objectIdentifier`. BeST ids are only unique within a region's namespace, so
/// joins must key on the pair to avoid cross-region collisions.
fn composite_key(namespace: &str, id: &str) -> String {
    let mut key = String::with_capacity(namespace.len() + 1 + id.len());
    key.push_str(namespace);
    key.push('|');
    key.push_str(id);
    key
}

/// Pick a single display spelling from the per-language aliases, preferring
/// French, then Dutch, then German, then whatever is present. BeST records carry
/// `nl`/`fr`/`de` spellings of the same entity; the display is cosmetic (the label)
/// while every spelling is interned as a searchable alias.
fn display_of(by_lang: &HashMap<String, String>, order: &[&str]) -> String {
    for lang in order {
        if let Some(v) = by_lang.get(*lang)
            && !v.is_empty()
        {
            return v.clone();
        }
    }
    by_lang.values().find(|v| !v.is_empty()).cloned().unwrap_or_default()
}

/// Parse a BeST `streetName` / `municipality` lookup XML into `id → Named`.
/// `record_tag` is the repeating element's local name (e.g. `streetName`). The id
/// is the `objectIdentifier` whose parent is the `code` block; every
/// `language`/`spelling` pair inside a `name` block adds a searchable alias.
fn parse_named_lookup(xml: &[u8], record_tag: &str) -> Result<HashMap<String, Named>, String> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut stack: Vec<String> = Vec::new();

    let mut out: HashMap<String, Named> = HashMap::new();
    let mut in_record = false;
    let mut cur_id: Option<String> = None;
    let mut cur_ns: Option<String> = None;
    let mut cur_lang: Option<String> = None;
    let mut by_lang: HashMap<String, String> = HashMap::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => return Err(format!("XML parse error in {record_tag}: {e}")),
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                if name == record_tag {
                    in_record = true;
                    cur_id = None;
                    cur_ns = None;
                    cur_lang = None;
                    by_lang.clear();
                }
                stack.push(name);
            }
            Ok(Event::Text(e)) if in_record => {
                let text = decode_text(&e)
                    .map_err(|err| format!("XML text error in {record_tag}: {err}"))?;
                let top = stack.last().map(|s| s.as_str());
                let parent = stack.iter().rev().nth(1).map(|s| s.as_str());
                match (top, parent) {
                    (Some("objectIdentifier"), Some("code")) => cur_id = Some(text),
                    (Some("namespace"), Some("code")) => cur_ns = Some(text),
                    (Some("language"), _) => cur_lang = Some(text),
                    (Some("spelling"), _) => {
                        let lang = cur_lang.clone().unwrap_or_default();
                        by_lang.entry(lang).or_insert(text);
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                stack.pop();
                let name = local_name(e.name().as_ref());
                if name == record_tag {
                    if let Some(id) = cur_id.take() {
                        let mut aliases: Vec<String> =
                            by_lang.values().filter(|v| !v.is_empty()).cloned().collect();
                        aliases.sort();
                        aliases.dedup();
                        let display = display_of(&by_lang, &["fr", "nl", "de"]);
                        let ns = cur_ns.take().unwrap_or_default();
                        out.insert(composite_key(&ns, &id), Named { display, aliases });
                    }
                    in_record = false;
                    by_lang.clear();
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

/// Parse a BeST `postalInfo` lookup into `id → postcode`. In BeST the postal
/// `objectIdentifier` (the id under the `code` block) is the numeric postcode
/// itself, so it doubles as the stored code.
fn parse_postal_lookup(xml: &[u8]) -> Result<HashMap<String, String>, String> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut stack: Vec<String> = Vec::new();

    let mut out: HashMap<String, String> = HashMap::new();
    let mut in_record = false;
    let mut cur_id: Option<String> = None;
    let mut cur_ns: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => return Err(format!("XML parse error in postalInfo: {e}")),
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                if name == "postalInfo" {
                    in_record = true;
                    cur_id = None;
                    cur_ns = None;
                }
                stack.push(name);
            }
            Ok(Event::Text(e)) if in_record => {
                let text = decode_text(&e)
                    .map_err(|err| format!("XML text error in postalInfo: {err}"))?;
                let top = stack.last().map(|s| s.as_str());
                let parent = stack.iter().rev().nth(1).map(|s| s.as_str());
                match (top, parent) {
                    (Some("objectIdentifier"), Some("code")) => cur_id = Some(text),
                    (Some("namespace"), Some("code")) => cur_ns = Some(text),
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                stack.pop();
                if local_name(e.name().as_ref()) == "postalInfo" {
                    if let Some(id) = cur_id.take() {
                        let ns = cur_ns.take().unwrap_or_default();
                        out.insert(composite_key(&ns, &id), id);
                    }
                    in_record = false;
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

/// Stream a BeST `address` XML into the index builder, joining each address to the
/// pre-parsed street / municipality / postal lookup tables by FK id and converting
/// its Lambert72 position to WGS84. The reader is consumed incrementally with
/// buffer reuse so memory stays flat across the millions of addresses in the large
/// regional files. Records whose street FK is unknown are skipped (cannot be
/// labelled).
fn stream_addresses<R: std::io::BufRead>(
    reader: R,
    streets: &HashMap<String, Named>,
    munis: &HashMap<String, Named>,
    postals: &HashMap<String, String>,
    conv: &Lambert72Converter,
    builder: &mut AddressIndexBuilder,
) -> Result<(), String> {
    let mut reader = Reader::from_reader(reader);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut stack: Vec<String> = Vec::new();

    let mut in_record = false;
    let mut id = String::new();
    let mut street_fk = String::new();
    let mut street_ns = String::new();
    let mut muni_fk = String::new();
    let mut muni_ns = String::new();
    let mut postal_fk = String::new();
    let mut postal_ns = String::new();
    let mut house = String::new();
    let mut boxn = String::new();
    let mut pos = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => return Err(format!("XML parse error in address: {e}")),
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                if name == "address" {
                    in_record = true;
                    id.clear();
                    street_fk.clear();
                    street_ns.clear();
                    muni_fk.clear();
                    muni_ns.clear();
                    postal_fk.clear();
                    postal_ns.clear();
                    house.clear();
                    boxn.clear();
                    pos.clear();
                }
                stack.push(name);
            }
            Ok(Event::Text(e)) if in_record => {
                let text =
                    decode_text(&e).map_err(|err| format!("XML text error in address: {err}"))?;
                let top = stack.last().map(|s| s.as_str());
                let parent = stack.iter().rev().nth(1).map(|s| s.as_str());
                match (top, parent) {
                    (Some("objectIdentifier"), Some("code")) => id = text,
                    (Some("objectIdentifier"), Some("hasStreetName")) => street_fk = text,
                    (Some("namespace"), Some("hasStreetName")) => street_ns = text,
                    (Some("objectIdentifier"), Some("hasMunicipality")) => muni_fk = text,
                    (Some("namespace"), Some("hasMunicipality")) => muni_ns = text,
                    (Some("objectIdentifier"), Some("hasPostalInfo")) => postal_fk = text,
                    (Some("namespace"), Some("hasPostalInfo")) => postal_ns = text,
                    (Some("houseNumber"), _) => house = text,
                    (Some("boxNumber"), _) => boxn = text,
                    (Some("pos"), _) => pos = text,
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                stack.pop();
                if local_name(e.name().as_ref()) == "address" {
                    in_record = false;
                    let street_key = composite_key(&street_ns, &street_fk);
                    let Some(street_named) = streets.get(&street_key) else {
                        continue;
                    };
                    let mut coords = pos.split_whitespace();
                    let (Some(xs), Some(ys)) = (coords.next(), coords.next()) else {
                        continue;
                    };
                    let (Ok(x), Ok(y)) = (xs.parse::<f64>(), ys.parse::<f64>()) else {
                        continue;
                    };
                    let (lat, lon) = match conv.to_wgs84(x, y) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let sid = builder.intern_street(&street_key, street_named.clone());
                    let muni_key = composite_key(&muni_ns, &muni_fk);
                    let muni_named = munis.get(&muni_key).cloned().unwrap_or_default();
                    let mid = builder.intern_municipality(&muni_key, muni_named);
                    let postal_key = composite_key(&postal_ns, &postal_fk);
                    let code = postals.get(&postal_key).cloned().unwrap_or_default();
                    let pid = builder.intern_postal(&postal_key, code);
                    builder.push_record(
                        std::mem::take(&mut id),
                        sid,
                        mid,
                        pid,
                        std::mem::take(&mut house),
                        std::mem::take(&mut boxn),
                        lat,
                        lon,
                    );
                }
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

/// BeST file kind, classified from the OUTER zip entry name. The real FULL feed is
/// a zip-of-zips: each top-level entry is a per-region/type `.zip` whose single
/// `.xml` member carries the records.
enum Kind {
    Street,
    Municipality,
    Postal,
    Address,
}

/// Classify a top-level entry of the FULL feed by case-insensitive substring of its
/// name. Only `.zip` members are considered; the license `.docx` and the Wallonia
/// `PartOfMunicipality` zip are ignored.
fn classify(name: &str) -> Option<Kind> {
    let lower = name.to_ascii_lowercase();
    if !lower.ends_with(".zip") || lower.contains("partofmunicipality") {
        return None;
    }
    if lower.contains("streetname") {
        Some(Kind::Street)
    } else if lower.contains("municipality") {
        Some(Kind::Municipality)
    } else if lower.contains("postalinfo") || lower.contains("postal") {
        Some(Kind::Postal)
    } else if lower.contains("address") {
        Some(Kind::Address)
    } else {
        None
    }
}

fn read_entry(archive: &mut zip::ZipArchive<std::fs::File>, idx: usize) -> Result<Vec<u8>, String> {
    let mut f = archive
        .by_index(idx)
        .map_err(|e| format!("failed to read zip entry {idx}: {e}"))?;
    let mut bytes = Vec::with_capacity(f.size() as usize);
    f.read_to_end(&mut bytes)
        .map_err(|e| format!("failed to decompress entry: {e}"))?;
    Ok(bytes)
}

/// Index of the single `.xml` member inside a nested region/type zip.
fn xml_member_index<R: Read + std::io::Seek>(
    inner: &mut zip::ZipArchive<R>,
) -> Result<usize, String> {
    (0..inner.len())
        .find(|&i| {
            inner
                .by_index(i)
                .ok()
                .map(|f| f.name().to_ascii_lowercase().ends_with(".xml"))
                .unwrap_or(false)
        })
        .ok_or_else(|| "nested zip has no .xml member".to_string())
}

/// Read the single `.xml` member of a nested region/type zip into memory. Used for
/// the small lookup files (street / municipality / postal).
fn nested_xml_bytes(outer_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut inner = zip::ZipArchive::new(Cursor::new(outer_bytes))
        .map_err(|e| format!("failed to read nested zip: {e}"))?;
    let idx = xml_member_index(&mut inner)?;
    let mut f = inner
        .by_index(idx)
        .map_err(|e| format!("failed to read nested xml: {e}"))?;
    let mut bytes = Vec::with_capacity(f.size() as usize);
    f.read_to_end(&mut bytes)
        .map_err(|e| format!("failed to decompress nested xml: {e}"))?;
    Ok(bytes)
}

/// Parse a BeST-Add FULL XML zip into a ready-to-query [`AddressIndex`], keeping
/// only the top-level entries whose name satisfies `keep`. The FULL feed is a
/// zip-of-zips: every matching outer entry is opened as a nested zip and its single
/// `.xml` member parsed. Lookups (streets / municipalities / postals) are parsed
/// first across all matching files, then every `Address` member is streamed and
/// FK-joined. Multiple regional files of the same kind accumulate into the shared
/// tables.
fn load_bestadd_zip_filtered(
    zip_path: &str,
    box_coord_epsilon_m: f64,
    keep: impl Fn(&str) -> bool,
) -> Result<AddressIndex, String> {
    let conv = Lambert72Converter::new()?;
    let file =
        std::fs::File::open(zip_path).map_err(|e| format!("failed to open '{zip_path}': {e}"))?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| format!("failed to read zip '{zip_path}': {e}"))?;

    let names: Vec<(usize, String)> = (0..archive.len())
        .filter_map(|i| {
            let f = archive.by_index(i).ok()?;
            if f.is_dir() {
                None
            } else {
                Some((i, f.name().to_string()))
            }
        })
        .filter(|(_, name)| keep(name))
        .collect();

    let mut streets: HashMap<String, Named> = HashMap::new();
    let mut munis: HashMap<String, Named> = HashMap::new();
    let mut postals: HashMap<String, String> = HashMap::new();
    let mut address_indices: Vec<usize> = Vec::new();

    for (i, name) in &names {
        match classify(name) {
            Some(Kind::Street) => {
                let outer = read_entry(&mut archive, *i)?;
                let xml = nested_xml_bytes(&outer)?;
                streets.extend(parse_named_lookup(&xml, "streetName")?);
            }
            Some(Kind::Municipality) => {
                let outer = read_entry(&mut archive, *i)?;
                let xml = nested_xml_bytes(&outer)?;
                munis.extend(parse_named_lookup(&xml, "municipality")?);
            }
            Some(Kind::Postal) => {
                let outer = read_entry(&mut archive, *i)?;
                let xml = nested_xml_bytes(&outer)?;
                postals.extend(parse_postal_lookup(&xml)?);
            }
            Some(Kind::Address) => address_indices.push(*i),
            None => {}
        }
    }

    let mut builder = AddressIndexBuilder::new();
    builder.set_box_coord_epsilon_m(box_coord_epsilon_m);
    for i in address_indices {
        let outer = read_entry(&mut archive, i)?;
        let mut inner = zip::ZipArchive::new(Cursor::new(outer.as_slice()))
            .map_err(|e| format!("failed to read nested address zip: {e}"))?;
        let idx = xml_member_index(&mut inner)?;
        let entry = inner
            .by_index(idx)
            .map_err(|e| format!("failed to read nested address xml: {e}"))?;
        stream_addresses(
            BufReader::new(entry),
            &streets,
            &munis,
            &postals,
            &conv,
            &mut builder,
        )?;
    }
    Ok(builder.finish())
}

/// Parse a BeST-Add FULL XML zip into a ready-to-query [`AddressIndex`].
/// `box_coord_epsilon_m` is the representative-coordinate divergence epsilon.
pub fn load_bestadd_zip(zip_path: &str, box_coord_epsilon_m: f64) -> Result<AddressIndex, String> {
    load_bestadd_zip_filtered(zip_path, box_coord_epsilon_m, |_| true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    const STREETS_A: &str = "\u{feff}<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<tns:streetNameResponseBySource xmlns:com=\"http://fsb.belgium.be/data/common\" xmlns:tns=\"http://fsb.belgium.be/mappingservices/FullDownload/v1_00\">
  <tns:streetName>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>S1</com:objectIdentifier><com:versionIdentifier>1</com:versionIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>Rue de la Loi</com:spelling></com:name>
    <com:name><com:language>nl</com:language><com:spelling>Wetstraat</com:spelling></com:name>
    <com:isAssignedByMunicipality><com:namespace>x</com:namespace><com:objectIdentifier>M1</com:objectIdentifier></com:isAssignedByMunicipality>
  </tns:streetName>
</tns:streetNameResponseBySource>";

    const STREETS_B: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:streetNameResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:streetName>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>S2</com:objectIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>Avenue Louise</com:spelling></com:name>
    <com:name><com:language>nl</com:language><com:spelling>Louizalaan</com:spelling></com:name>
  </tns:streetName>
</tns:streetNameResponseBySource>"#;

    const MUNIS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:municipalityResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:municipality>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>M1</com:objectIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>Bruxelles</com:spelling></com:name>
    <com:name><com:language>nl</com:language><com:spelling>Brussel</com:spelling></com:name>
  </tns:municipality>
</tns:municipalityResponseBySource>"#;

    const POSTALS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:postalInfoResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:postalInfo>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>1000</com:objectIdentifier></com:code>
  </tns:postalInfo>
</tns:postalInfoResponseBySource>"#;

    const ADDRESSES_A: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:addressResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:address>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>A1</com:objectIdentifier></com:code>
    <com:position><com:pointGeometry><com:point><com:pos srsName="http://www.opengis.net/def/crs/EPSG/0/31370" srsDimension="2">148378.77 172011.96</com:pos></com:point></com:pointGeometry></com:position>
    <com:houseNumber>16</com:houseNumber>
    <com:hasStreetName><com:namespace>x</com:namespace><com:objectIdentifier>S1</com:objectIdentifier></com:hasStreetName>
    <com:hasMunicipality><com:namespace>x</com:namespace><com:objectIdentifier>M1</com:objectIdentifier></com:hasMunicipality>
    <com:hasPostalInfo><com:namespace>x</com:namespace><com:objectIdentifier>1000</com:objectIdentifier></com:hasPostalInfo>
  </tns:address>
  <tns:address>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>A2</com:objectIdentifier></com:code>
    <com:position><com:pointGeometry><com:point><com:pos srsName="http://www.opengis.net/def/crs/EPSG/0/31370">148400.0 172050.0</com:pos></com:point></com:pointGeometry></com:position>
    <com:boxNumber>A</com:boxNumber>
    <com:houseNumber>200</com:houseNumber>
    <com:hasStreetName><com:namespace>x</com:namespace><com:objectIdentifier>S1</com:objectIdentifier></com:hasStreetName>
    <com:hasMunicipality><com:namespace>x</com:namespace><com:objectIdentifier>M1</com:objectIdentifier></com:hasMunicipality>
    <com:hasPostalInfo><com:namespace>x</com:namespace><com:objectIdentifier>1000</com:objectIdentifier></com:hasPostalInfo>
  </tns:address>
</tns:addressResponseBySource>"#;

    const ADDRESSES_B: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:addressResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:address>
    <com:code><com:namespace>x</com:namespace><com:objectIdentifier>A3</com:objectIdentifier></com:code>
    <com:position><com:pointGeometry><com:point><com:pos srsName="http://www.opengis.net/def/crs/EPSG/0/31370">148500.0 172100.0</com:pos></com:point></com:pointGeometry></com:position>
    <com:houseNumber>10</com:houseNumber>
    <com:hasStreetName><com:namespace>x</com:namespace><com:objectIdentifier>S2</com:objectIdentifier></com:hasStreetName>
    <com:hasMunicipality><com:namespace>x</com:namespace><com:objectIdentifier>M1</com:objectIdentifier></com:hasMunicipality>
    <com:hasPostalInfo><com:namespace>x</com:namespace><com:objectIdentifier>1000</com:objectIdentifier></com:hasPostalInfo>
  </tns:address>
</tns:addressResponseBySource>"#;

    /// Wrap one XML string in a nested region/type `.zip`, mirroring the real feed's
    /// zip-of-zips layout, and return the nested zip's bytes.
    fn nested_zip(base: &str, xml: &str) -> Vec<u8> {
        let mut out = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(Cursor::new(&mut out));
            zip.start_file(format!("{base}.xml"), SimpleFileOptions::default())
                .unwrap();
            zip.write_all(xml.as_bytes()).unwrap();
            zip.finish().unwrap();
        }
        out
    }

    /// Build the FULL outer zip (zip-of-zips) with two regions' worth of
    /// streets/addresses plus shared municipality/postal lookups, and a couple of
    /// entries the parser must ignore (license docx, PartOfMunicipality).
    fn write_fixture_zip(path: &std::path::Path) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = SimpleFileOptions::default();
        let entries: [(&str, Vec<u8>); 8] = [
            ("RegionAStreetName.zip", nested_zip("RegionAStreetName", STREETS_A)),
            ("RegionBStreetName.zip", nested_zip("RegionBStreetName", STREETS_B)),
            ("RegionAMunicipality.zip", nested_zip("RegionAMunicipality", MUNIS)),
            ("RegionAPostalInfo.zip", nested_zip("RegionAPostalInfo", POSTALS)),
            ("RegionAAddress.zip", nested_zip("RegionAAddress", ADDRESSES_A)),
            ("RegionBAddress.zip", nested_zip("RegionBAddress", ADDRESSES_B)),
            ("RegionAPartOfMunicipality.zip", nested_zip("RegionAPartOfMunicipality", MUNIS)),
            ("best-ccby-license.docx", b"not a zip".to_vec()),
        ];
        for (name, content) in entries {
            zip.start_file(name, opts).unwrap();
            zip.write_all(&content).unwrap();
        }
        zip.finish().unwrap();
    }

    #[test]
    fn parses_named_lookup_with_two_languages() {
        let map = parse_named_lookup(STREETS_A.as_bytes(), "streetName").unwrap();
        assert_eq!(map.len(), 1);
        let s1 = &map["x|S1"];
        assert_eq!(s1.display, "Rue de la Loi");
        assert!(s1.aliases.contains(&"Wetstraat".to_string()));
        assert!(s1.aliases.contains(&"Rue de la Loi".to_string()));
    }

    #[test]
    fn parses_postal_objectidentifier_as_code() {
        let map = parse_postal_lookup(POSTALS.as_bytes()).unwrap();
        assert_eq!(map["x|1000"], "1000");
    }

    #[test]
    fn full_zip_joins_fks_and_converts_coords() {
        let dir = std::env::temp_dir().join("maas_bestadd_zip_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("best.zip");
        write_fixture_zip(&path);

        let idx = load_bestadd_zip(path.to_str().unwrap(), 5.0).unwrap();
        assert_eq!(idx.record_count(), 3, "three buildings: S1/16, S1/200, S2/10");

        let fr = idx.search("rue de la loi 16", 10, None);
        assert_eq!(fr.len(), 1);
        assert_eq!(fr[0].id, "A1");
        assert_eq!(fr[0].label, "Rue de la Loi 16, 1000 Bruxelles");
        assert!((fr[0].lat - 50.85849524).abs() < 1e-4, "lat {}", fr[0].lat);
        assert!((fr[0].lon - 4.34572624).abs() < 1e-4, "lon {}", fr[0].lon);

        let nl = idx.search("wetstraat 16", 10, None);
        assert_eq!(nl.len(), 1);
        assert_eq!(nl[0].id, fr[0].id);

        let building = idx.search("wetstraat 200", 10, None);
        assert_eq!(building.len(), 1);
        assert_eq!(building[0].id, "A2");
        assert_eq!(
            building[0].label, "Rue de la Loi 200, 1000 Bruxelles",
            "a bare number token is building-level (no bus)"
        );

        let bus = idx.search("wetstraat 200 bus a", 10, None);
        assert_eq!(bus.len(), 1);
        assert_eq!(bus[0].id, "A2");
        assert!(bus[0].label.contains("bus A"), "label {}", bus[0].label);

        let region_b = idx.search("avenue louise 10", 10, None);
        assert_eq!(region_b.len(), 1);
        assert_eq!(region_b[0].id, "A3");
    }

    const STREETS_BRU: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:streetNameResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:streetName>
    <com:code><com:namespace>https://databrussels.be/id/streetname</com:namespace><com:objectIdentifier>4568</com:objectIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>Rue Bru Collision</com:spelling></com:name>
  </tns:streetName>
</tns:streetNameResponseBySource>"#;

    const STREETS_WAL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:streetNameResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:streetName>
    <com:code><com:namespace>https://wallonie.be/id/streetname</com:namespace><com:objectIdentifier>4568</com:objectIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>Rue Wal Collision</com:spelling></com:name>
  </tns:streetName>
</tns:streetNameResponseBySource>"#;

    const MUNIS_BRU: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:municipalityResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:municipality>
    <com:code><com:namespace>https://databrussels.be/id/municipality</com:namespace><com:objectIdentifier>99</com:objectIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>VilleBru</com:spelling></com:name>
  </tns:municipality>
</tns:municipalityResponseBySource>"#;

    const MUNIS_WAL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:municipalityResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:municipality>
    <com:code><com:namespace>https://wallonie.be/id/municipality</com:namespace><com:objectIdentifier>99</com:objectIdentifier></com:code>
    <com:name><com:language>fr</com:language><com:spelling>VilleWal</com:spelling></com:name>
  </tns:municipality>
</tns:municipalityResponseBySource>"#;

    const POSTALS_BRU: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:postalInfoResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:postalInfo>
    <com:code><com:namespace>https://databrussels.be/id/postalinfo</com:namespace><com:objectIdentifier>1070</com:objectIdentifier></com:code>
  </tns:postalInfo>
</tns:postalInfoResponseBySource>"#;

    const POSTALS_WAL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:postalInfoResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:postalInfo>
    <com:code><com:namespace>https://wallonie.be/id/postalinfo</com:namespace><com:objectIdentifier>5000</com:objectIdentifier></com:code>
  </tns:postalInfo>
</tns:postalInfoResponseBySource>"#;

    const ADDR_BRU: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:addressResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:address>
    <com:code><com:namespace>https://databrussels.be/id/address</com:namespace><com:objectIdentifier>ABRU</com:objectIdentifier></com:code>
    <com:position><com:pointGeometry><com:point><com:pos srsName="http://www.opengis.net/def/crs/EPSG/0/31370">148378.77 172011.96</com:pos></com:point></com:pointGeometry></com:position>
    <com:houseNumber>1</com:houseNumber>
    <com:hasStreetName><com:namespace>https://databrussels.be/id/streetname</com:namespace><com:objectIdentifier>4568</com:objectIdentifier></com:hasStreetName>
    <com:hasMunicipality><com:namespace>https://databrussels.be/id/municipality</com:namespace><com:objectIdentifier>99</com:objectIdentifier></com:hasMunicipality>
    <com:hasPostalInfo><com:namespace>https://databrussels.be/id/postalinfo</com:namespace><com:objectIdentifier>1070</com:objectIdentifier></com:hasPostalInfo>
  </tns:address>
</tns:addressResponseBySource>"#;

    const ADDR_WAL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<tns:addressResponseBySource xmlns:com="http://fsb.belgium.be/data/common" xmlns:tns="http://fsb.belgium.be/mappingservices/FullDownload/v1_00">
  <tns:address>
    <com:code><com:namespace>https://wallonie.be/id/address</com:namespace><com:objectIdentifier>AWAL</com:objectIdentifier></com:code>
    <com:position><com:pointGeometry><com:point><com:pos srsName="http://www.opengis.net/def/crs/EPSG/0/31370">148500.0 172100.0</com:pos></com:point></com:pointGeometry></com:position>
    <com:houseNumber>1</com:houseNumber>
    <com:hasStreetName><com:namespace>https://wallonie.be/id/streetname</com:namespace><com:objectIdentifier>4568</com:objectIdentifier></com:hasStreetName>
    <com:hasMunicipality><com:namespace>https://wallonie.be/id/municipality</com:namespace><com:objectIdentifier>99</com:objectIdentifier></com:hasMunicipality>
    <com:hasPostalInfo><com:namespace>https://wallonie.be/id/postalinfo</com:namespace><com:objectIdentifier>5000</com:objectIdentifier></com:hasPostalInfo>
  </tns:address>
</tns:addressResponseBySource>"#;

    fn write_collision_zip(path: &std::path::Path) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = SimpleFileOptions::default();
        let entries: [(&str, Vec<u8>); 8] = [
            ("BrusselsStreetName.zip", nested_zip("BrusselsStreetName", STREETS_BRU)),
            ("WalloniaStreetName.zip", nested_zip("WalloniaStreetName", STREETS_WAL)),
            ("BrusselsMunicipality.zip", nested_zip("BrusselsMunicipality", MUNIS_BRU)),
            ("WalloniaMunicipality.zip", nested_zip("WalloniaMunicipality", MUNIS_WAL)),
            ("BrusselsPostalInfo.zip", nested_zip("BrusselsPostalInfo", POSTALS_BRU)),
            ("WalloniaPostalInfo.zip", nested_zip("WalloniaPostalInfo", POSTALS_WAL)),
            ("BrusselsAddress.zip", nested_zip("BrusselsAddress", ADDR_BRU)),
            ("WalloniaAddress.zip", nested_zip("WalloniaAddress", ADDR_WAL)),
        ];
        for (name, content) in entries {
            zip.start_file(name, opts).unwrap();
            zip.write_all(&content).unwrap();
        }
        zip.finish().unwrap();
    }

    #[test]
    fn cross_region_same_id_does_not_collide() {
        let dir = std::env::temp_dir().join("maas_bestadd_collision_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("best.zip");
        write_collision_zip(&path);

        let idx = load_bestadd_zip(path.to_str().unwrap(), 5.0).unwrap();
        assert_eq!(idx.record_count(), 2);

        let bru = idx.search("rue bru collision 1", 10, None);
        assert_eq!(bru.len(), 1, "Brussels street 4568 must resolve to its own name");
        assert_eq!(bru[0].id, "ABRU");
        assert!(bru[0].label.contains("Rue Bru Collision"), "label {}", bru[0].label);
        assert!(bru[0].label.contains("1070"), "label {}", bru[0].label);
        assert!(bru[0].label.contains("VilleBru"), "label {}", bru[0].label);

        let wal = idx.search("rue wal collision 1", 10, None);
        assert_eq!(wal.len(), 1, "Wallonia street 4568 must resolve to its own name");
        assert_eq!(wal[0].id, "AWAL");
        assert!(wal[0].label.contains("Rue Wal Collision"), "label {}", wal[0].label);
        assert!(wal[0].label.contains("5000"), "label {}", wal[0].label);
        assert!(wal[0].label.contains("VilleWal"), "label {}", wal[0].label);
    }

    #[test]
    #[ignore]
    fn real_brussels_data_parses_into_belgium() {
        let path = "cache/bestadd.zip";
        if !std::path::Path::new(path).exists() {
            eprintln!("skipping: {path} not present");
            return;
        }
        let idx =
            load_bestadd_zip_filtered(path, 5.0, |name| name.starts_with("Brussels")).unwrap();
        eprintln!("Brussels building records: {}", idx.record_count());
        assert!(idx.record_count() > 100_000, "got {}", idx.record_count());

        let hits = idx.search("avenue des dauphinelles", 5, None);
        assert!(!hits.is_empty(), "central street not found");
        let h = &hits[0];
        eprintln!("sample: {} @ {},{}", h.label, h.lat, h.lon);
        assert!((49.4..=51.6).contains(&h.lat), "lat {}", h.lat);
        assert!((2.5..=6.5).contains(&h.lon), "lon {}", h.lon);

        let nl = idx.search("ridderspoorlaan", 5, None);
        assert!(!nl.is_empty(), "dutch alias not found");
    }

    #[test]
    #[ignore]
    fn real_brussels_fuzzy_resolves_misspelled_street() {
        let path = "cache/bestadd.zip";
        if !std::path::Path::new(path).exists() {
            eprintln!("skipping: {path} not present");
            return;
        }
        let idx =
            load_bestadd_zip_filtered(path, 5.0, |name| name.starts_with("Brussels")).unwrap();

        let exact = idx.search("avenue des dauphinelles", 5, None);
        assert!(!exact.is_empty(), "control: exact spelling must resolve");
        let want = exact[0].street.clone();
        eprintln!("control street: {}", want);

        let t0 = std::time::Instant::now();
        let typo = idx.search("avenue des dauphineles", 5, None);
        let elapsed = t0.elapsed();
        eprintln!("fuzzy query took {elapsed:?}, top: {:?}", typo.first().map(|h| &h.label));
        assert!(!typo.is_empty(), "misspelled 'dauphineles' must resolve via fuzzy");
        assert_eq!(typo[0].street, want, "fuzzy must reach the same street");
        assert!(elapsed.as_millis() < 500, "fuzzy latency {elapsed:?} too high");
    }

    #[test]
    #[ignore]
    fn real_brussels_wallonia_no_cross_region_collision() {
        let path = "cache/bestadd.zip";
        if !std::path::Path::new(path).exists() {
            eprintln!("skipping: {path} not present");
            return;
        }
        let t0 = std::time::Instant::now();
        let idx = load_bestadd_zip_filtered(path, 5.0, |name| {
            name.starts_with("Brussels") || name.starts_with("Wallonia")
        })
        .unwrap();
        eprintln!(
            "Brussels+Wallonia records: {} ({:?})",
            idx.record_count(),
            t0.elapsed()
        );

        let bru = idx.search("dauphinelles", 5, None);
        assert!(!bru.is_empty(), "Brussels street 'dauphinelles' not found");
        let b = &bru[0];
        eprintln!("Brussels sample: {} @ {},{}", b.label, b.lat, b.lon);
        assert!(b.label.contains("1070"), "expected postcode 1070, got: {}", b.label);
        assert!(
            b.label.to_lowercase().contains("anderlecht"),
            "expected municipality Anderlecht, got: {}",
            b.label
        );

        let wal = idx.search("rue de fer", 5, None);
        assert!(!wal.is_empty(), "Wallonia street not found");
        eprintln!("Wallonia sample: {}", wal[0].label);
    }

    /// Building aggregation on the real feed: a street query collapses to a single
    /// street-in-municipality entry (not one row per house number), and a house
    /// number collapses its apartment/box rows to one building. Reports the
    /// building-record count for the whole feed so the row→building drop is visible.
    #[test]
    #[ignore]
    fn real_data_collapses_to_buildings() {
        let path = "cache/bestadd.zip";
        if !std::path::Path::new(path).exists() {
            eprintln!("skipping: {path} not present");
            return;
        }
        let t0 = std::time::Instant::now();
        let idx = load_bestadd_zip(path, 5.0).unwrap();
        eprintln!(
            "Belgium building records: {} ({:?})",
            idx.record_count(),
            t0.elapsed()
        );

        let street = idx.search("rue de la loi", 10, None);
        let brussels: Vec<_> = street
            .iter()
            .filter(|h| h.municipality.to_lowercase().contains("bruxelles"))
            .collect();
        eprintln!(
            "'rue de la loi' hits: {} (Brussels: {})",
            street.len(),
            brussels.len()
        );
        for h in &street {
            eprintln!("  {}", h.label);
        }
        assert_eq!(
            brussels.len(),
            1,
            "the street collapses to one Brussels entry, got {brussels:?}"
        );
        assert!(
            brussels[0].house_number.is_empty(),
            "street-level hit carries no house number"
        );

        let building = idx.search("rue de la loi 16", 50, None);
        for h in &building {
            eprintln!("  {}", h.label);
        }
        let bxl_1000: Vec<_> = building
            .iter()
            .filter(|h| h.postcode == "1000" && h.municipality.to_lowercase().contains("bruxelles"))
            .collect();
        assert_eq!(
            bxl_1000.len(),
            1,
            "house number 16 collapses its apartments to one building in 1000 Bruxelles"
        );
        let mut per_muni: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for h in building.iter().filter(|h| h.house_number == "16") {
            *per_muni.entry(h.municipality.as_str()).or_default() += 1;
        }
        assert!(
            per_muni.values().all(|&n| n == 1),
            "apartments collapsed: exactly one house-16 building per municipality, got {per_muni:?}"
        );
    }
}
