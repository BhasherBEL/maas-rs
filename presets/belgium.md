# Belgium preset

The full Belgium engine: STIB/MIVB, SNCB/NMBS, De Lijn and TEC transit, multi-operator fares, realtime, NGI elevation and BeST address search.

Run it:

```bash
cargo run --release -- --config presets/belgium.yaml --serve
```

Remote feeds (STIB, SNCB, De Lijn, TEC, addresses) are auto-downloaded and cached on first build; the two `path:data/…` inputs (the OSM extract and the DEM) are files you fetch and place yourself. The preset writes to distinct paths (`graph.belgium.bin`, `osm.belgium.bin`, `address.belgium.bin`, `cache/belgium`) so it never clobbers the generic caches.

## Data sources

### OSM (required)

The street network comes from the Belgium [Geofabrik](https://download.geofabrik.de/) extract, referenced as `path:data/belgium-latest.osm.pbf`:

```bash
mkdir -p data
curl -L -o data/belgium-latest.osm.pbf https://download.geofabrik.de/europe/belgium-latest.osm.pbf
```

### BMC transit key (for the Belgian transit and realtime feeds)

STIB, SNCB and De Lijn (static GTFS and realtime) are served through the BeMobility gateway (BMC). The feed headers reference `${BMC_PARTNER_KEY}`, read from the environment:

```bash
export BMC_PARTNER_KEY=your-key-here
```

The gateway has an anonymous tier (100 requests/day, 10 requests/minute) that is enough to try the engine without realtime, so a registered key is only needed for heavier or realtime use. A registered key raises the quota to roughly 8 requests/minute and 12,000 requests/day, and is per-user (it cannot be shared across deployments; the realtime poller is tuned to stay under that budget). Register at the [BeMobility open-data gateway](https://api-management-opendata-production.developer.azure-api.net/).

The engine does not otherwise require the key: point the STIB/SNCB/De Lijn inputs at keyless feeds, or drop them, and Belgium builds and routes with no key. Realtime is separately optional (`realtime.enabled: false` builds and routes on the static schedule only).

### TEC / Wallonia (keyless, auto-downloaded)

Wallonia bus/tram coverage comes from the OTW (Opérateur de Transport de Wallonie) public open-data GTFS export, referenced directly as a remote feed:

```
url: https://opendata.tec-wl.be/Current%20GTFS/TEC-GTFS.zip
```

Like the other remote feeds it is auto-downloaded and cached on first build (no key, no manual placement). The export's agencies are the five regional brands (`TEC Brabant Wallon`, `TEC Charleroi`, `TEC Hainaut`, `TEC Liège - Verviers`, `TEC Namur - Luxembourg`); each carries the `TEC` token, so the `TEC` fare operator matches them all and TEC legs price live. Published as OTW open data (free reuse); see the [TEC Open Data](https://www.letec.be/View/Open_Data_of_TEC/4296) and [Wallonia open-data portal](https://opendata.digitalwallonia.be/) terms.

### Elevation (optional)

`data/belgium-DTM-20m.tif` is Belgium's NGI 20 m Digital Terrain Model (EPSG:3812, Belgian Lambert 2008), consumed by the `dem/belgian-lambert-2008` input to drive bike climb cost only. Download it from the [NGI open-data DTM 20m](https://ac.ngi.be/catalogue/getopenaccess/ngi-standard-open/Rasterdata/DTM_20m) and place it at `data/belgium-DTM-20m.tif`. Drop the `dem/*` input to skip elevation.

### Addresses (optional)

The `address/bestadd` input builds Belgian address autocomplete from the FPS BOSA BeST Address feed (auto-downloaded to `cache/bestadd.zip`, ~hundreds of MB, built into the separate `address.belgium.bin`). Drop the input to skip address search.

## Build cost

Measured 2026-07-14 on an AMD Ryzen AI 9 HX 370 (12 cores / 24 threads), 46 GiB RAM, NixOS 26.05 (Linux 6.18), Rust 1.95.0, on full-Belgium data. Numbers scale with the graph size you build.

| Metric | Value |
|---|---|
| Code compile (`cargo build --release`, clean) | ~1 min 49 s |
| Startup restoring a cached graph (~2.9 GB `graph.bin`) | ~100 s to ready |
| Memory while serving | ~8.8 GiB resident (~9.7 GiB peak during load) |
| Cache artifacts on disk (`graph.belgium.bin` + `osm.belgium.bin` + `cch.belgium.bin` + `address.belgium.bin`) | ~4.4 GB |
| Query latency (warm, cross-Belgium multi-modal) | ~300 to 470 ms median |

A first-time build ingests OSM, GTFS and address data from scratch and takes noticeably longer than restoring a cached graph.
