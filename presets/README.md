# Presets

A preset is a full config file for one country or region: it names the OSM extract, transit feeds, and any optional elevation and address inputs, with distinct output and cache paths so it never clobbers the generic caches or another preset.

Run a preset with `--config`:

```bash
cargo run --release -- --config presets/belgium.yaml --serve
```

The repo root's `config.yaml` is the generic, runnable-out-of-the-box default (Luxembourg, no keys). Presets here are the ready-made alternatives.

## Choose a setup

Pick one to try. Luxembourg needs nothing; Belgium needs two local files you fetch yourself (plus an optional key for De Lijn realtime).

- **Luxembourg** (the default `config.yaml`): keyless, works out of the box. The OSM extract (Geofabrik) and the national GTFS (data.public.lu) auto-download from open sources. Just:

  ```bash
  cargo run --release
  ```

- **Belgium** ([belgium.yaml](belgium.yaml), see [belgium.md](belgium.md)): STIB/SNCB/De Lijn/TEC transit, fares, realtime, NGI elevation, BeST addresses. The transit and address feeds auto-download, but you must fetch two local files yourself (the OSM extract and the DEM GeoTIFF; see [belgium.md](belgium.md)). De Lijn *realtime* needs a free BMC key, but that is optional (routing works on the static schedule without it). Run:

  ```bash
  cargo run --release -- --config presets/belgium.yaml
  ```

## Ingestor families

Every entry under `build.inputs` has an `ingestor:` tag that selects the parser and, where relevant, its family:

- `osm/pbf`: OpenStreetMap street network from a `.pbf` extract. Any [Geofabrik](https://download.geofabrik.de/) region works.
- `gtfs/generic`: a standard GTFS feed. `gtfs/stib` and `gtfs/sncb` are enriching variants that repair operator-specific quirks (STIB bike-allowance rules; SNCB route shapes and railway snapping).
- `dem/<projection>`: an optional elevation raster, used for bike climb cost only. The tag names the raster's map projection; the shipped one is `dem/belgian-lambert-2008` (EPSG:3812). A DEM in another CRS needs its own `dem/<projection>` ingestor added in code, and its raster must match that projection (the engine reads the GeoTIFF EPSG on load and warns if it differs). Omit to disable elevation.
- `address/*`: an optional, country-specific address-search index, built into a separate `address.bin`. The shipped family is `address/bestadd` (Belgian BeST Address). Omit to disable address search.

A `url:` may be remote (`https://…`, auto-downloaded and cached) or local (`path:data/…`). Header values may reference secrets via `${VAR}` or `${file:/path}` rather than inlining them.

## Adding a country

1. Copy `belgium.yaml` to `presets/<country>.yaml`.
2. Swap `build.inputs`: the OSM extract for your region, the country's GTFS feed(s), and (optionally) a `dem/<projection>` raster and an `address/*` feed.
3. Set distinct output paths so the preset never clobbers the generic caches or another preset: `build.output`, `build.osm_output`, `build.address_output`, and `build.cache_dir` (the directory for downloaded sources and build caches, used even when `auto_update` is disabled; falls back to the legacy `auto_update.cache_dir` if unset).
4. Run it with `cargo run --release -- --config presets/<country>.yaml --serve`.

The only required key is `build.inputs`; everything else defaults. Drop the sections you do not need (`fares`, `realtime`, `auto_update`, `default_routing`).
