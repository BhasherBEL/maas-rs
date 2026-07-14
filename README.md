# MaaS-rs

MaaS-rs is a multimodal routing engine that plans journeys without making assumptions. It combines walking, cycling, driving and transit into a single planner that lays out several realistic options and weighs them on multiple criteria at once, rather than deciding for you. It also takes real-world data seriously, repairing transit feeds that rarely follow their own spec.

Licensed under **AGPL-3.0-only** (see [LICENSE](LICENSE)): the copyleft is deliberate, so that anyone running a modified version as a network service shares those modifications back.

## Features

 - **Options, not answers.** The planner surfaces many realistic ways to make a trip instead of picking one, including some that look unlikely on paper. Preferences reorder and highlight the list; they never filter it down.
 - **Multi-objective.** Routes are weighed on several criteria at once, such as time, elevation, comfort and reliability, with the relevant set depending on the mode. Nothing collapses to a single blended score.
 - **Reliability as a real criterion.** Travel-time uncertainty is part of the search itself, surfaced as a p50/p95 arrival window rather than a hidden safety buffer.
 - **Enriching ingestors.** Real GTFS feeds rarely implement the full spec, so each provider gets an ingestor that repairs and completes the data: rebuilding missing station groupings, recovering trip ids and delays that realtime feeds leave out, snapping trains onto the OSM railway graph.
 - **Realtime.** GTFS-RT delays are matched back onto the static schedule, even for feeds that ship no trip ids.
 - **Multi-modal.** Walking, cycling, driving and transit run over a single shared graph.
 - **Runs on one box.** Pure Rust, no cluster and no per-origin precomputation; cached builds restart in seconds.

## Quickstart

The shipped `config.yaml` is generic and runnable out of the box: a small remote OSM extract plus one open GTFS feed, no API keys, no local data files. Clone and run:

```bash
cargo run --release -- --serve
```

The first run downloads a Luxembourg OSM extract (~40 MB from Geofabrik) and Luxembourg's national GTFS feed, builds a real graph, and serves. Plan a trip at `http://127.0.0.1:8000` and you get the multi-option Pareto front rather than a single answer. To build and cache without serving, use `cargo run --release -- --build --save`; later runs restore the cache and rebuild only what changed.

For the full Belgium engine (STIB/SNCB/De Lijn/TEC transit, fares, realtime, addresses, elevation) run the preset:

```bash
cargo run --release -- --config presets/belgium.yaml --serve
```

See [`presets/belgium.md`](presets/belgium.md) for the data sources and API key it needs.

## Data Acquisition

Data sources are declared under `build.inputs` in the config file, each handled by a named ingestor. The ingestor tag selects the parser and, where relevant, the input family:

- `osm/pbf` for an OpenStreetMap street network `.pbf` extract. Any [Geofabrik](https://download.geofabrik.de/) region works; point the input's `url:` at the extract that covers your transit area.
- `gtfs/generic` for a standard GTFS feed. `gtfs/stib` and `gtfs/sncb` are enriching variants that repair operator-specific quirks (STIB bike-allowance rules; SNCB route shapes and railway snapping).
- `dem/<projection>` for an optional elevation raster (see below).
- `address/*` for an optional address-search index (see below).

Remote `url:`s are auto-downloaded and cached on first build; a `path:` URL points at a local file you provide.

### Elevation (optional)

Elevation drives bike climb cost only (climb timing and the ascent axis); walking, driving and transit are unaffected. It is an optional `dem/<projection>` input whose ingestor tag names the raster's map projection. The one shipped projection is `dem/belgian-lambert-2008` (EPSG:3812); a DEM in another CRS needs its own `dem/<projection>` ingestor added in code. A raster's projection must match its ingestor: on load the engine reads the GeoTIFF's EPSG and warns (non-fatal) if it differs, and nodata cells are detected and skipped. Omit the `dem/*` input to disable elevation entirely.

### Address search (optional)

Address autocomplete is an optional feature backed by an `address/*` input. Its data is country-specific: the shipped family is `address/bestadd` (Belgian BeST Address). The index is built into a separate `address.bin` (not the routing graph), so leaving the input out simply disables address search with no effect on routing.

### Per-country presets

Ready-made country setups live in [`presets/`](presets/). Each is a full config file naming that country's OSM extract, transit feeds, elevation DEM and address feed, with distinct output/cache paths so presets never clobber each other or the generic caches. Run one with `--config`:

```bash
cargo run --release -- --config presets/belgium.yaml --serve
```

See [`presets/README.md`](presets/README.md) for the ingestor families and how to add a new country, and [`presets/belgium.md`](presets/belgium.md) for the Belgium setup (data sources, the BMC transit key, build cost).

## Minimal config and self-hosting

The only required key is `build.inputs`. Everything else defaults: `build.output` is `graph.bin`, `default_routing` is fully optional (every tunable has a compiled-in default), and the `server`, `realtime` and `auto_update` sections all default when absent. A minimal config is just a list of inputs:

```yaml
build:
  inputs:
    - ingestor: osm/pbf
      url: https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf
    - ingestor: gtfs/generic
      name: transit
      url: https://path/to/an/open/gtfs.zip
```

A [NixOS module](flake.nix) (`nixosModules.default`) ships too: enable it with `services.maas-rs.enable = true` and set essentially only `services.maas-rs.settings.build.inputs`.

The server binds to `127.0.0.1` by default. If you expose it publicly (`server.host: 0.0.0.0`) review `graphiql_enabled` and the `graphql_max_depth` / `graphql_max_complexity` limits first.

The `MAAS_HOST` and `MAAS_PORT` env vars override `server.host` and `server.port` at load time. `docker-compose.yaml` uses `MAAS_HOST=0.0.0.0` to bind all interfaces inside the container while publishing only to the host loopback (`127.0.0.1:8000`).

## Realtime

Realtime is optional (`realtime.enabled`, off unless a `realtime` section enables it) and handled by its own ingestors behind a shared feed interface:

- Generic GTFS-Realtime trip updates for any operator that publishes them.
- A custom STIB parser: its waiting-times feed carries no trip ids or delays, so both are recovered by matching each predicted arrival against the static schedule.

<!-- Screenshots section: re-enable once image assets are added
## Screenshots

A picture is worth a thousand Pareto fronts. The planner is built around showing options and their uncertainty, which is easier to see than to describe.

TODO: add GIF of the multi-option Pareto front, p50/p95 brackets, highlight cursor
TODO: add screenshot of a single plan's detail view (boxed leg spine, per-leg alternatives)
TODO: add screenshot of the travel-time map / isochrone heatmap
-->

## Data Licensing

MaaS-rs processes third-party data; you are responsible for complying with the terms of whatever data you feed it.

- **Map data © OpenStreetMap contributors, licensed under the Open Database License (ODbL).** See <https://www.openstreetmap.org/copyright>.
- **Transit, realtime and address feeds are user-configured.** The feeds in the presets are examples and can be changed or removed. Each provider publishes its own terms of use, and the end user is responsible for complying with the terms of whatever feeds they configure. When a BeST Address feed is used, the app emits the required attribution via the `addressAttribution` GraphQL field, but you remain responsible for its terms.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the project stance and how to get involved.

## License

MaaS-rs is licensed under **AGPL-3.0-only**. See [LICENSE](LICENSE) for the full text.
