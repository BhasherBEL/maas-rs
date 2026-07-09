# MaaS-rs

MaaS-rs is a multimodal routing engine that plans journeys without making assumptions. It combines walking, cycling, driving and transit into a single planner that lays out several realistic options and weighs them on multiple criteria at once, rather than deciding for you. It also takes real-world data seriously, repairing transit feeds that rarely follow their own spec.

## Features

 - **Options, not answers.** The planner surfaces many realistic ways to make a trip instead of picking one, including some that look unlikely on paper. Preferences reorder and highlight the list; they never filter it down.
 - **Multi-objective.** Routes are weighed on several criteria at once, such as time, elevation, comfort and reliability, with the relevant set depending on the mode. Nothing collapses to a single blended score.
 - **Reliability as a real criterion.** Travel-time uncertainty is part of the search itself, surfaced as a p50/p95 arrival window rather than a hidden safety buffer.
 - **Enriching ingestors.** Real GTFS feeds rarely implement the full spec, so each provider gets an ingestor that repairs and completes the data: rebuilding missing station groupings, recovering trip ids and delays that realtime feeds leave out, snapping trains onto the OSM railway graph.
 - **Realtime.** GTFS-RT delays are matched back onto the static schedule, even for feeds that ship no trip ids.
 - **Multi-modal.** Walking, cycling, driving and transit run over a single shared graph.
 - **Runs on one box.** Pure Rust, no cluster and no per-origin precomputation; cached builds restart in seconds.

## Getting Started

Configuration lives in `config.yaml`, which is commented and self-explanatory. Edit it to set your data sources, then build and run:

```bash
cargo build --release
./target/release/maas-rs
```

On first run it builds the graph and caches it; afterwards it restores the cache and rebuilds only what changed. The UI is at `http://127.0.0.1:8000`.

## Data Sources

Sources are declared in `config.yaml`, each handled by a dedicated ingestor:

- `osm/pbf` for OpenStreetMap street network from a `.pbf` extract
- `gtfs/generic` for standard GTFS feed
- `gtfs/stib` for STIB/MIVB. Adds the operator's bike-allowance rules
- `gtfs/sncb` for SNCB/NMBS. Synthesises missing route shapes and applies the operator's bike-allowance rule 
- `best/add` for Belgian address search (BeST Address)

Elevation is optional: point `elevation:` at a GeoTIFF DEM and per-node altitude is sampled at ingest to drive climb cost and timing. Without it, elevation is simply disabled.

Realtime is handled by its own set of ingestors, behind a shared feed interface:

- Generic GTFS-Realtime trip updates for any operator that publishes them.
- A custom STIB parser: its waiting-times feed carries no trip ids or delays, so both are recovered by matching each predicted arrival against the static schedule.
