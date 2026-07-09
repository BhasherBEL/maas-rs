# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**maas-rs** is a Rust-based Mobility-as-a-Service (MaaS) multi-modal, multi-objective routing engine for Belgium. It ingests OpenStreetMap (OSM), GTFS transit, and Belgian address (BeST) data, builds an in-memory graph, and exposes routing over a GraphQL API. It combines several engines:

- **RAPTOR** — public-transit journeys (bus/tram/metro/rail) with street access/egress.
- **Multi-objective Pareto street search** — walk / bike / car, ranking non-dominated routes over cost axes (Time, ascent, surface, cycleway-deficit, reliability variance), plus **transit hybrids** (park & ride, bike-to-transit).
- **CCH (Customizable Contraction Hierarchy)** — exact one-to-many foot access/egress from arbitrary coordinates to all transit stops.
- **Realtime layer** — GTFS-RT + STIB feeds folded into routing (delays/cancellations) and a live-journey overlay.

## Commands

Rust/Cargo commands run directly — no `devenv shell` wrapper needed.

```bash
# Build the project
cargo build --release

# Self-healing startup (how the service runs): restore the cached graph.bin if
# its schema version matches, else rebuild — reusing osm.bin when its version
# still matches so only the GTFS phase re-runs — then serve. A cron-gated GTFS
# refresh (config.yaml `auto_update`) runs in the background, and at startup if a
# scheduled tick was missed while down.
cargo run -- --serve

# Manual / first-time full build from config + save to graph.bin (and osm.bin)
cargo run -- --build --save

# Explicitly load a pre-built graph and serve (no rebuild fallback)
cargo run -- --restore --serve

# Manually re-ingest GTFS only on top of cached osm.bin
cargo run -- --update-gtfs --serve

# Rust tests
cargo test                        # all (unit + integration)
cargo test <name>                 # single test, substring match
cargo test --lib                  # inline unit tests only
cargo test --test graph_tests     # graph integration tests
cargo test --test graphql_tests   # in-process GraphQL tests
cargo test -- --nocapture         # with output visible

# JS unit tests (live-journey client logic — pure modules, node's built-in runner)
node --test src/web/static/js/*.test.mjs

# Lint
cargo clippy
```

The GraphQL playground is at `http://127.0.0.1:8000/graphiql` when the server is running (port from `config.yaml`, see below).

> **Server startup.** Do not start the server for general use — that is the user's responsibility. The **one exception** is the autonomous API-probing workflow below, used to validate routing end-to-end against the live GraphQL API. Even then, **always detect a server already listening first and reuse it** (the user may be running their own concurrently); never start a second instance.

## API Probing (autonomous end-to-end testing)

Test routing exactly as the UI does, by querying the **same GraphQL API the UI uses** — no separate harness, no new endpoints. The UI is just one client of this API; this workflow is another.

The port comes from `config.yaml` (`server.port`, currently **8000**; the code default when unset is 3000). Adjust the examples below if it changes.

1. **Detect first.** Before starting anything, check whether a server is already listening:
   `curl -s -X POST http://127.0.0.1:8000/graphql -H 'Content-Type: application/json' -d '{"query":"{ ping }"}'` should return `{"data":{"ping":"pong"}}`.
   If it answers, **reuse it** — never launch a second instance.
2. **Otherwise start one in the background.** Rebuild if stale (`cargo build --release`), then run `target/release/maas-rs --restore --serve` as a background process. It loads `graph.bin` (~2.7 GB), so poll `{ ping }` until it returns `pong` before querying. Reuse this single instance for all queries in the session.
3. **Query** `http://127.0.0.1:8000/graphql` with `curl`, using the same queries the UI sends — `raptor` (ranked plans) and `raptorExplain` (plans + every candidate's drop/filter reason + access-walk metadata + stops reached). The query bodies (incl. `PLAN_FRAGMENT`) live in `src/web/static/index.html`. Inputs are raw coordinates.
4. **Parse** the JSON response (e.g. with `jq`) and analyze.

## Architecture

### Data Flow

```
config.yaml → Ingestion:  phase 0 OSM → phase 1 GTFS → phase 2 BeST addresses
           → Graph build (+ CCH for foot access/egress)
           → serialize graph.bin / osm.bin / cch.bin / address.bin (postcard)
           → GraphQL server (Poem + async-graphql), default port 8000
           ↑ Realtime poller (background) folds GTFS-RT/STIB feeds into an ArcSwap index
```

Phase order matters: OSM (phase 0) runs first so GTFS stops (phase 1) can snap to street nodes; BeST addresses (phase 2) build a **separate** `address.bin` index, not the routing graph.

### Module Structure

- **`src/structures/graph/`** — `mod.rs` defines the `Graph` (OSM street network) with `pub raptor: RaptorIndex` holding all transit data. Constants: `MAX_TRANSFER_DISTANCE_M` (1000 m), `MAX_ROUNDS` (20), `MAX_SCENARIOS` (2).
  - `raptor_index.rs` — `RaptorIndex` struct (all transit/railway fields + tuning params `min_access_secs`, `walking_speed_mps`); designed for future atomic hot-reload.
  - `raptor_route.rs` / `raptor_backward.rs` / `raptor_plan.rs` / `raptor_build.rs` / `raptor_access.rs` — RAPTOR core loop & range query, backward pass, plan reconstruction/pareto-filter, index construction, `walk_dijkstra`/`nearby_stops`.
  - `raptor_cch.rs` — CCH one-to-many exact foot access/egress (coords → all stops); saved to `cch.bin`.
  - `multiobj.rs` / `multiobj_plan.rs` — multi-objective label-setting street search (ε-pruned per-node Pareto frontiers) and its conversion to user-facing `LegOption`s (geometry, dedup, diversity, ride/push segmentation).
  - `contraction.rs` — degree-2 contracted graph (super-edges over junction chains); baked-cost traversal for bike; underpins CCH.
  - `representatives.rs` / `path_distribution.rs` / `platform_reach.rs` / `edge_index.rs` / `street_enrich.rs` / `latency_profile.rs` — Pareto-front trimming, post-hoc time-moment aggregation, platform connector reachability, R-tree edge snapping, walk-leg enrichment with alternatives, query latency profiler.
  - `realtime_match.rs` — STIB waiting-times → scheduled-arrival matching (`best_match`).
  - `transit.rs` / `railway.rs` — public transit accessors; SNCB railway topology cache (build-time).
- **`src/structures/cost/`** — multi-objective cost model: `axis.rs` (the cost axes + dominance), `mode_axes.rs` (per-mode active axes), `variance.rs` (reliability variance from signals/turns/etc.). Plus `mode.rs` (`RoutingMode` + burden hierarchy), `bike_profile.rs`/`bike_attrs.rs`/`surface_speed.rs`/`graph/bike_cost.rs` (kinematic bike model), `street_time.rs` (stochastic access/egress log-normal model), `delay.rs` (`DelayCDF`, scenario bags), `address.rs` (`AddressIndex`), `realtime.rs` (`RealtimeIndex`).
- **`src/ingestion/`** — parses inputs into nodes/edges:
  - `osm/` — PBF parse (`pbf.rs`), bike classification (`bike_class.rs`), DEM elevation sampling + RDP smoothing (`elevation.rs`, `elevation_smooth.rs`, `lambert.rs`), platform indexing (`platforms.rs`).
  - `gtfs/` — generic GTFS (`gtfs.rs`) plus `sncb.rs` (rail: snaps stops to OSM railway topology) and `stib.rs` (tram/metro: peak-hour bike-allowance rules).
  - `bestadd/` — BeST Belgian address feed (XML stream parse + Lambert72→WGS84).
  - `realtime/` — `RealtimeFeed` trait + GTFS-RT protobuf and STIB parsers; rate-limited `fetcher.rs`.
  - `cache.rs` (download/hash caching, `last_checked`), `secrets.rs` (`${ENV}` / `${file:…}` interpolation in URLs/headers).
- **`src/services/`** — `build.rs` (orchestrates ingestion phases + index construction + `apply_routing_defaults`/`finalize_contraction`), `persistence.rs` (postcard (de)serialization + schema-version headers), `scheduler.rs` (cron-gated feed refresh, freshness gate), `realtime_poller.rs` (background feed polling → ArcSwap `RealtimeIndex`).
- **`src/routing/`** — `routing_raptor.rs` wraps the graph's routing into a callable service.
- **`src/web/`** — Poem HTTP + async-graphql server (`app.rs`); `build_schema()` for tests. Static UI + PWA under `static/` (`index.html`, `maas.js`, service worker); the **live-journey client** in `static/js/` (`live-db`/`live-store`/`live-logic`/`live-view`/`live-mem`, `station-rank`) persists tracked journeys to **SQLite-WASM + OPFS**, falling back to in-memory on insecure contexts.

### GraphQL Surface (`QueryRoot` in `web/app.rs`)

- `ping` — health check.
- `raptor` — ranked multi-modal plans from/to coordinates (date/time optional).
- `raptorExplain` — plans plus every candidate's drop/filter reason + access metadata (debugging).
- `onboardRaptor` — re-plan from aboard a running trip (stay-on / alight-transfer / alight-walk).
- `legAlternatives` — per-leg walk/bike/drive Pareto alternatives and prev/next departures.
- `liveRefresh` — realtime overlay for a client-selected journey (no re-routing).
- `stationBackups` — same-station backup departures scored by catch-reliability.
- `realtimeGeneratedAt` — unix time of the current realtime snapshot.
- `gtfsStops` / `gtfsStations` / `gtfsAgencies` — GTFS catalogue.
- `searchAddresses` / `addressAttribution` — BeST address autocomplete (proximity/fuzzy ranked).

### Graph Model

- **Nodes**: `NodeData::OsmNode` (street intersections) or `NodeData::TransitStop` (GTFS stops).
- **Edges**: `EdgeData::Street` (foot/bike/car) or `EdgeData::Transit` (GTFS trip segments).
- **Spatial index**: KD-tree for nearest street node from lat/lng; R-tree (`edge_index.rs`) for nearest edge.
- Routes return as `Plan → PlanLeg → PlanLegStep`. `PlanLeg` is `Walk` (a street leg, mode Walk/Bike/Car) or `Transit`, with trip/route/agency metadata and prev/next departure alternatives.

## Configuration

**`config.yaml`** is the single source of tunables (it is self-documenting — read it rather than duplicating values here). Sections:
- `build.inputs` — ordered feeds (`ingestor: gtfs/stib|gtfs/sncb|gtfs/generic`, `osm/pbf`, `best/add`; `url: path:data/…` or remote), each with an optional `phase`.
- `build` — `output`/`osm_output`, DEM `elevation`, `surface_speed_factors`, `delay_models`.
- `default_routing` — walk/bike/car speeds, `min_access_secs`, `station_merge_radius_m`, address-search ranking, bike physics (`bike_profile`), stochastic `street_time`, multi-objective axis/bucket tuning.
- `server` (`host`/`port`), `auto_update` (cron schedule + cache dir), `realtime` (feeds, poll interval, staleness TTLs).

**Config policy:** tunable constants must come from `config.yaml`, not be hardcoded.

## Testing

> **Mandatory:** Every bug fix, new feature, or change to existing behaviour **must** include tests that validate it. Do not consider a task done until the relevant tests are written and passing (`cargo test --lib` + `cargo test --test graph_tests`, and `node --test src/web/static/js/*.test.mjs` for live-journey client changes).

### Test layout

| Location | What it covers |
|---|---|
| Inline `#[cfg(test)]` across `src/` | Most modules carry unit tests (geo, delay, raptor, cost/*, bike, address, ingestion parsers, config, persistence, scheduler, realtime, web) |
| `tests/graph_tests.rs` | `Graph` construction, KD-tree lookup, transit accessors, departures, `build_raptor_index`, `walk_dijkstra`, `nearby_stops`, `raptor`, `raptor_range` |
| `tests/graphql_tests.rs` | In-process GraphQL via `build_schema()`: ping, raptor error cases, `gtfsStops`, `gtfsAgencies`, etc. |
| `tests/next_day_fallback_tests.rs` | Next-day routing fallback |
| `src/web/static/js/*.test.mjs` | Live-journey client logic (SQLite store, live-logic, station-rank, time-fmt) — `node --test` |

### Important test invariants

- **`walk_dijkstra` / `nearby_stops`**: `build_raptor_index()` **must** be called first — the function reads `raptor.transit_node_to_stop[node.0]` every iteration and panics if the vector is empty.
- **Weekday bitmask**: Mon = `0x01`, Tue = `0x02`, Wed = `0x04`, Thu = `0x08`, Fri = `0x10`, Sat = `0x20`, Sun = `0x40`.
- **Time and date units**: times are **seconds since midnight** (`u32`), dates are **days since 2000-01-01** (`u32`).
- Transit stops are **not** added to the OSM KD-tree (`nodes_tree`), so `nearest_node` only returns `OsmNode` results.

## Key Implementation Notes

- The graph is a **custom adjacency list**. `Graph` holds the OSM street network; all transit state lives in `graph.raptor` (`RaptorIndex`), designed for future hot GTFS reload via `Arc<RwLock<…>>`.
- `walking_speed_mps` (default 1.2 m/s), `cycling_speed_mps`, `driving_speed_mps` live in config (`default_routing`).
- `MAX_ROUNDS` in `graph/mod.rs` controls RAPTOR transit rounds (higher = more transfers explored).
- The `.envrc` sets up a Nix environment for OpenSSL; run `direnv allow` if using Nix.
- **Cache artifacts & schema versions** — all treated as caches with an 8-byte header (`MAAS` magic + `u32` version) checked at load; a mismatch triggers auto-rebuild on `--serve`, no manual step. Consts live in `src/services/persistence.rs` — **bump them when the corresponding fields change layout**:
  - `graph.bin` — full graph + `RaptorIndex`; gated by `GRAPH_SCHEMA_VERSION`.
  - `osm.bin` — OSM-only view (no transit); gated by `OSM_SCHEMA_VERSION`, so a transit-only change reuses `osm.bin` and re-runs only the GTFS phase.
  - `address.bin` — BeST address index; gated by `ADDRESS_SCHEMA_VERSION` (FSTs rebuilt on load).
  - `cch.bin` — foot access/egress CCH; header is `CCH_SCHEMA_VERSION ^ GRAPH_SCHEMA_VERSION`, so any graph-topology change also invalidates it.
- **Freshness gate**: `cache/last_checked` (RFC3339) records the last feed *check* (download+hash), stamped every scheduler cycle and on every build — *not* only on change. At startup the auto path refreshes once if a cron tick elapsed since then (`feeds_stale` in `src/services/scheduler.rs`).
- `DelayCDF` (`structures/delay.rs`) and the `delay_models` in config feed reliability/variance scoring across routing.
