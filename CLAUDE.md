# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**maas-rs** is a Rust-based Mobility-as-a-Service (MaaS) multi-modal routing engine. It ingests OpenStreetMap (OSM) and GTFS transit data, builds an in-memory graph, and exposes routing via a GraphQL API. Two routing algorithms are implemented: **A\*** (for street/walking routes) and **RAPTOR** (for public transit).

## Commands

Rust/Cargo commands run directly — no `devenv shell` wrapper needed.

```bash
# Build the project
cargo build --release

# Build graph from config + save to graph.bin
cargo run -- --build --save

# Load pre-built graph and start API server
cargo run -- --restore --serve

# Build in-memory (no save) and serve
cargo run -- --build --serve

# Run all tests (unit + integration)
cargo test

# Run a single test by name (substring match)
cargo test <test_name>

# Run only the inline unit tests
cargo test --lib

# Run only the graph integration tests
cargo test --test graph_tests

# Run tests with output visible (useful for debugging)
cargo test -- --nocapture

# Lint
cargo clippy
```

The GraphQL playground is available at `http://127.0.0.1:3000/graphiql` when the server is running.

> **Never run the server directly** (`cargo run -- --serve`). Starting the server is the user's responsibility. Only write and run tests, or build the binary.

## Architecture

### Data Flow

```
config.yaml → Ingestion (OSM phase 0, then GTFS phase 1) → Graph build
           → (optional) graph.bin serialization via postcard
           → GraphQL server (Poem + async-graphql) on port 3000
```

### Module Structure

- **`src/structures/`** — Core data types: `graph.rs` is the central file (~1500 lines) containing the graph data structure, A\* and RAPTOR algorithm implementations. Key constants: `MAX_TRANSFER_DISTANCE_M`, `MAX_ROUNDS`, `WALKING_SPEED_MS`.
- **`src/ingestion/`** — Parses OSM PBF and GTFS zip files into nodes/edges. Phase ordering matters: OSM runs first (phase 0) so transit stops (phase 1) can snap to street nodes.
- **`src/routing/`** — `routing_astar.rs` and `routing_raptor.rs` wrap the graph methods into callable routing services.
- **`src/services/`** — `build.rs` orchestrates ingestion + RAPTOR index construction; `persistence.rs` handles `postcard` binary serialization of the graph.
- **`src/web/`** — Poem HTTP + async-graphql server. Exposes `astar` and `raptor` GraphQL queries. GraphQL types use `#[graphql(complex)]` with async resolvers for nested objects.

### Graph Model

- **Nodes**: `NodeData::OsmNode` (street intersections) or `NodeData::TransitStop` (GTFS stops)
- **Edges**: `EdgeData::Street` (foot/bike/car) or `EdgeData::Transit` (GTFS trip segments)
- **Spatial index**: KD-tree for nearest-node lookup from lat/lng coordinates
- **RAPTOR index**: Preprocessed transit patterns stored in `src/structures/raptor.rs`

### Output Structure

Routes return as `Plan → PlanLeg (Walk|Transit) → PlanLegStep`, with rich metadata including trip/route/agency info and previous/next departure alternatives.

## Configuration

**`config.yaml`** specifies:
- Input sources (`ingestor: gtfs/generic` or `osm/pbf`, with `url: path:data/...`)
- Output path for serialized graph (`output: graph.bin`)
- Default routing parameters (`walking_speed`, `estimator_speed` in mm/s)
- Server host/port (note: port in `app.rs` is currently hardcoded to 3000)

## Testing

> **Mandatory:** Every bug fix, new feature, or change to existing behaviour **must** include tests that validate it. Do not consider a task done until the relevant tests are written and passing (`cargo test --lib` + `cargo test --test graph_tests`).

### Test layout

| Location | What it covers |
|---|---|
| `src/structures/geo.rs` | `LatLng` Haversine distance, `meters_to_degrees` / `degrees_to_meters` |
| `src/structures/delay.rs` | `DelayCDF::prob_on_time`, all `ScenarioBag` methods |
| `src/structures/raptor.rs` | `Lookup::of`, `Trace::is_transit` / `is_transfer` |
| `src/ingestion/gtfs/gtfs.rs` | `ServicePattern::is_active`, `date_to_days` |
| `src/ingestion/gtfs/utils.rs` | `IdMapper`, `display_route_type`, `sec_to_time` |
| `tests/graph_tests.rs` | `Graph` construction, KD-tree lookup, `nodes_distance`, transit accessors, `next_transit_departure`, `previous/next_departures`, `build_raptor_index`, `walk_dijkstra`, `nearby_stops` |

### Important test invariants

- **`walk_dijkstra` / `nearby_stops`**: `build_raptor_index()` **must** be called first — the function reads `transit_node_to_stop[node.0]` on every iteration and will panic if the vector is empty.
- **Weekday bitmask**: Mon = `0x01`, Tue = `0x02`, Wed = `0x04`, Thu = `0x08`, Fri = `0x10`, Sat = `0x20`, Sun = `0x40`.
- **Time and date units**: times are **seconds since midnight** (`u32`), dates are **days since 2000-01-01** (`u32`).
- **`RoutingParameters` speeds** are in **mm/s** (e.g. 5 km/h = 1389 mm/s).
- Transit stops are **not** added to the OSM KD-tree (`nodes_tree`), so `nearest_node` only returns `OsmNode` results.

## Key Implementation Notes

- The graph is a **custom adjacency list** (not petgraph's Graph type, despite petgraph being a dependency).
- `DelayCDF` in `structures/delay.rs` models delay probability distributions for future multi-scenario routing.
- `MAX_ROUNDS` in `graph.rs` controls RAPTOR transit rounds (higher = more transfers explored).
- The `.envrc` sets up a Nix environment for OpenSSL; run `direnv allow` if using Nix.
- `graph.bin` (~63MB for Brussels) is the serialized graph cache — commit-ignored, regenerated with `--build --save`.
