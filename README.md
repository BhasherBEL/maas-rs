# maas-rs

Multi-modal routing engine for public transport planning. Ingests OpenStreetMap and GTFS data, builds an in-memory graph, and serves routes over a GraphQL API.

**Algorithms:** A\* (walking/cycling/driving) · RAPTOR (public transit, Range-RAPTOR)  
**License:** MIT

---

## Features

- Walk + transit multi-modal routing
- Range-RAPTOR: all Pareto-optimal journeys within a departure window
- Transfer risk scoring via per-mode delay CDF models
- Previous/next departure alternatives on every transit leg
- Leg geometry (walk traces, transit stop sequences)
- GTFS catalogue endpoints for stop search and agency/route listing
- Fast restarts via binary graph cache (`postcard` serialization)
- Two-phase build: OSM (phase 0) cached separately so GTFS can be refreshed independently

---

## Quick Start

### Prerequisites

- Rust (edition 2024)
- OpenSSL (for `poem`/`hyper-tls`); set `PKG_CONFIG_PATH` or use the Nix dev shell

```bash
nix develop   # sets PKG_CONFIG_PATH automatically
```

### Data

Place your data files under `data/`:

| File | Description |
|------|-------------|
| `data/region.osm.pbf` | OSM extract (e.g. from Geofabrik) |
| `data/gtfs.zip` | GTFS feed(s) |

Edit `config.yaml` to point at your files (see [Configuration](#configuration)).

### Build and run

```bash
# First run — ingest OSM + GTFS, save both caches, then serve
cargo run --release -- --build --save --serve

# Subsequent runs — load pre-built graph.bin (fast)
cargo run --release -- --restore --serve

# GTFS-only refresh — reloads osm.bin, re-ingests GTFS, saves graph.bin, then serves
# Requires a prior --build --save run.
cargo run --release -- --update-gtfs --save --serve
```

The GraphQL playground is available at `http://127.0.0.1:3000/graphiql`.

---

## Configuration

`config.yaml` is required at the working directory.

```yaml
build:
  inputs:
    - ingestor: osm/pbf
      url: path:data/region.osm.pbf

    - ingestor: gtfs/generic
      name: MyAgency
      url: path:data/gtfs.zip

    # STIB-flavoured GTFS (Brussels metro/tram/bus)
    # - ingestor: gtfs/stib
    #   name: STIB
    #   url: path:data/stib.zip

    # SNCB rail — requires a separate OSM file for railway matching
    # - ingestor: gtfs/sncb
    #   name: SNCB
    #   url: path:data/sncb.zip
    #   osm_url: path:data/region.osm.pbf

  output: graph.bin       # combined OSM+GTFS graph
  osm_output: osm.bin     # OSM-only intermediate (used by --update-gtfs)

  # Optional per-mode delay CDF models for transfer risk scoring.
  # Each bin is [delay_seconds, cumulative_probability].
  delay_models:
    - mode: bus
      bins: [[-300, 0.03], [0, 0.45], [300, 0.84], [900, 0.97], [1800, 1.00]]
    - mode: tram
      bins: [[-300, 0.02], [0, 0.55], [300, 0.90], [1800, 1.00]]

log_level: info   # trace | debug | info | warn | error

default_routing:
  walking_speed: 1390      # mm/s  (≈ 5 km/h)
  estimator_speed: 13900   # mm/s  (≈ 50 km/h, A* heuristic)
  min_access_secs: 600     # walk-radius for stop search (seconds)
```

URLs accept `path:relative/to/cwd` or `http(s)://` (fetched at build time).

---

## GraphQL API

### Routing

#### `raptor` — multi-modal journey planning

```graphql
query {
  raptor(
    fromLat: 50.846
    fromLng: 4.352
    toLat: 50.860
    toLng: 4.361
    date: "2025-06-01"      # optional, defaults to today
    time: "08:30"           # optional, defaults to now
    windowMinutes: 60       # optional, Range-RAPTOR departure window
    walkRadiusSecs: 600     # optional, override default access/egress radius
  ) {
    legs {
      ... on PlanWalkLeg  { start end duration from { name } to { name } geometry { lat lon } }
      ... on PlanTransitLeg {
        start end duration
        from { name } to { name }
        geometry { lat lon }
        trip { headsign route { shortName longName mode } agency { name } }
        transferRisk { reliability scheduledDeparture nextDeparture nextReliability }
        bikesAllowed
        previousDepartures(count: 3) { start end }
        nextDepartures(count: 3)     { start end }
      }
    }
  }
}
```

Times are **seconds since midnight**. Dates are `YYYY-MM-DD`.

#### `astar` — walk-only routing

```graphql
query {
  astar(fromLat: 50.846 fromLng: 4.352 toLat: 50.860 toLng: 4.361) {
    legs { ... on PlanWalkLeg { duration steps { instruction length } } }
  }
}
```

### GTFS Catalogue (for client sync)

```graphql
query { gtfsStops    { id name lat lon mode } }
query { gtfsAgencies { id name routes { id shortName longName mode color } } }
query { ping }
```

---

## Testing

```bash
cargo test                         # all tests
cargo test --lib                   # unit tests only
cargo test --test graph_tests      # integration tests only
cargo test <name>                  # filter by test name substring
cargo test -- --nocapture          # show println! output
```

---

## NixOS Module

The flake exposes a `nixosModules.default` for declarative deployment:

```nix
{
  imports = [ maas-rs.nixosModules.default ];

  services.maas-rs = {
    enable = true;
    mode = "restore-and-serve";   # restore-and-serve | build-and-serve | update-gtfs-and-serve
    openFirewall = true;
    dataDir = "/var/lib/maas-rs"; # graph.bin, osm.bin, and data/ must live here

    settings = {
      build.inputs = [
        { ingestor = "osm/pbf"; url = "path:data/region.osm.pbf"; }
        { ingestor = "gtfs/generic"; name = "MyAgency"; url = "path:data/gtfs.zip"; }
      ];
      default_routing.walking_speed = 1390;
    };
  };
}
```

---

## Architecture

```
config.yaml
    │
    ├─ Phase 0: OSM PBF → street nodes + edges → osm.bin
    └─ Phase 1: GTFS zip → transit stops/edges snapped to OSM → graph.bin
                                    │
                         GraphQL server (0.0.0.0:3000)
                         ├─ raptor(...)  → Vec<Plan>
                         ├─ astar(...)   → Plan
                         ├─ gtfsStops    → [GtfsStop]
                         └─ gtfsAgencies → [GtfsAgency]
```

**Graph:** custom adjacency list with a KD-tree for nearest-node lookups.  
**Nodes:** `OsmNode` (street intersections) or `TransitStop` (GTFS stops).  
**Edges:** `Street` (foot/bike/car weight) or `Transit` (GTFS trip segment).  
**RAPTOR index:** preprocessed patterns in `src/structures/raptor.rs`; must be built before running `walk_dijkstra` / `nearby_stops`.

Key constants (in `src/structures/graph/mod.rs`):

| Constant | Default | Meaning |
|----------|---------|---------|
| `MAX_TRANSFER_DISTANCE_M` | 500 m | Maximum walk distance for a transfer |
| `MAX_ROUNDS` | 5 | Maximum RAPTOR rounds (= max transfers + 1) |
| `WALKING_SPEED_MS` | 1.39 m/s | Fallback if not set via config |
