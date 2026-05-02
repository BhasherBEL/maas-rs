# maas-rs

MaaS-rs is a multi-modal routing engine for public transport planning based on the acknoledgement that public data are often partial, specific or either completely wrong. It is build around ingestors that can read common data format such as GTFS, but also enrich or solve known problems such as bike allowance, railway mapping, ... with high degree of freedom to adapt to every weird way those data are available.

## Features

- Multi-modal routing
- Custom/enriched ingestors
- Transfer risk scoring
- Previous/next departure alternatives on every transit leg
- Leg geometry (walk traces, transit stop sequences)
- Fast restarts/rebuild via multi-phase binary graph cache

## Quick Start

## Configuration

MaaS-rs is configuration-driven. All configuration must be defined in `config.yaml`.

```yaml
build:
  inputs:
    # General PBF ingestor
    - ingestor: osm/pbf
      url: path:data/belgium-latest.osm.pbf

    # Generic GTFS ingestor
    - ingestor: gtfs/generic
      name: MyAgency
      url: path:data/gtfs.zip

    # Belgium railway - Use a specific ingestor based on GTFS to enrich
    - ingestor: gtfs/sncb
      name: SNCB
      url: path:data/sncb.zip
      osm_url: path:data/belgium-latest.osm.pbf  # Custom parameter, used for Railway path matching

  osm_output: osm.bin  # OSM-only intermediate graph
  output: graph.bin    # Final graph

  # Per-mode delay CDF models for transfer risk scoring.
  # Each bin is [delay_seconds, cumulative_probability].
  delay_models:
    - mode: bus
      bins: [[-300, 0.03], [0, 0.45], [300, 0.84], [900, 0.97], [1800, 1.00]]
    - mode: tram
      bins: [[-300, 0.02], [0, 0.55], [300, 0.90], [1800, 1.00]]

log_level: info  # trace | debug | info | warn | error

default_routing:
  walking_speed: 1390     # mm/s  (≈ 5 km/h)
  estimator_speed: 13900  # mm/s  (≈ 50 km/h, heuristic)
  min_access_secs: 600    # walk-radius for stop search (seconds)

server:
  host: 127.0.0.1
  port: 3000
```

URLs accept `path:relative/to/cwd` or `http(s)://` (fetched at build time).

### Data

Place any data required by the ingestors under `data/`.

Example data for Belgium could be:
```
data
├── belgium-latest.osm.pbf
└── sncb.zip # Belgium Railway
```

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
