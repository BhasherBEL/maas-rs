# Architecture

This is a map of how MaaS-rs is put together, for readers who want the shape of the system without reading the Rust. It is written to complement the [README](README.md); the source of truth for tunables is `config.yaml`, and `AGENTS.md` carries deeper implementation notes.

## The core idea: a cost vector, not a cost

Most routers reduce a route to one number and return the minimum. MaaS-rs instead scores each route on a vector of cost axes and keeps every route that is not beaten on all of them at once.

The axes include time, ascent (elevation gain), surface roughness/comfort, cycleway deficit (how much of a bike route is off dedicated infrastructure), and reliability variance (travel-time uncertainty). Which axes are active depends on the mode: a walk leg cares about surface, a bike leg about ascent and cycleway coverage, a car leg about neither. Price is handled separately as a post-hoc annotation on finished plans, not as an in-search axis.

Comparison between two routes uses **Pareto dominance**: route A dominates route B only if A is at least as good on every axis and strictly better on at least one. The result of a search is the non-dominated set (the Pareto front), which is the set of genuinely different trade-offs. Preferences reorder and highlight this front for display; they never filter it.

## Bounding the front

A pure Pareto search can explode, so several mechanisms bound it:

- **Epsilon-dominance / grid bucketing.** Axes are quantized into buckets (per-axis `epsilon` and `*_bucket_*_k` values in config) so near-identical routes collapse into one representative. This keeps per-node frontiers small without losing the extremes of a trade-off.
- **Distance corridor.** Multi-objective street legs only explore paths up to `(1 + distance_budget)` times the shortest distance, bounding the search ellipse.
- **Burden hierarchy.** Modes are ordered by how much of a burden they impose (walk < bike < car, plus park-and-ride and bike-to-transit hybrids). A per-burden cutoff means a lower-burden option is never starved by a higher-burden one dominating on raw time.
- **Representative trimming and diversity.** After the search, near-duplicate and heavily-overlapping alternatives are dropped so the returned list is diverse rather than a cluster of variations.

## The engines

Routing is several cooperating engines over one shared graph:

- **RAPTOR** plans public-transit journeys (bus/tram/metro/rail) round by round, with a backward pass that tightens departure times and a range query over a departure window. Reliability is folded in through per-mode delay distributions.
- **Multi-objective street search** is a label-setting search with per-node Pareto frontiers, used for walk / bike / car legs and the transit hybrids. This is where the cost vector and dominance above live.
- **CCH (Customizable Contraction Hierarchy)** answers exact one-to-many foot access and egress: from an arbitrary coordinate to every transit stop, quickly, so RAPTOR has correct walk connections at both ends.
- **Realtime layer** folds GTFS-RT and STIB feeds (delays, cancellations, vehicle positions) into routing through an atomically-swapped index, plus a live-journey overlay that re-scores a tracked trip without re-planning.

The graph is a custom adjacency list. Nodes are OSM street intersections or GTFS transit stops; edges are street segments (foot/bike/car) or transit trip segments. A KD-tree finds the nearest street node from a coordinate and an R-tree finds the nearest edge.

## Ingestion: the ingestor pattern

Data is turned into the graph by ingestors, run in ordered phases:

- **Phase 0, OSM.** A `.pbf` extract is parsed into the street network, with bike classification, optional DEM elevation sampling, and platform indexing.
- **Phase 1, GTFS.** Transit feeds become stops, trips and schedules. Each provider gets its own ingestor (`gtfs/generic`, `gtfs/sncb`, `gtfs/stib`) that repairs what real feeds omit: rebuilding station groupings, snapping trains onto the OSM railway graph, applying operator-specific bike-allowance rules. GTFS stops snap to phase-0 street nodes, which is why OSM must run first.
- **Phase 2, BeST addresses.** The Belgian address feed is parsed into a **separate** address index (`address.bin`), not the routing graph, powering address autocomplete.

Realtime feeds sit behind a shared feed interface (GTFS-RT protobuf and a custom STIB parser) and are polled in the background, independent of the build.

## Caches and schema versions

Building the graph is expensive, so results are serialized (postcard) and reused. The graph, OSM and address artifacts carry a 40-byte header: an 8-byte magic-plus-version prefix (`MAAS` magic plus a `u32` schema version) followed by a 32-byte input fingerprint, both checked at load; a mismatch (stale schema or changed inputs/params) triggers an automatic rebuild, with no manual step.

- `graph.bin`, the full graph plus all transit state, gated by `GRAPH_SCHEMA_VERSION`.
- `osm.bin`, the OSM-only view (no transit), gated by `OSM_SCHEMA_VERSION`, so a transit-only change reuses `osm.bin` and re-runs only the GTFS phase.
- `cch.bin`, the foot access/egress CCH, whose header is `CCH_SCHEMA_VERSION ^ GRAPH_SCHEMA_VERSION`, so any change to graph topology also invalidates it.
- `address.bin`, the BeST address index, gated by `ADDRESS_SCHEMA_VERSION`.

The schema constants live in `src/services/persistence.rs` and are bumped when the corresponding on-disk layout changes.

## Serving

The graph feeds a GraphQL API (Poem + async-graphql) that also serves the web UI. The UI is just one client of that API. Journey queries return `Plan → PlanLeg → PlanLegStep`, where a leg is either a street leg (walk/bike/car) or a transit leg with route and agency metadata. See the README and `AGENTS.md` for the full GraphQL surface.
