import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildGrid,
  marchingSquares,
  contourThresholds,
  exposedEdges,
  ramp,
  summarizeLegs,
  sampleSeconds,
} from "./travel-map.mjs";

// ── buildGrid ────────────────────────────────────────────────────────────────
test("buildGrid reconstructs a regular lattice with a +Infinity sentinel", () => {
  // 2x2 lattice; drop one corner (unreachable => sentinel).
  const cells = [
    { lat: 50.0, lng: 4.0, seconds: 100 },
    { lat: 50.0, lng: 4.1, seconds: 200 },
    { lat: 50.1, lng: 4.0, seconds: 300 },
    // (50.1, 4.1) missing on purpose
  ];
  const g = buildGrid(cells);
  assert.equal(g.rows, 2);
  assert.equal(g.cols, 2);
  assert.deepEqual(g.lats, [50.0, 50.1]);
  assert.deepEqual(g.lngs, [4.0, 4.1]);
  assert.equal(g.sec[0][0], 100);
  assert.equal(g.sec[0][1], 200);
  assert.equal(g.sec[1][0], 300);
  assert.equal(g.sec[1][1], Infinity); // missing cell -> sentinel
});

test("buildGrid keeps the minimum seconds when a coordinate repeats", () => {
  const g = buildGrid([
    { lat: 50.0, lng: 4.0, seconds: 400 },
    { lat: 50.0, lng: 4.0, seconds: 250 },
  ]);
  assert.equal(g.sec[0][0], 250);
});

// ── contourThresholds ────────────────────────────────────────────────────────
test("contourThresholds draws only the outer reachability boundary", () => {
  assert.deepEqual(contourThresholds(30 * 60), [1800]);
  assert.deepEqual(contourThresholds(45 * 60), [2700]);
  assert.deepEqual(contourThresholds(1), [1]);
});

test("contourThresholds is empty for a non-positive maxSeconds", () => {
  assert.deepEqual(contourThresholds(0), []);
  assert.deepEqual(contourThresholds(-5), []);
});

// ── marchingSquares ──────────────────────────────────────────────────────────
test("marchingSquares emits no segments when the whole cell is inside", () => {
  const g = buildGrid([
    { lat: 0, lng: 0, seconds: 100 },
    { lat: 0, lng: 1, seconds: 100 },
    { lat: 1, lng: 0, seconds: 100 },
    { lat: 1, lng: 1, seconds: 100 },
  ]);
  assert.equal(marchingSquares(g, 200).length, 0); // all <= threshold
});

test("marchingSquares crosses a single-corner cell and interpolates the edge", () => {
  // Only the top-left corner is below the threshold; the iso-line should cut
  // across the top and left edges. seconds tl=0, others=100, threshold=50 ->
  // crossings at the edge midpoints.
  const g = buildGrid([
    { lat: 1, lng: 0, seconds: 0 },   // top-left (i=1 is the higher lat, but grid sorts asc)
    { lat: 1, lng: 1, seconds: 100 },
    { lat: 0, lng: 0, seconds: 100 },
    { lat: 0, lng: 1, seconds: 100 },
  ]);
  const segs = marchingSquares(g, 50);
  assert.equal(segs.length, 1);
  // Endpoints must be finite fractional grid coords within the single cell.
  for (const s of segs) {
    for (const p of [s.a, s.b]) {
      assert.ok(Number.isFinite(p.i) && Number.isFinite(p.j));
      assert.ok(p.i >= 0 && p.i <= 1 && p.j >= 0 && p.j <= 1);
    }
  }
});

test("marchingSquares treats Infinity (unreachable) corners as outside", () => {
  // One reachable corner, three unreachable -> one boundary segment.
  const g = buildGrid([
    { lat: 0, lng: 0, seconds: 100 },
    { lat: 0, lng: 1, seconds: 100 },
    { lat: 1, lng: 0, seconds: 100 },
    // (1,1) missing -> Infinity, outside for any finite threshold
  ]);
  const segs = marchingSquares(g, 200);
  assert.ok(segs.length >= 1); // reachable meets unreachable => a boundary
});

// ── exposedEdges ─────────────────────────────────────────────────────────────
// Canonicalize a segment set (order-independent, endpoint-order-independent) so
// tests compare geometry, not traversal order.
function edgeKey(s) {
  const p = (q) => `${q.i.toFixed(3)},${q.j.toFixed(3)}`;
  return [p(s.a), p(s.b)].sort().join("|");
}
const edgeSet = (segs) => new Set(segs.map(edgeKey));

test("exposedEdges outlines a single cell as its 4 square edges", () => {
  const g = buildGrid([{ lat: 0, lng: 0, seconds: 100 }]); // 1x1
  const segs = exposedEdges(g);
  assert.equal(segs.length, 4);
  // Square around centre (0,0): corners at ±0.5 in i and j.
  const expect = edgeSet([
    { a: { i: -0.5, j: -0.5 }, b: { i: -0.5, j: 0.5 } }, // top
    { a: { i: 0.5, j: -0.5 }, b: { i: 0.5, j: 0.5 } },   // bottom
    { a: { i: -0.5, j: -0.5 }, b: { i: 0.5, j: -0.5 } }, // left
    { a: { i: -0.5, j: 0.5 }, b: { i: 0.5, j: 0.5 } },   // right
  ]);
  assert.deepEqual(edgeSet(segs), expect);
});

test("exposedEdges emits only the OUTER ring for a full 2x2 (no interior edges)", () => {
  const g = buildGrid([
    { lat: 0, lng: 0, seconds: 100 },
    { lat: 0, lng: 1, seconds: 100 },
    { lat: 1, lng: 0, seconds: 100 },
    { lat: 1, lng: 1, seconds: 100 },
  ]);
  const segs = exposedEdges(g);
  // Each of the 4 cells contributes exactly 2 outer edges; the shared interior
  // edges are suppressed (both sides reachable) -> 8 edges, none duplicated.
  assert.equal(segs.length, 8);
  assert.equal(edgeSet(segs).size, 8);
  // No edge lies on an interior lattice line (i=0.5 spanning j across the middle,
  // or j=0.5 spanning i) with reachable cells on both sides.
  for (const s of segs) {
    const interiorHoriz = s.a.i === 0.5 && s.b.i === 0.5 && s.a.j > -0.5 && s.b.j < 1.5;
    const interiorVert = s.a.j === 0.5 && s.b.j === 0.5 && s.a.i > -0.5 && s.b.i < 1.5;
    assert.ok(!(interiorHoriz && s.a.j === 0.5), "no shared interior horizontal edge");
    assert.ok(!(interiorVert && s.a.i === 0.5), "no shared interior vertical edge");
  }
});

test("exposedEdges wraps a hole left by a missing interior cell", () => {
  // 3x3 fully reachable except the centre -> outer ring (12 edges) + the 4 edges
  // around the hole = 16 exposed edges.
  const cells = [];
  for (let i = 0; i < 3; i++)
    for (let j = 0; j < 3; j++)
      if (!(i === 1 && j === 1)) cells.push({ lat: i, lng: j, seconds: 100 });
  const segs = exposedEdges(buildGrid(cells));
  assert.equal(segs.length, 16);
});

test("exposedEdges returns nothing for an empty/degenerate grid", () => {
  assert.deepEqual(exposedEdges(null), []);
  assert.deepEqual(exposedEdges({ rows: 0, cols: 0, sec: [] }), []);
});

// ── sampleSeconds ─────────────────────────────────────────────────────────────
test("sampleSeconds bilinearly interpolates inside a full cell", () => {
  const g = buildGrid([
    { lat: 0, lng: 0, seconds: 0 },
    { lat: 0, lng: 1, seconds: 100 },
    { lat: 1, lng: 0, seconds: 200 },
    { lat: 1, lng: 1, seconds: 300 },
  ]);
  // Exactly on a corner returns that corner.
  assert.equal(sampleSeconds(g, 0, 0), 0);
  assert.equal(sampleSeconds(g, 1, 1), 300);
  // Cell centre = mean of the four corners.
  assert.equal(sampleSeconds(g, 0.5, 0.5), 150);
  // Midpoint of the bottom edge (lat=0) between 0 and 100.
  assert.equal(sampleSeconds(g, 0, 0.5), 50);
});

test("sampleSeconds falls back to the nearest reachable corner inside a partial cell", () => {
  const g = buildGrid([
    { lat: 0, lng: 0, seconds: 100 },
    { lat: 0, lng: 1, seconds: 100 },
    { lat: 1, lng: 0, seconds: 100 },
    // (1,1) missing -> Infinity
  ]);
  // A point INSIDE the cell (not on the missing corner) has finite neighbours,
  // so the nearest-reachable-corner fallback yields a value.
  assert.equal(sampleSeconds(g, 0.1, 0.1), 100);
  // The exact missing corner clamps to that lattice node only -> unreachable.
  assert.equal(sampleSeconds(g, 1, 1), Infinity);
});

test("sampleSeconds handles a single reachable point and a null grid", () => {
  const g = buildGrid([{ lat: 0, lng: 0, seconds: 100 }]); // 1x1
  assert.equal(sampleSeconds(g, 0, 0), 100); // single reachable point
  assert.equal(sampleSeconds(null, 0, 0), Infinity);
});

// ── ramp ─────────────────────────────────────────────────────────────────────
test("ramp anchors green at 0, yellow at 0.5, red at 1 and clamps", () => {
  assert.deepEqual(ramp(0), [22, 163, 74]);
  assert.deepEqual(ramp(0.5), [234, 179, 8]);
  assert.deepEqual(ramp(1), [220, 38, 38]);
  assert.deepEqual(ramp(-1), [22, 163, 74]); // clamps below 0
  assert.deepEqual(ramp(2), [220, 38, 38]);  // clamps above 1
});

// ── summarizeLegs ────────────────────────────────────────────────────────────
test("summarizeLegs merges adjacent walks and names transit lines", () => {
  const legs = [
    { __typename: "PlanWalkLeg", start: 0, end: 120, duration: 120 },
    {
      __typename: "PlanTransitLeg", start: 120, end: 600,
      trip: { headsign: "Roodebeek", route: { shortName: "29", mode: "Bus" } },
      to: { node: { name: "Roodebeek" } },
    },
    { __typename: "PlanWalkLeg", start: 600, end: 660, duration: 60 },
    { __typename: "PlanWalkLeg", start: 660, end: 720, duration: 60 },
  ];
  const out = summarizeLegs(legs);
  assert.equal(out.length, 3);
  assert.deepEqual(out[0], { kind: "walk", mins: 2 });
  assert.equal(out[1].kind, "transit");
  assert.equal(out[1].line, "29");
  assert.equal(out[1].to, "Roodebeek");
  assert.deepEqual(out[2], { kind: "walk", mins: 2 }); // 60+60 merged
});

test("summarizeLegs falls back to mode then '?' for an unnamed line", () => {
  const out = summarizeLegs([
    { __typename: "PlanTransitLeg", start: 0, end: 60, trip: { route: { mode: "Rail" } } },
    { __typename: "PlanTransitLeg", start: 60, end: 120, trip: { route: {} } },
  ]);
  assert.equal(out[0].line, "Rail");
  assert.equal(out[1].line, "?");
});
