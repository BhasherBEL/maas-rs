import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

// maas.js is a classic (non-module) browser script included via include_str,
// so load its source into a sandbox with minimal stubs and exercise the real
// shipped detectHeadway helper (the same one index.html's departures UI calls).
const src = readFileSync(
  fileURLToPath(new URL("../maas.js", import.meta.url)),
  "utf8",
);

// maas.js has top-level side effects (Leaflet pins, etc.). We only care about
// the pure headway helper, so resolve every unrelated global to a chainable
// no-op stub.
const anyStub = new Proxy(function () {}, {
  get: () => anyStub,
  apply: () => anyStub,
  construct: () => anyStub,
});
const realGlobals = {
  document: { createElement: () => ({ appendChild() {} }) },
  console,
  __out: {},
};
const sandbox = new Proxy(realGlobals, {
  has: () => true,
  get: (t, k) =>
    k in t ? t[k] : k in globalThis ? globalThis[k] : anyStub,
});
vm.createContext(sandbox);
vm.runInContext(
  src + "\n;Object.assign(__out, { detectHeadway, REGULAR_GAP_TOLERANCE });",
  sandbox,
);

// detectHeadway runs inside the vm realm, so the plain object it returns carries
// that realm's Object.prototype. assert.deepStrictEqual (used by deepEqual from
// assert/strict) rejects cross-realm objects even when structurally identical
// ("same structure but are not reference-equal"). Re-homing the result into this
// realm's prototype lets the structural assertions below judge only the data.
const rawDetectHeadway = realGlobals.__out.detectHeadway;
const detectHeadway = (times) => ({ ...rawDetectHeadway(times) });
const { REGULAR_GAP_TOLERANCE } = realGlobals.__out;

const M = 60; // one minute in seconds

test("fewer than 3 times cannot form an interval", () => {
  assert.deepEqual(detectHeadway([]), { regular: false });
  assert.deepEqual(detectHeadway([10 * M]), { regular: false });
  assert.deepEqual(detectHeadway([10 * M, 25 * M]), { regular: false });
});

test("evenly spaced departures are regular, everyMins = gap", () => {
  // 08:00, 08:15, 08:30, 08:45 → every 15 minutes
  const t = [8 * 3600, 8 * 3600 + 15 * M, 8 * 3600 + 30 * M, 8 * 3600 + 45 * M];
  assert.deepEqual(detectHeadway(t), { regular: true, everyMins: 15 });
});

test("input order is irrelevant (sorted internally)", () => {
  const t = [8 * 3600 + 30 * M, 8 * 3600, 8 * 3600 + 45 * M, 8 * 3600 + 15 * M];
  assert.deepEqual(detectHeadway(t), { regular: true, everyMins: 15 });
});

test("small jitter within tolerance stays regular", () => {
  // gaps 10, 11, 10 min → median 10; |11-10| = 1 <= 10 * 0.2 = 2 → regular
  const base = 9 * 3600;
  const t = [base, base + 10 * M, base + 21 * M, base + 31 * M];
  const r = detectHeadway(t);
  assert.equal(r.regular, true);
  assert.equal(r.everyMins, 10);
});

test("one large gap breaks regularity", () => {
  // 15,15,60-min gaps → the 60-min gap is far from the 15-min median
  const base = 9 * 3600;
  const t = [base, base + 15 * M, base + 30 * M, base + 90 * M];
  assert.deepEqual(detectHeadway(t), { regular: false });
});

test("tolerance boundary: a gap 20% off the median is still regular", () => {
  assert.equal(REGULAR_GAP_TOLERANCE, 0.2);
  // median gap 10 min; one gap of 12 min = exactly +20% → within tolerance
  const base = 9 * 3600;
  const t = [base, base + 10 * M, base + 20 * M, base + 32 * M];
  assert.equal(detectHeadway(t).regular, true);
});

test("identical times (zero median gap) are not regular", () => {
  const t = [7 * 3600, 7 * 3600, 7 * 3600];
  assert.deepEqual(detectHeadway(t), { regular: false });
});
