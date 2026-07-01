// Static guard against the live-view bridge drifting out of sync with the
// live-logic exports the index.html controller actually calls. No browser, no
// deps — pure source-text analysis under node:test.
//
// The controller aliases `const L = window.MaaSLive?.logic` and calls logic
// functions as `L.<fn>`. But `L` is ALSO the Leaflet global in index.html, so a
// raw `L.<name>` is either a Leaflet API call or a live-logic call. This test
// asserts every non-Leaflet `L.<name>` is a real live-logic export (catching a
// missing bridge export OR a typo'd logic call), and that the bridge exposes the
// WHOLE module rather than a hand-curated subset (so it can't regress).

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const read = (name) => readFileSync(join(here, name), "utf8");

const liveLogicSrc = read("live-logic.mjs");
const liveViewSrc = read("live-view.mjs");
const indexSrc = read(join("..", "index.html"));

// Names that belong to the Leaflet global `L`, NOT to live-logic.
const LEAFLET_METHODS = new Set([
  "map", "tileLayer", "marker", "divIcon", "polyline", "polygon", "latLng",
  "latLngBounds", "icon", "popup", "tooltip", "control", "layerGroup",
  "featureGroup", "geoJSON", "circle", "circleMarker", "rectangle",
  "imageOverlay", "videoOverlay", "svg", "canvas", "DomEvent", "DomUtil",
  "Util", "Browser", "point", "bounds", "Icon", "Marker", "Control",
]);

function exportedNames(src) {
  const names = new Set();
  const re = /export\s+(?:async\s+)?(?:function|const|let|var)\s+([A-Za-z_$][\w$]*)/g;
  let m;
  while ((m = re.exec(src)) !== null) names.add(m[1]);
  return names;
}

function aliasCalls(src) {
  const names = new Set();
  // L.<name>, window.MaaSLive.logic.<name>, MaaSLive.logic.<name>
  const re = /(?:\bwindow\.MaaSLive\.logic|\bMaaSLive\.logic|\bL)\.([A-Za-z_$][\w$]*)/g;
  let m;
  while ((m = re.exec(src)) !== null) names.add(m[1]);
  return names;
}

test("every non-Leaflet L.<name> in index.html is a live-logic export", () => {
  const exports = exportedNames(liveLogicSrc);
  assert.ok(exports.size > 0, "failed to parse live-logic exports");

  const called = aliasCalls(indexSrc);
  const unresolved = [...called].filter(
    (n) => !LEAFLET_METHODS.has(n) && !exports.has(n),
  );
  assert.deepEqual(
    unresolved,
    [],
    `controller calls names that are neither Leaflet methods nor live-logic exports: ${unresolved.join(", ")}`,
  );

  // Sanity: the four functions the bug originally dropped must be present and
  // both called and exported (so the test is genuinely exercising them).
  for (const fn of ["isLiveStale", "activeLegAt", "transferRiskRows", "applyDepartureChange"]) {
    assert.ok(exports.has(fn), `${fn} should be a live-logic export`);
    assert.ok(called.has(fn), `${fn} should be called by the controller`);
  }
});

test("live-view bridge exposes the WHOLE live-logic module, not a curated subset", () => {
  // Namespace import of live-logic...
  assert.match(
    liveViewSrc,
    /import\s+\*\s+as\s+([A-Za-z_$][\w$]*)\s+from\s+["']\.\/live-logic\.mjs["']/,
    "live-view.mjs must namespace-import live-logic (import * as ...)",
  );
  const ns = liveViewSrc.match(
    /import\s+\*\s+as\s+([A-Za-z_$][\w$]*)\s+from\s+["']\.\/live-logic\.mjs["']/,
  )[1];

  // ...and assign that whole namespace to `logic` (spread or direct).
  const assignRe = new RegExp(
    `const\\s+logic\\s*=\\s*(?:${ns}|\\{\\s*\\.\\.\\.${ns}\\s*\\})\\s*;`,
  );
  assert.match(
    liveViewSrc,
    assignRe,
    "live-view.mjs must set `logic` to the full live-logic namespace",
  );

  // Guard against re-introducing a hand-listed named import from live-logic.
  assert.doesNotMatch(
    liveViewSrc,
    /import\s+\{[^}]*\}\s+from\s+["']\.\/live-logic\.mjs["']/,
    "live-view.mjs must not hand-list named imports from live-logic (that's the bug)",
  );
});
