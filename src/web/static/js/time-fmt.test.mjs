import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

// maas.js is a classic (non-module) browser script included via include_str,
// so load its source into a sandbox with minimal DOM stubs and exercise the
// real shipped time-formatting helpers.
const src = readFileSync(
  fileURLToPath(new URL("../maas.js", import.meta.url)),
  "utf8",
);

function mkFakeEl(tag) {
  return {
    tagName: tag,
    className: "",
    textContent: "",
    children: [],
    appendChild(c) { this.children.push(c); return c; },
  };
}

// maas.js has top-level side effects (Leaflet pins, etc.). We only care about
// the pure time helpers, so resolve every unrelated global to a chainable no-op
// stub while providing a real minimal `document` so mkEl builds real elements.
const anyStub = new Proxy(function () {}, {
  get: () => anyStub,
  apply: () => anyStub,
  construct: () => anyStub,
});
const realGlobals = {
  document: { createElement: (tag) => mkFakeEl(tag) },
  console,
  __out: {},
};
const sandbox = new Proxy(realGlobals, {
  has: () => true,
  get: (t, k) =>
    k in t ? t[k] : k in globalThis ? globalThis[k] : anyStub,
});
vm.createContext(sandbox);
// Append an epilogue that copies the helpers (in lexical scope at end of file)
// into a captured object, since global function decls don't reliably surface on
// a Proxy-contextified global.
vm.runInContext(
  src +
    "\n;Object.assign(__out, { fmtTime, dayOffset, fmtTimeDay, mkDayMark, mkTimeEl });",
  sandbox,
);

const { fmtTime, dayOffset, fmtTimeDay, mkDayMark, mkTimeEl } = realGlobals.__out;

test("dayOffset: seconds-since-midnight → whole days", () => {
  assert.equal(dayOffset(0), 0);
  assert.equal(dayOffset(12 * 3600), 0);
  assert.equal(dayOffset(86399), 0);
  assert.equal(dayOffset(86400), 1);
  assert.equal(dayOffset(169920), 1); // 47:12
  assert.equal(dayOffset(2 * 86400), 2);
  assert.equal(dayOffset(null), 0);
});

test("fmtTime: wall-clock hour, unchanged for same-day", () => {
  assert.equal(fmtTime(12 * 3600), "12:00");
  assert.equal(fmtTime(169920), "23:12"); // 47:12 → 23:12 next day
  assert.equal(fmtTime(25 * 3600 + 30 * 60), "01:30");
  assert.equal(fmtTime(null), "—");
});

test("fmtTimeDay: appends (+N) only past midnight", () => {
  assert.equal(fmtTimeDay(12 * 3600), "12:00"); // same-day: no marker
  assert.equal(fmtTimeDay(169920), "23:12 (+1)");
  assert.equal(fmtTimeDay(25 * 3600 + 30 * 60), "01:30 (+1)");
  assert.equal(fmtTimeDay(2 * 86400 + 90 * 60), "01:30 (+2)");
  assert.equal(fmtTimeDay(null), "—");
});

test("mkDayMark: null same-day, styled superscript otherwise", () => {
  assert.equal(mkDayMark(12 * 3600), null);
  const m1 = mkDayMark(169920);
  assert.equal(m1.tagName, "sup");
  assert.equal(m1.className, "day-sup");
  assert.equal(m1.textContent, "+1");
  assert.equal(mkDayMark(2 * 86400).textContent, "+2");
});

test("mkTimeEl: byte-identical DOM same-day, marker appended past midnight", () => {
  const sameDay = mkTimeEl("span", "big", 12 * 3600);
  assert.equal(sameDay.tagName, "span");
  assert.equal(sameDay.className, "big");
  assert.equal(sameDay.textContent, "12:00");
  assert.equal(sameDay.children.length, 0); // no marker for same-day

  const nextDay = mkTimeEl("b", null, 169920);
  assert.equal(nextDay.textContent, "23:12");
  assert.equal(nextDay.children.length, 1);
  assert.equal(nextDay.children[0].textContent, "+1");
});
