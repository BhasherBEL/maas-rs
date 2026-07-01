import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import {
  schemaVersion,
  initSchema,
  saveSelectedJourney,
  getSelectedJourney,
  clearSelectedJourney,
  appendChangeEvent,
  listChangeEvents,
} from "./live-store.mjs";

// Thin adapter wrapping node:sqlite to the store's exec/run/all contract.
// sqlite-wasm's oo1.DB is wrapped equivalently in live-db.mjs.
function makeDb() {
  const sync = new DatabaseSync(":memory:");
  return {
    exec(sql) {
      sync.exec(sql);
    },
    run(sql, params = []) {
      return sync.prepare(sql).run(...params);
    },
    all(sql, params = []) {
      return sync.prepare(sql).all(...params);
    },
  };
}

test("initSchema is idempotent and sets user_version", () => {
  const db = makeDb();
  assert.equal(initSchema(db), schemaVersion);
  assert.doesNotThrow(() => initSchema(db));
  const uv = db.all("PRAGMA user_version")[0];
  assert.equal(Number(uv.user_version), schemaVersion);
  // Tables exist and are queryable.
  assert.deepEqual(db.all("SELECT * FROM selected_journey"), []);
  assert.deepEqual(db.all("SELECT * FROM change_events"), []);
});

test("initSchema rejects a db newer than the supported schema", () => {
  const db = makeDb();
  db.exec(`PRAGMA user_version = ${schemaVersion + 1}`);
  assert.throws(() => initSchema(db), /newer than supported/);
});

test("saveSelectedJourney round-trips payload via getSelectedJourney", () => {
  const db = makeDb();
  initSchema(db);
  const payload = { legs: [{ mode: "WALK", dist: 120 }], descriptor: { v: 1 } };
  saveSelectedJourney(db, {
    id: "j1",
    payload,
    originLabel: "Home",
    destinationLabel: "Office",
    status: "active",
    createdAt: "2026-06-28T08:00:00.000Z",
  });
  const got = getSelectedJourney(db);
  assert.equal(got.id, "j1");
  assert.equal(got.originLabel, "Home");
  assert.equal(got.destinationLabel, "Office");
  assert.equal(got.status, "active");
  assert.deepEqual(got.payload, payload);
});

test("saveSelectedJourney commits atomically: one active row + matching 'select' event", () => {
  const db = makeDb();
  initSchema(db);
  saveSelectedJourney(db, {
    id: "j1",
    payload: { a: 1 },
    originLabel: "Home",
    destinationLabel: "Office",
    createdAt: "2026-06-28T08:00:00.000Z",
  });
  assert.equal(db.all("SELECT COUNT(*) AS n FROM selected_journey")[0].n, 1);
  const selects = listChangeEvents(db).filter((e) => e.kind === "select");
  assert.equal(selects.length, 1);
  assert.equal(selects[0].journeyId, "j1");
});

test("saveSelectedJourney with {logSelect:false} replaces journey but logs NO select event", () => {
  const db = makeDb();
  initSchema(db);
  saveSelectedJourney(db, {
    id: "j1",
    payload: { a: 1 },
    originLabel: "Home",
    destinationLabel: "Office",
    createdAt: "2026-06-28T08:00:00.000Z",
  });
  // Edit-time persistence: silent, must NOT append a duplicate generic select.
  saveSelectedJourney(
    db,
    { id: "j1", payload: { a: 2 }, originLabel: "Home", destinationLabel: "Office" },
    { logSelect: false },
  );
  // Journey IS replaced (latest payload active).
  assert.equal(db.all("SELECT COUNT(*) AS n FROM selected_journey")[0].n, 1);
  assert.deepEqual(getSelectedJourney(db).payload, { a: 2 });
  // Only the first (default) save logged a select; the silent one did not.
  const selects = listChangeEvents(db).filter((e) => e.kind === "select");
  assert.equal(selects.length, 1);
});

test("getSelectedJourney returns null when none selected", () => {
  const db = makeDb();
  initSchema(db);
  assert.equal(getSelectedJourney(db), null);
});

test("saving a second journey replaces the first; both saves recorded in history", () => {
  const db = makeDb();
  initSchema(db);
  saveSelectedJourney(db, {
    id: "j1",
    payload: { a: 1 },
    originLabel: "A",
    destinationLabel: "B",
    createdAt: "2026-06-28T08:00:00.000Z",
  });
  saveSelectedJourney(db, {
    id: "j2",
    payload: { a: 2 },
    originLabel: "C",
    destinationLabel: "D",
    createdAt: "2026-06-28T09:00:00.000Z",
  });
  // Only one active journey, and it is the latest.
  assert.equal(db.all("SELECT COUNT(*) AS n FROM selected_journey")[0].n, 1);
  assert.equal(getSelectedJourney(db).id, "j2");
  // Both selects are in history, most-recent-first.
  const hist = listChangeEvents(db);
  const selects = hist.filter((e) => e.kind === "select");
  assert.equal(selects.length, 2);
  assert.equal(selects[0].journeyId, "j2");
  assert.equal(selects[1].journeyId, "j1");
});

test("appendChangeEvent + listChangeEvents: most-recent-first, limit, journeyId filter", () => {
  const db = makeDb();
  initSchema(db);
  appendChangeEvent(db, { journeyId: "j1", kind: "select", summary: "s1", ts: "2026-06-28T08:00:00.000Z" });
  appendChangeEvent(db, { journeyId: "j1", kind: "departure_change", summary: "s2", ts: "2026-06-28T08:05:00.000Z" });
  appendChangeEvent(db, { journeyId: "j2", kind: "walk_bike_change", summary: "s3", ts: "2026-06-28T08:10:00.000Z" });

  const all = listChangeEvents(db);
  assert.deepEqual(all.map((e) => e.summary), ["s3", "s2", "s1"]);

  const limited = listChangeEvents(db, { limit: 2 });
  assert.deepEqual(limited.map((e) => e.summary), ["s3", "s2"]);

  const onlyJ1 = listChangeEvents(db, { journeyId: "j1" });
  assert.deepEqual(onlyJ1.map((e) => e.summary), ["s2", "s1"]);

  const onlyJ1Limited = listChangeEvents(db, { journeyId: "j1", limit: 1 });
  assert.deepEqual(onlyJ1Limited.map((e) => e.summary), ["s2"]);
});

test("same-ts events keep insertion order via id tie-break", () => {
  const db = makeDb();
  initSchema(db);
  const ts = "2026-06-28T08:00:00.000Z";
  appendChangeEvent(db, { journeyId: "j1", kind: "status_update", summary: "first", ts });
  appendChangeEvent(db, { journeyId: "j1", kind: "status_update", summary: "second", ts });
  const hist = listChangeEvents(db);
  assert.deepEqual(hist.map((e) => e.summary), ["second", "first"]);
});

test("clearSelectedJourney removes active journey but leaves history intact", () => {
  const db = makeDb();
  initSchema(db);
  saveSelectedJourney(db, {
    id: "j1",
    payload: { a: 1 },
    originLabel: "A",
    destinationLabel: "B",
  });
  appendChangeEvent(db, { journeyId: "j1", kind: "status_update", summary: "moving" });
  clearSelectedJourney(db);
  assert.equal(getSelectedJourney(db), null);
  const hist = listChangeEvents(db);
  // 'select' (from save) + 'status_update' survive.
  assert.equal(hist.length, 2);
  assert.ok(hist.some((e) => e.kind === "select"));
  assert.ok(hist.some((e) => e.kind === "status_update"));
});

test("JSON payloads survive round-trip in both tables", () => {
  const db = makeDb();
  initSchema(db);
  const nested = { arr: [1, 2, { x: "é", nul: null }], flag: true, n: 3.5 };
  saveSelectedJourney(db, { id: "j1", payload: nested });
  assert.deepEqual(getSelectedJourney(db).payload, nested);

  appendChangeEvent(db, { journeyId: "j1", kind: "alternative_confirmed", payload: nested });
  const evt = listChangeEvents(db, { journeyId: "j1" }).find(
    (e) => e.kind === "alternative_confirmed",
  );
  assert.deepEqual(evt.payload, nested);
});
