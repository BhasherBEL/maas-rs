import { test } from "node:test";
import assert from "node:assert/strict";
import { makeMemoryStore } from "./live-mem.mjs";

test("saveJourney then getJourney round-trips", () => {
  const s = makeMemoryStore();
  const id = s.saveJourney({
    id: 42,
    createdAt: "2026-06-28T10:00:00.000Z",
    payload: { legs: [] },
    originLabel: "A",
    destinationLabel: "B",
    status: "active",
  });
  assert.equal(id, "42");
  const got = s.getJourney();
  assert.equal(got.id, "42");
  assert.equal(got.createdAt, "2026-06-28T10:00:00.000Z");
  assert.deepEqual(got.payload, { legs: [] });
  assert.equal(got.originLabel, "A");
  assert.equal(got.destinationLabel, "B");
  assert.equal(got.status, "active");
});

test("getJourney returns a COPY: mutating it does not corrupt stored state", () => {
  const s = makeMemoryStore();
  s.saveJourney({ id: 1, originLabel: "A" });
  const a = s.getJourney();
  a.originLabel = "MUTATED";
  a.id = "999";
  const b = s.getJourney();
  assert.equal(b.originLabel, "A");
  assert.equal(b.id, "1");
  assert.notEqual(a, b);
});

test("getJourney deep-copies payload: nested mutation does not corrupt stored state", () => {
  const s = makeMemoryStore();
  const input = { id: 1, payload: { legs: [1] } };
  s.saveJourney(input);
  // Mutating the caller's input after save must not leak in.
  input.payload.legs.push("input-mut");
  const got = s.getJourney();
  got.payload.legs.push("read-mut");
  assert.deepEqual(s.getJourney().payload.legs, [1]);
});

test("second saveJourney overwrites the active journey", () => {
  const s = makeMemoryStore();
  s.saveJourney({ id: 1, originLabel: "first" });
  s.saveJourney({ id: 2, originLabel: "second" });
  const got = s.getJourney();
  assert.equal(got.id, "2");
  assert.equal(got.originLabel, "second");
});

test("clearJourney removes active journey but history (events) survives", () => {
  const s = makeMemoryStore();
  s.saveJourney({ id: 1, originLabel: "A", summary: "selected A" });
  assert.ok(s.getJourney());
  s.clearJourney();
  assert.equal(s.getJourney(), null);
  // The select event recorded by saveJourney must still be listable.
  const events = s.listEvents();
  assert.equal(events.length, 1);
  assert.equal(events[0].kind, "select");
  assert.equal(events[0].journeyId, "1");
});

test("saveJourney with {logSelect:false} replaces journey but logs NO select event", () => {
  const s = makeMemoryStore();
  s.saveJourney({ id: 1, payload: { a: 1 }, originLabel: "A" });
  s.saveJourney({ id: 1, payload: { a: 2 }, originLabel: "A" }, { logSelect: false });
  // Replaced.
  assert.deepEqual(s.getJourney().payload, { a: 2 });
  // Only the first (default) save logged a select.
  const selects = s.listEvents().filter((e) => e.kind === "select");
  assert.equal(selects.length, 1);
});

test("appendEvent + listEvents returns most-recent-first", () => {
  const s = makeMemoryStore();
  s.appendEvent({ kind: "a", journeyId: "1" });
  s.appendEvent({ kind: "b", journeyId: "1" });
  s.appendEvent({ kind: "c", journeyId: "1" });
  const events = s.listEvents();
  assert.deepEqual(
    events.map((e) => e.kind),
    ["c", "b", "a"],
  );
  assert.deepEqual(
    events.map((e) => e.id),
    [3, 2, 1],
  );
});

test("listEvents respects limit (most-recent-first)", () => {
  const s = makeMemoryStore();
  s.appendEvent({ kind: "a" });
  s.appendEvent({ kind: "b" });
  s.appendEvent({ kind: "c" });
  const events = s.listEvents({ limit: 2 });
  assert.equal(events.length, 2);
  assert.deepEqual(
    events.map((e) => e.kind),
    ["c", "b"],
  );
});

test("listEvents filters by journeyId", () => {
  const s = makeMemoryStore();
  s.appendEvent({ kind: "a", journeyId: "1" });
  s.appendEvent({ kind: "b", journeyId: "2" });
  s.appendEvent({ kind: "c", journeyId: "1" });
  const events = s.listEvents({ journeyId: "1" });
  assert.deepEqual(
    events.map((e) => e.kind),
    ["c", "a"],
  );
});

test("getJourney returns null when none saved", () => {
  const s = makeMemoryStore();
  assert.equal(s.getJourney(), null);
});
