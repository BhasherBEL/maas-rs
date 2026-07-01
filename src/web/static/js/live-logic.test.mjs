import { test } from "node:test";
import assert from "node:assert/strict";
import {
  secOfDayToHHMM,
  relDelta,
  isConnectionAtRisk,
  etaFromLegs,
  interpolatePosition,
  applyRealtime,
  activeLegAt,
  transferRiskRows,
  applyDepartureChange,
  isLiveStale,
  reliabilityClass,
  reliabilityPct,
  backupRowModel,
  backupConfirmLabel,
  backupSummary,
  planTripSequence,
  isSamePlan,
  dedupeSwitchPlans,
  switchCardModel,
  switchEventSummary,
  chooseVehiclePosition,
  alertSummary,
  transferRiskState,
  RISK_CALM_MARGIN_SECS,
} from "./live-logic.mjs";

const isTransit = (l) => l.kind === "transit";

test("secOfDayToHHMM: basic times", () => {
  assert.equal(secOfDayToHHMM(0), "00:00");
  assert.equal(secOfDayToHHMM(9 * 3600 + 5 * 60), "09:05");
  assert.equal(secOfDayToHHMM(23 * 3600 + 59 * 60), "23:59");
});

test("secOfDayToHHMM: after-midnight (>24h) renders as 25:30", () => {
  assert.equal(secOfDayToHHMM(25 * 3600 + 30 * 60), "25:30");
});

test("secOfDayToHHMM: clamps to 47:59 and floors negatives to 00:00", () => {
  assert.equal(secOfDayToHHMM(48 * 3600), "47:59");
  assert.equal(secOfDayToHHMM(-100), "00:00");
});

test("isLiveStale: online + >= threshold consecutive fails -> stale", () => {
  assert.equal(isLiveStale(2, true), true);
  assert.equal(isLiveStale(3, true), true);
});

test("isLiveStale: online but below threshold -> fresh (single miss tolerated)", () => {
  assert.equal(isLiveStale(0, true), false);
  assert.equal(isLiveStale(1, true), false);
});

test("isLiveStale: offline never stale (net badge owns that signal)", () => {
  assert.equal(isLiveStale(5, false), false);
});

test("isLiveStale: custom threshold respected", () => {
  assert.equal(isLiveStale(1, true, 1), true);
  assert.equal(isLiveStale(0, true, 1), false);
});

test("relDelta: up / down / same", () => {
  assert.deepEqual(relDelta(0.5, 0.8), { dir: "up", oldP: 0.5, newP: 0.8 });
  assert.deepEqual(relDelta(0.8, 0.35), { dir: "down", oldP: 0.8, newP: 0.35 });
  assert.deepEqual(relDelta(0.6, 0.6), { dir: "same", oldP: 0.6, newP: 0.6 });
});

test("isConnectionAtRisk: README 0.80->0.35 (56% drop) is at risk", () => {
  assert.equal(isConnectionAtRisk(0.8, 0.35), true);
});

test("isConnectionAtRisk: 0.80->0.50 (37.5% drop) not at risk at 0.5", () => {
  assert.equal(isConnectionAtRisk(0.8, 0.5), false);
});

test("isConnectionAtRisk: exact threshold boundary is at risk (<=)", () => {
  assert.equal(isConnectionAtRisk(0.8, 0.4), true);
});

test("isConnectionAtRisk: oldP==0 never at risk", () => {
  assert.equal(isConnectionAtRisk(0, 0), false);
});

test("etaFromLegs: last leg with realtimeEnd wins", () => {
  assert.equal(
    etaFromLegs([{ realtimeEnd: 100 }, { realtimeEnd: 200 }, { realtimeEnd: null }]),
    200,
  );
});

test("etaFromLegs: empty and all-null -> null", () => {
  assert.equal(etaFromLegs([]), null);
  assert.equal(etaFromLegs([{ realtimeEnd: null }, { realtimeEnd: null }]), null);
  assert.equal(etaFromLegs(undefined), null);
});

test("interpolatePosition: midpoint of a two-point segment", () => {
  const seg = {
    points: [
      { lat: 0, lng: 0 },
      { lat: 0, lng: 2 },
    ],
    tPrev: 0,
    tNext: 100,
  };
  const p = interpolatePosition(seg, 50);
  assert.equal(p.fraction, 0.5);
  assert.ok(Math.abs(p.lat - 0) < 1e-9);
  assert.ok(Math.abs(p.lng - 1) < 1e-6);
});

test("interpolatePosition: multi-point polyline midpoint", () => {
  // three collinear points along the equator: 0,0 -> 0,1 -> 0,3 (total span lng 3)
  const seg = {
    points: [
      { lat: 0, lng: 0 },
      { lat: 0, lng: 1 },
      { lat: 0, lng: 3 },
    ],
    tPrev: 0,
    tNext: 100,
  };
  const p = interpolatePosition(seg, 50);
  // halfway by distance => lng 1.5 (within the second sub-segment)
  assert.equal(p.fraction, 0.5);
  assert.ok(Math.abs(p.lng - 1.5) < 1e-6, `lng=${p.lng}`);
});

test("interpolatePosition: fraction clamps below 0", () => {
  const seg = { points: [{ lat: 0, lng: 0 }, { lat: 0, lng: 2 }], tPrev: 10, tNext: 20 };
  const p = interpolatePosition(seg, 5);
  assert.equal(p.fraction, 0);
  assert.ok(Math.abs(p.lng - 0) < 1e-9);
});

test("interpolatePosition: fraction clamps above 1", () => {
  const seg = { points: [{ lat: 0, lng: 0 }, { lat: 0, lng: 2 }], tPrev: 10, tNext: 20 };
  const p = interpolatePosition(seg, 999);
  assert.equal(p.fraction, 1);
  assert.ok(Math.abs(p.lng - 2) < 1e-6);
});

test("interpolatePosition: tNext==tPrev returns first point, fraction 0", () => {
  const seg = { points: [{ lat: 1, lng: 1 }, { lat: 2, lng: 2 }], tPrev: 42, tNext: 42 };
  const p = interpolatePosition(seg, 42);
  assert.equal(p.fraction, 0);
  assert.equal(p.lat, 1);
  assert.equal(p.lng, 1);
});

test("applyRealtime: DELAYED transit shifts egress walk and arrival", () => {
  const legs = [
    { kind: "transit", scheduledStart: 100, scheduledEnd: 200, start: 100, end: 200 },
    { kind: "walk", start: 200, end: 260 }, // egress, 60s duration
  ];
  const refresh = {
    legs: [{ status: "DELAYED", scheduledStart: 100, scheduledEnd: 200, realtimeStart: 130, realtimeEnd: 230 }],
  };
  const r = applyRealtime(legs, refresh, isTransit);
  assert.equal(r.legs[0].realtime, true);
  assert.equal(r.legs[0].start, 130);
  assert.equal(r.legs[0].end, 230);
  assert.equal(r.legs[1].start, 230);  // walk follows realtime end
  assert.equal(r.legs[1].end, 290);    // + original 60s duration
  assert.equal(r.transitEta, 230);
  assert.equal(r.arrival, 290);        // egress walk INCLUDED
  assert.equal(legs[0].end, 200);      // pristine input untouched
  assert.equal(legs[1].start, 200);
});

test("applyRealtime: NO_DATA keeps scheduled times and is not marked realtime", () => {
  const legs = [
    { kind: "transit", scheduledStart: 100, scheduledEnd: 200, start: 100, end: 200 },
    { kind: "walk", start: 200, end: 260 },
  ];
  const refresh = {
    legs: [{ status: "NO_DATA", scheduledStart: 100, scheduledEnd: 200, realtimeStart: 100, realtimeEnd: 200 }],
  };
  const r = applyRealtime(legs, refresh, isTransit);
  assert.equal(r.legs[0].realtime, false);
  assert.equal(r.legs[0].liveStatus, "NO_DATA");
  assert.equal(r.legs[0].end, 200);
  assert.equal(r.arrival, 260);
});

test("applyRealtime: access walk before transit keeps its original clock", () => {
  const legs = [
    { kind: "walk", start: 0, end: 60 }, // access
    { kind: "transit", scheduledStart: 100, scheduledEnd: 200, start: 100, end: 200 },
  ];
  const refresh = {
    legs: [{ status: "DELAYED", scheduledStart: 100, scheduledEnd: 200, realtimeStart: 140, realtimeEnd: 250 }],
  };
  const r = applyRealtime(legs, refresh, isTransit);
  assert.equal(r.legs[0].start, 0);   // unshifted
  assert.equal(r.legs[0].end, 60);
  assert.equal(r.legs[1].end, 250);
  assert.equal(r.arrival, 250);       // ends on transit, no egress
  assert.equal(r.transitEta, 250);
});

test("activeLegAt: picks the transit leg whose window contains now", () => {
  const legs = [
    { kind: "walk", start: 0, end: 60 },
    { kind: "transit", start: 60, end: 300 },
    { kind: "walk", start: 300, end: 360 },
    { kind: "transit", start: 360, end: 600 },
  ];
  assert.equal(activeLegAt(legs, 200, isTransit).index, 1);
  assert.equal(activeLegAt(legs, 500, isTransit).index, 3);
  assert.equal(activeLegAt(legs, 60, isTransit).index, 1); // inclusive start
  assert.equal(activeLegAt(legs, 600, isTransit).index, 3); // inclusive end
});

test("activeLegAt: returns null in walk/transfer gaps and out of bounds", () => {
  const legs = [
    { kind: "walk", start: 0, end: 60 },
    { kind: "transit", start: 60, end: 300 },
    { kind: "walk", start: 300, end: 360 },
  ];
  assert.equal(activeLegAt(legs, 30, isTransit), null); // access walk
  assert.equal(activeLegAt(legs, 330, isTransit), null); // egress walk
  assert.equal(activeLegAt(legs, 9999, isTransit), null);
  assert.equal(activeLegAt(undefined, 100, isTransit), null);
});

test("transferRiskRows: baseline is the BOARDED (fromLegIndex+1) leg's reliability", () => {
  const transitLegs = [
    { tripId: "a", transferRisk: { reliability: 0.99 } }, // leg 0 (boarded from access)
    { tripId: "b", transferRisk: { reliability: 0.8 } }, // leg 1 (boarded at transfer 0)
    { tripId: "c", transferRisk: { reliability: 0.9 } }, // leg 2 (boarded at transfer 1)
  ];
  const transfers = [
    { fromLegIndex: 0, reliability: 0.35, marginSecs: 30 }, // into leg 1: 0.8 -> 0.35 at risk
    { fromLegIndex: 1, reliability: 0.88, marginSecs: 200 }, // into leg 2: 0.9 -> 0.88 fine
  ];
  const rows = transferRiskRows(transitLegs, transfers);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].boardedIndex, 1);
  assert.equal(rows[0].baseline, 0.8);
  assert.equal(rows[0].live, 0.35);
  assert.equal(rows[0].atRisk, true);
  assert.deepEqual(rows[0].delta, { dir: "down", oldP: 0.8, newP: 0.35 });
  assert.equal(rows[1].atRisk, false);
  assert.equal(rows[1].marginSecs, 200);
});

test("transferRiskRows: missing baseline/live yields atRisk=false, delta=null", () => {
  const rows = transferRiskRows(
    [{ tripId: "a" }, { tripId: "b" }],
    [{ fromLegIndex: 0, reliability: 0.2 }],
  );
  assert.equal(rows[0].baseline, null);
  assert.equal(rows[0].atRisk, false);
  assert.equal(rows[0].delta, null);
});

test("applyDepartureChange: replaces only the target leg, returns a new descriptor", () => {
  const descriptor = {
    legs: [
      { tripId: "a", boardStopId: "a1", alightStopId: "a2" },
      { tripId: "b", boardStopId: "b1", alightStopId: "b2" },
    ],
  };
  const chosen = { tripId: "B2", from: { stopId: "b1x" }, to: { stopId: "b2x" } };
  const next = applyDepartureChange(descriptor, 1, chosen);
  assert.notEqual(next, descriptor);
  assert.notEqual(next.legs, descriptor.legs);
  assert.deepEqual(next.legs[0], { tripId: "a", boardStopId: "a1", alightStopId: "a2" });
  assert.deepEqual(next.legs[1], { tripId: "B2", boardStopId: "b1x", alightStopId: "b2x" });
  assert.equal(descriptor.legs[1].tripId, "b"); // input untouched
});

test("applyDepartureChange: out-of-range index leaves legs intact", () => {
  const descriptor = { legs: [{ tripId: "a", boardStopId: "a1", alightStopId: "a2" }] };
  const next = applyDepartureChange(descriptor, 5, { tripId: "z" });
  assert.deepEqual(next.legs, descriptor.legs);
});

test("reliabilityClass: high/mid/low buckets and null for unknown", () => {
  assert.equal(reliabilityClass(0.94), "high");
  assert.equal(reliabilityClass(0.8), "high");
  assert.equal(reliabilityClass(0.5), "mid");
  assert.equal(reliabilityClass(0.49), "low");
  assert.equal(reliabilityClass(null), null);
  assert.equal(reliabilityClass(undefined), null);
  assert.equal(reliabilityClass(NaN), null);
});

test("reliabilityPct: rounds, em-dash for unknown (never a false 0%)", () => {
  assert.equal(reliabilityPct(0.94), "94%");
  assert.equal(reliabilityPct(0.005), "1%");
  assert.equal(reliabilityPct(null), "—");
  assert.equal(reliabilityPct(undefined), "—");
});

test("backupRowModel: maps a realtime row, sameLine -> 'next train'", () => {
  const m = backupRowModel({
    tripId: "t1", boardStopId: "b", alightStopId: "a",
    routeShortName: "IC", routeLongName: "InterCity", mode: "Rail",
    routeColor: "1A4FA0", sameLine: true,
    scheduledDeparture: 9 * 3600 + 9 * 60, scheduledArrival: 9 * 3600 + 53 * 60,
    realtimeDeparture: 9 * 3600 + 9 * 60, realtimeArrival: 9 * 3600 + 53 * 60,
    reliability: 0.99,
  });
  assert.equal(m.line, "IC");
  assert.equal(m.sameLine, true);
  assert.equal(m.hint, "next train");
  assert.equal(m.depShifted, false);
  assert.equal(m.relClass, "high");
  assert.equal(m.relPct, "99%");
  assert.equal(m.depSec, 9 * 3600 + 9 * 60);
});

test("backupRowModel: shifted realtime + null reliability -> neutral chip, depShifted true", () => {
  const m = backupRowModel({
    tripId: "t2", boardStopId: "b", alightStopId: "a",
    routeShortName: "S", mode: "Rail", routeColor: null, sameLine: false,
    scheduledDeparture: 9 * 3600 + 5 * 60, scheduledArrival: 9 * 3600 + 46 * 60,
    realtimeDeparture: 9 * 3600 + 7 * 60, realtimeArrival: 9 * 3600 + 48 * 60,
    reliability: null,
  });
  assert.equal(m.hint, "another line");
  assert.equal(m.depShifted, true);
  assert.equal(m.depSec, 9 * 3600 + 7 * 60);
  assert.equal(m.schedDepSec, 9 * 3600 + 5 * 60);
  assert.equal(m.reliability, null);
  assert.equal(m.relClass, null);
  assert.equal(m.relPct, "—");
});

test("backupRowModel: missing realtime falls back to scheduled; null input -> null", () => {
  const m = backupRowModel({
    tripId: "t3", routeShortName: "11", mode: "Tram", sameLine: false,
    scheduledDeparture: 100, scheduledArrival: 200,
    realtimeDeparture: null, realtimeArrival: null, reliability: 0.6,
  });
  assert.equal(m.depSec, 100);
  assert.equal(m.arrSec, 200);
  assert.equal(m.depShifted, false);
  assert.equal(m.relClass, "mid");
  assert.equal(backupRowModel(null), null);
});

test("backupRowModel: line falls back to mode then '?'", () => {
  assert.equal(backupRowModel({ mode: "Bus", sameLine: false }).line, "Bus");
  assert.equal(backupRowModel({ sameLine: false }).line, "?");
});

test("backupConfirmLabel: invites a pick, then reflects keep+backup", () => {
  assert.equal(backupConfirmLabel("IC", null), "Pick a backup");
  const m = backupRowModel({ routeShortName: "S", sameLine: false, scheduledDeparture: 9 * 3600 + 5 * 60 });
  assert.equal(backupConfirmLabel("IC", m), "Keep IC · back up with 09:05 S →");
});

test("backupSummary: human history line includes time, line, pct, station", () => {
  const m = backupRowModel({ routeShortName: "S", sameLine: false, scheduledDeparture: 9 * 3600 + 5 * 60, reliability: 0.94 });
  assert.equal(backupSummary("Bruxelles-Central", "IC", m),
    "Keep IC, backup 09:05 S (94%) at Bruxelles-Central");
  assert.equal(backupSummary("", "IC", null), "Backup cleared");
});

// --- Phase 2b: onboard requery / switch cards -------------------------------

// GraphQL-shaped fixtures (mirror how the functions read legs).
const txLeg = (tripId, o = {}) => ({
  __typename: "PlanTransitLeg",
  tripId,
  end: o.end ?? null,
  to: { node: { name: o.toName ?? null } },
  trip: {
    route: {
      shortName: o.shortName ?? null,
      mode: o.mode ?? null,
      color: o.color ?? null,
      textColor: o.textColor ?? null,
    },
  },
  ...(o.reliability != null ? { transferRisk: { reliability: o.reliability } } : {}),
});
const wkLeg = (o = {}) => ({ __typename: "PlanWalkLeg", end: o.end ?? null });
const planOf = (...legs) => ({ legs });

test("planTripSequence: ordered transit tripIds, walk legs ignored", () => {
  const plan = planOf(wkLeg(), txLeg("t1"), wkLeg(), txLeg("t2"));
  assert.deepEqual(planTripSequence(plan), ["t1", "t2"]);
});

test("planTripSequence: empty for null / no legs / walk-only", () => {
  assert.deepEqual(planTripSequence(null), []);
  assert.deepEqual(planTripSequence({}), []);
  assert.deepEqual(planTripSequence(planOf(wkLeg(), wkLeg())), []);
});

test("isSamePlan: identical trip sequences are the same", () => {
  const a = planOf(txLeg("t1"), wkLeg(), txLeg("t2"));
  const b = planOf(wkLeg(), txLeg("t1"), txLeg("t2"), wkLeg());
  assert.equal(isSamePlan(a, b), true);
});

test("isSamePlan: different or reordered sequences differ", () => {
  assert.equal(isSamePlan(planOf(txLeg("t1")), planOf(txLeg("t2"))), false);
  assert.equal(
    isSamePlan(planOf(txLeg("t1"), txLeg("t2")), planOf(txLeg("t2"), txLeg("t1"))),
    false,
  );
});

test("isSamePlan: two walk-only plans are the same (both empty seq)", () => {
  assert.equal(isSamePlan(planOf(wkLeg()), planOf(wkLeg(), wkLeg())), true);
});

test("dedupeSwitchPlans: drops stay-on, walk-only, and duplicate; keeps distinct in order", () => {
  const stayOn = planOf(txLeg("cur"), txLeg("b")); // matches excludeSeq
  const walkOnly = planOf(wkLeg());
  const altX = planOf(txLeg("cur"), txLeg("x"));
  const altXdup = planOf(txLeg("cur"), txLeg("x")); // same seq as altX
  const altY = planOf(txLeg("cur"), txLeg("y"));
  const out = dedupeSwitchPlans(
    [stayOn, walkOnly, altX, altXdup, altY],
    ["cur", "b"],
  );
  assert.equal(out.length, 2);
  assert.deepEqual(planTripSequence(out[0]), ["cur", "x"]);
  assert.deepEqual(planTripSequence(out[1]), ["cur", "y"]);
  assert.equal(out[0], altX); // first occurrence kept (not the dup)
  assert.equal(out[1], altY);
});

test("dedupeSwitchPlans: tolerates null plans/excludeSeq", () => {
  assert.deepEqual(dedupeSwitchPlans(null, null), []);
  const alt = planOf(txLeg("a"));
  assert.deepEqual(dedupeSwitchPlans([alt], undefined), [alt]);
});

test("switchCardModel: single transit ending in walk -> 'Alight at X + walk'", () => {
  const plan = planOf(
    txLeg("t1", { toName: "Gare X", shortName: "IC", mode: "Rail", color: "1A4FA0", reliability: 0.9 }),
    wkLeg({ end: 200 }),
  );
  const m = switchCardModel(plan);
  assert.equal(m.title, "Alight at Gare X + walk");
  assert.equal(m.arrSec, 200);
  assert.equal(m.reliability, 0.9);
  assert.equal(m.relClass, "high");
  assert.equal(m.relPct, "90%");
  assert.deepEqual(m.tripSeq, ["t1"]);
  assert.equal(m.badges.length, 1);
  assert.deepEqual(m.badges[0], {
    line: "IC", mode: "Rail", routeColor: "1A4FA0", textColor: null,
  });
});

test("switchCardModel: single transit ending on the vehicle -> 'Continue to X'", () => {
  const plan = planOf(txLeg("t1", { toName: "Gare X", shortName: "S", end: 300 }));
  const m = switchCardModel(plan);
  assert.equal(m.title, "Continue to Gare X");
  assert.equal(m.arrSec, 300);
});

test("switchCardModel: two transit legs -> 'Reroute via X', reliability is the product", () => {
  const plan = planOf(
    txLeg("t1", { toName: "Junction", reliability: 0.8 }),
    txLeg("t2", { reliability: 0.5, end: 600 }),
  );
  const m = switchCardModel(plan);
  assert.equal(m.title, "Reroute via Junction");
  assert.ok(Math.abs(m.reliability - 0.4) < 1e-9);
  assert.equal(m.relClass, "low");
  assert.deepEqual(m.tripSeq, ["t1", "t2"]);
});

test("switchCardModel: unknown reliability -> null rel, em-dash pct; badge line falls back to mode then '?'", () => {
  const plan = planOf(txLeg("t1", { toName: "Y", mode: "Bus" }), txLeg("t2"));
  const m = switchCardModel(plan);
  assert.equal(m.reliability, null);
  assert.equal(m.relClass, null);
  assert.equal(m.relPct, "—");
  assert.equal(m.badges[0].line, "Bus");
  assert.equal(m.badges[1].line, "?");
});

test("switchCardModel: walk-only plan and null input", () => {
  const m = switchCardModel(planOf(wkLeg({ end: 120 })));
  assert.equal(m.title, "Walk the rest of the way");
  assert.equal(m.arrSec, 120);
  assert.deepEqual(m.tripSeq, []);
  assert.equal(switchCardModel(null), null);
});

test("switchEventSummary: composes board name + title + arrival", () => {
  const m = switchCardModel(
    planOf(txLeg("t1", { toName: "Gare X", shortName: "IC" }), wkLeg({ end: 9 * 3600 + 53 * 60 })),
  );
  assert.equal(
    switchEventSummary(m, "Bruxelles-Central"),
    "Switched at Bruxelles-Central — Alight at Gare X + walk, arr 09:53",
  );
});

test("switchEventSummary: no board name / no arrival / null model", () => {
  // undefined arrSec (Number(undefined)=NaN) drops the arrival clause
  assert.equal(
    switchEventSummary({ title: "Reroute via Y", arrSec: undefined }, ""),
    "Switched — Reroute via Y",
  );
  assert.equal(switchEventSummary(null, "X"), "Switched route");
});

test("switchEventSummary: null arrSec drops the arrival clause (no 'arr 00:00')", () => {
  // switchCardModel emits arrSec: null when the last leg has no finite end;
  // Number(null)===0 is finite, so it must be omitted, not rendered as 00:00.
  assert.equal(
    switchEventSummary({ title: "Reroute via Z", arrSec: null }, "Stop A"),
    "Switched at Stop A — Reroute via Z",
  );
  assert.equal(
    switchEventSummary({ title: "Reroute via Z", arrSec: undefined }, "Stop A"),
    "Switched at Stop A — Reroute via Z",
  );
  // A genuine finite arrSec (including 0) still renders.
  assert.equal(
    switchEventSummary({ title: "Reroute via Z", arrSec: 9 * 3600 + 5 * 60 }, "Stop A"),
    "Switched at Stop A — Reroute via Z, arr 09:05",
  );
  assert.equal(
    switchEventSummary({ title: "Reroute via Z", arrSec: 0 }, "Stop A"),
    "Switched at Stop A — Reroute via Z, arr 00:00",
  );
});

test("applyRealtime: CANCELED with null realtime falls back to scheduled (no NaN)", () => {
  const legs = [
    { kind: "transit", scheduledStart: 100, scheduledEnd: 200, start: 100, end: 200 },
    { kind: "walk", start: 200, end: 260 },
  ];
  const refresh = {
    legs: [{ status: "CANCELED", scheduledStart: 100, scheduledEnd: 200, realtimeStart: null, realtimeEnd: null }],
  };
  const r = applyRealtime(legs, refresh, isTransit);
  assert.equal(r.legs[0].realtime, false);
  assert.equal(r.legs[0].liveStatus, "CANCELED");
  assert.equal(r.legs[0].end, 200);
  assert.equal(Number.isFinite(r.arrival), true);
  assert.equal(r.arrival, 260);
});

// --- Phase 3: chooseVehiclePosition ------------------------------------------

const interp = { lat: 50.8, lng: 4.35, fraction: 0.5 };

test("chooseVehiclePosition: real+fresh vehicle -> source real, lat/lng from vehicle", () => {
  const v = { lat: 50.85, lng: 4.36, bearing: 90, observedAt: "2026-06-30T10:00:00Z", stale: false };
  const r = chooseVehiclePosition(v, interp);
  assert.equal(r.source, "real");
  assert.equal(r.lat, 50.85);
  assert.equal(r.lng, 4.36);
});

test("chooseVehiclePosition: stale vehicle -> falls back to interpolated", () => {
  const v = { lat: 50.85, lng: 4.36, stale: true };
  const r = chooseVehiclePosition(v, interp);
  assert.equal(r.source, "interpolated");
  assert.equal(r.lat, interp.lat);
  assert.equal(r.lng, interp.lng);
});

test("chooseVehiclePosition: null vehicle -> falls back to interpolated", () => {
  const r = chooseVehiclePosition(null, interp);
  assert.equal(r.source, "interpolated");
  assert.deepEqual({ lat: r.lat, lng: r.lng }, { lat: interp.lat, lng: interp.lng });
});

test("chooseVehiclePosition: stale vehicle + null interpolated -> null", () => {
  const v = { lat: 50.85, lng: 4.36, stale: true };
  assert.equal(chooseVehiclePosition(v, null), null);
});

test("chooseVehiclePosition: null vehicle + null interpolated -> null", () => {
  assert.equal(chooseVehiclePosition(null, null), null);
});

test("chooseVehiclePosition: real+fresh vehicle + null interpolated -> source real (real wins)", () => {
  const v = { lat: 50.85, lng: 4.36, stale: false };
  const r = chooseVehiclePosition(v, null);
  assert.equal(r.source, "real");
  assert.equal(r.lat, 50.85);
});

test("chooseVehiclePosition: non-finite lat -> falls back to interpolated", () => {
  const v = { lat: NaN, lng: 4.36, stale: false };
  const r = chooseVehiclePosition(v, interp);
  assert.equal(r.source, "interpolated");
});

test("chooseVehiclePosition: non-finite lng -> falls back to interpolated", () => {
  const v = { lat: 50.85, lng: Infinity, stale: false };
  const r = chooseVehiclePosition(v, interp);
  assert.equal(r.source, "interpolated");
});

test("chooseVehiclePosition: null lat (not NaN, not finite) -> falls back to interpolated", () => {
  const v = { lat: null, lng: 4.36, stale: false };
  const r = chooseVehiclePosition(v, interp);
  assert.equal(r.source, "interpolated");
});

// --- Phase 4: alertSummary ---------------------------------------------------

test("alertSummary: null for empty/null/undefined input", () => {
  assert.equal(alertSummary([]), null);
  assert.equal(alertSummary(null), null);
  assert.equal(alertSummary(undefined), null);
});

test("alertSummary: single alert returns its header, or null when blank", () => {
  assert.equal(alertSummary([{ header: "Delays on line 1" }]), "Delays on line 1");
  assert.equal(alertSummary([{ header: "" }]), null);
  assert.equal(alertSummary([{}]), null);
});

test("alertSummary: multiple alerts -> first header + +N more suffix", () => {
  assert.equal(alertSummary([{ header: "Alert A" }, { header: "Alert B" }]), "Alert A +1 more");
  assert.equal(alertSummary([{ header: "Alert A" }, {}, {}]), "Alert A +2 more");
});

test("alertSummary: multiple alerts with blank first header -> null", () => {
  assert.equal(alertSummary([{ header: "" }, { header: "Alert B" }]), null);
  assert.equal(alertSummary([{}, { header: "Alert B" }]), null);
});

test("transferRiskState: null/missing riskRow -> calm defaults", () => {
  const r = transferRiskState(null);
  assert.equal(r.state, 'calm');
  assert.equal(r.oldRel, null);
  assert.equal(r.newRel, null);
  assert.equal(r.delta, null);
  assert.equal(r.marginSecs, null);
  assert.equal(transferRiskState(undefined).state, 'calm');
});

test("transferRiskState: healthy transfer (good margin, no drop) -> calm", () => {
  const r = transferRiskState({
    atRisk: false,
    marginSecs: 300,
    baseline: 0.85,
    live: 0.82,
    delta: { oldP: 0.85, newP: 0.82 },
  });
  assert.equal(r.state, 'calm');
  assert.equal(r.oldRel, 0.85);
  assert.equal(r.newRel, 0.82);
  assert.equal(r.marginSecs, 300);
});

test("transferRiskState: >=50% relative reliability drop -> alarm", () => {
  const r = transferRiskState({
    atRisk: true,
    marginSecs: 120,
    baseline: 0.8,
    live: 0.3,
    delta: { oldP: 0.8, newP: 0.3 },
  });
  assert.equal(r.state, 'alarm');
  assert.equal(r.oldRel, 0.8);
  assert.equal(r.newRel, 0.3);
});

test("transferRiskState: non-positive margin -> alarm even without atRisk flag", () => {
  const r = transferRiskState({
    atRisk: false,
    marginSecs: 0,
    baseline: 0.75,
    live: 0.6,
    delta: null,
  });
  assert.equal(r.state, 'alarm');
  const r2 = transferRiskState({ atRisk: false, marginSecs: -60, baseline: 0.7, live: 0.65, delta: null });
  assert.equal(r2.state, 'alarm');
});

test("transferRiskState: positive but tight margin -> open", () => {
  const r = transferRiskState({
    atRisk: false,
    marginSecs: RISK_CALM_MARGIN_SECS - 30,
    baseline: 0.75,
    live: 0.7,
    delta: { oldP: 0.75, newP: 0.7 },
  });
  assert.equal(r.state, 'open');
});

test("transferRiskState: margin exactly at calm threshold -> calm", () => {
  const r = transferRiskState({
    atRisk: false,
    marginSecs: RISK_CALM_MARGIN_SECS,
    baseline: 0.8,
    live: 0.78,
    delta: null,
  });
  assert.equal(r.state, 'calm');
});

test("transferRiskState: null marginSecs with no atRisk -> calm", () => {
  const r = transferRiskState({ atRisk: false, marginSecs: null, baseline: 0.9, live: 0.88, delta: null });
  assert.equal(r.state, 'calm');
});
