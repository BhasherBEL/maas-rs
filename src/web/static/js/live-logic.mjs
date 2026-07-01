// Pure logic for the Live (realtime) journey view.
// No DOM, no browser globals, no fetch — importable in both Node (node:test)
// and the browser (<script type="module">).

/** Format seconds-since-midnight as "HH:MM", clamped to a 0–47h display window. */
export function secOfDayToHHMM(sec) {
  const s = Math.max(0, Math.floor(Number(sec) || 0));
  const total = Math.min(s, 47 * 3600 + 59 * 60 + 59);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

/** Describe a reliability change old→new as a direction plus the raw probabilities. */
export function relDelta(oldP, newP) {
  const o = Number(oldP);
  const n = Number(newP);
  let dir = "same";
  if (n > o) dir = "up";
  else if (n < o) dir = "down";
  return { dir, oldP: o, newP: n };
}

/**
 * Reliability bucket for the shared --ds-rel-* scale. `null` when the probability
 * is unknown (Option<f64> from the backend) — callers render a neutral chip then,
 * never a false red "low". ≥0.8 high · ≥0.5 mid · else low.
 */
export function reliabilityClass(p) {
  if (p == null || !Number.isFinite(Number(p))) return null;
  const v = Number(p);
  return v >= 0.8 ? "high" : v >= 0.5 ? "mid" : "low";
}

/** Reliability as a whole-percent string, or an em-dash when unknown. */
export function reliabilityPct(p) {
  if (p == null || !Number.isFinite(Number(p))) return "—";
  return Math.round(Number(p) * 100) + "%";
}

/**
 * Normalize one raw `stationBackups` row into a render-ready view-model. Pure:
 * carries the raw `routeColor`/`mode` so the DOM layer (maas.js globals
 * `routeColors`/`mkModeIc`) builds the badge — no DOM here. Times are seconds of
 * day; `depShifted` is true when realtime departure moved off schedule (RelDelta
 * struck-time treatment). Unknown reliability stays `null` (neutral chip).
 */
export function backupRowModel(b) {
  if (!b) return null;
  const num = (v) => (v != null && Number.isFinite(Number(v)) ? Number(v) : null);
  const schedDep = num(b.scheduledDeparture);
  const dep = num(b.realtimeDeparture) ?? schedDep;
  const arr = num(b.realtimeArrival) ?? num(b.scheduledArrival);
  const rel = num(b.reliability);
  return {
    tripId: b.tripId,
    boardStopId: b.boardStopId,
    alightStopId: b.alightStopId,
    line: b.routeShortName || b.mode || "?",
    mode: b.mode ?? null,
    routeColor: b.routeColor ?? null,
    sameLine: !!b.sameLine,
    hint: b.sameLine ? "next train" : "another line",
    depSec: dep,
    schedDepSec: schedDep,
    depShifted: schedDep != null && dep != null && dep !== schedDep,
    arrSec: arr,
    reliability: rel,
    relClass: reliabilityClass(rel),
    relPct: reliabilityPct(rel),
  };
}

/**
 * Label for the rail's confirm button. Card 1 keeps the main plan and only sets a
 * net, so the copy reflects BOTH: "Keep <main> · back up with <09:05 S> →". With
 * no backup chosen yet it invites a pick (the DOM disables the button then).
 */
export function backupConfirmLabel(mainLine, model) {
  if (!model) return "Pick a backup";
  return `Keep ${mainLine || "your plan"} · back up with ${secOfDayToHHMM(model.depSec)} ${model.line} →`;
}

/** Human one-liner for the change_event history row when a backup net is set. */
export function backupSummary(boardName, mainLine, model) {
  if (!model) return "Backup cleared";
  const where = boardName ? ` at ${boardName}` : "";
  return `Keep ${mainLine || "your plan"}, backup ${secOfDayToHHMM(model.depSec)} ${model.line} (${model.relPct})${where}`;
}

/**
 * True when reliability fell by >= `threshold` fraction of its old value
 * (relative drop): newP <= oldP * (1 - threshold).
 */
export function isConnectionAtRisk(oldP, newP, threshold = 0.5) {
  const o = Number(oldP);
  const n = Number(newP);
  if (!(o > 0)) return false;
  return n <= o * (1 - threshold);
}

export const RISK_ALARM_DROP_THRESHOLD = 0.5;
export const RISK_CALM_MARGIN_SECS = 180;

export function transferRiskState(riskRow) {
  if (!riskRow) return { state: 'calm', oldRel: null, newRel: null, delta: null, marginSecs: null };
  const { atRisk, marginSecs, baseline, live, delta } = riskRow;
  const marginAlarm = marginSecs != null && marginSecs <= 0;
  if (atRisk || marginAlarm) {
    return { state: 'alarm', oldRel: baseline, newRel: live, delta, marginSecs };
  }
  if (marginSecs != null && marginSecs < RISK_CALM_MARGIN_SECS) {
    return { state: 'open', oldRel: baseline, newRel: live, delta, marginSecs };
  }
  return { state: 'calm', oldRel: baseline, newRel: live, delta, marginSecs };
}

/**
 * Whether the live data should be shown as STALE (poll silently failing while
 * the device still claims to be online). Offline is the net-badge's job, so it
 * never counts as stale here. Goes stale once `consecutiveFails` reaches
 * `threshold` (default 2) — a single transient miss doesn't flip it.
 */
export function isLiveStale(consecutiveFails, online, threshold = 2) {
  if (!online) return false;
  return Number(consecutiveFails) >= threshold;
}

/** Realtime ETA (seconds) = realtimeEnd of the LAST leg that has one, else null. */
export function etaFromLegs(legs) {
  if (!Array.isArray(legs)) return null;
  for (let i = legs.length - 1; i >= 0; i--) {
    const e = legs[i] && legs[i].realtimeEnd;
    if (e != null && Number.isFinite(Number(e))) return Number(e);
  }
  return null;
}

/**
 * Project a realtime refresh onto an ordered list of plan legs, returning a NEW
 * array (never mutating the input — callers MUST start from the pristine plan
 * each poll so durations don't drift) plus the recomputed journey arrival.
 *
 * legs    : ordered plan legs. Transit legs carry {scheduledStart,scheduledEnd,
 *           start,end}; walk legs carry {start,end}. Identified by a `transit`
 *           predicate so this stays DOM/typename-agnostic and unit-testable.
 * refresh : { legs:[{status, scheduledStart, scheduledEnd, realtimeStart,
 *           realtimeEnd}] } — one entry per transit leg, in transit order.
 * isTransit(leg) → bool.
 *
 * Rules:
 *  - ON_TIME / DELAYED → realtime=true, use realtime{Start,End}.
 *  - NO_DATA / NOT_FOUND / CANCELED / missing times → realtime=false, fall back
 *    to scheduled (or original) times so nothing renders a false "on schedule".
 *  - A walk leg AFTER a transit leg shifts: its start follows the preceding
 *    (transit or walk) realtime end; end = start + its ORIGINAL duration. Walk
 *    legs BEFORE the first transit (access) keep their original clock.
 *
 * Returns { legs, arrival, transitEta }:
 *  - arrival    = last leg's realtime end (egress walk INCLUDED).
 *  - transitEta = realtime end of the last transit leg (egress NOT included).
 */
export function applyRealtime(legs, refresh, isTransit) {
  const src = Array.isArray(legs) ? legs : [];
  const rt = (refresh && Array.isArray(refresh.legs)) ? refresh.legs : [];
  const num = (v) => (v != null && Number.isFinite(Number(v)) ? Number(v) : null);

  let ti = 0;
  let shift = null;        // realtime end carried forward from the last leg
  let transitEta = null;
  const out = src.map((leg) => {
    const next = { ...leg };
    if (isTransit(leg)) {
      const r = rt[ti++] || {};
      const live = r.status === "ON_TIME" || r.status === "DELAYED";
      const schedStart = num(r.scheduledStart) ?? num(leg.scheduledStart) ?? num(leg.start);
      const schedEnd = num(r.scheduledEnd) ?? num(leg.scheduledEnd) ?? num(leg.end);
      const realStart = live ? (num(r.realtimeStart) ?? schedStart) : schedStart;
      const realEnd = live ? (num(r.realtimeEnd) ?? schedEnd) : schedEnd;
      next.scheduledStart = schedStart;
      next.scheduledEnd = schedEnd;
      next.start = realStart;
      next.end = realEnd;
      next.realtime = live;
      next.liveStatus = r.status ?? "NO_DATA";
      shift = realEnd;
      transitEta = realEnd;
    } else {
      const dur = (num(leg.end) ?? 0) - (num(leg.start) ?? 0);
      if (shift != null) {
        next.start = shift;
        next.end = shift + dur;
        shift = next.end;
      }
    }
    return next;
  });

  const arrival = out.length ? num(out[out.length - 1].end) : null;
  return { legs: out, arrival, transitEta };
}

/**
 * The transit leg whose [start,end] realtime window contains `now` (the vehicle
 * the rider is currently aboard). Returns { leg, index } into the SAME `legs`
 * array (so callers can read its geometry), or null in an access/transfer/egress
 * gap or before/after the journey. Times are seconds-of-day; a leg with `start`
 * == `end` still matches at that instant.
 */
export function activeLegAt(legs, now, isTransit) {
  if (!Array.isArray(legs) || typeof isTransit !== "function") return null;
  const t = Number(now);
  if (!Number.isFinite(t)) return null;
  for (let i = 0; i < legs.length; i++) {
    const leg = legs[i];
    if (!isTransit(leg)) continue;
    const s = Number(leg.start);
    const e = Number(leg.end);
    if (Number.isFinite(s) && Number.isFinite(e) && t >= s && t <= e) {
      return { leg, index: i };
    }
  }
  return null;
}

/**
 * Reduce live transfer reliabilities against the journey's BASELINE (the
 * reliability captured at select time on the boarded — i.e. arriving — leg).
 *
 * `transitLegs` : pristine plan transit legs, in transit order, each carrying
 *                 `transferRisk.reliability` (the catch-this-leg odds).
 * `transfers`   : liveRefresh `transfers[]`, each `{fromLegIndex, reliability,
 *                 marginSecs}`. A transfer at `fromLegIndex=k` is the connection
 *                 INTO transit leg `k+1`, so its baseline is that leg's risk.
 *
 * Returns one row per transfer: { fromLegIndex, boardedIndex, baseline, live,
 * atRisk (relative ≥50% drop), delta (relDelta or null), marginSecs }.
 */
export function transferRiskRows(transitLegs, transfers) {
  const tl = Array.isArray(transitLegs) ? transitLegs : [];
  const tr = Array.isArray(transfers) ? transfers : [];
  return tr.map((t) => {
    const boardedIndex = Number(t.fromLegIndex) + 1;
    const boarded = tl[boardedIndex];
    const b = boarded && boarded.transferRisk ? Number(boarded.transferRisk.reliability) : NaN;
    const l = t.reliability != null ? Number(t.reliability) : NaN;
    const baseline = Number.isFinite(b) ? b : null;
    const live = Number.isFinite(l) ? l : null;
    const known = baseline != null && live != null;
    return {
      fromLegIndex: Number(t.fromLegIndex),
      boardedIndex,
      baseline,
      live,
      atRisk: known ? isConnectionAtRisk(baseline, live) : false,
      delta: known ? relDelta(baseline, live) : null,
      marginSecs: t.marginSecs != null ? Number(t.marginSecs) : null,
    };
  });
}

/**
 * Pure descriptor edit: replace the transit leg at `transitIndex` with the trip
 * the rider switched to. `descriptor.legs` is indexed in TRANSIT order
 * ({tripId, boardStopId, alightStopId}); `chosen` is a PlanTransitLeg-shaped
 * object (or already a descriptor leg). Returns a NEW descriptor — the old one
 * is never mutated so the pristine journey stays intact for re-render.
 */
export function applyDepartureChange(descriptor, transitIndex, chosen) {
  const legs = descriptor && Array.isArray(descriptor.legs) ? descriptor.legs.slice() : [];
  if (transitIndex < 0 || transitIndex >= legs.length || !chosen) {
    return { ...(descriptor || {}), legs };
  }
  const prev = legs[transitIndex] || {};
  legs[transitIndex] = {
    tripId: chosen.tripId ?? prev.tripId,
    boardStopId: chosen.boardStopId ?? (chosen.from && chosen.from.stopId) ?? prev.boardStopId,
    alightStopId: chosen.alightStopId ?? (chosen.to && chosen.to.stopId) ?? prev.alightStopId,
  };
  return { ...descriptor, legs };
}

/** True for a GraphQL plan leg that is a transit (vehicle) leg. */
function isTransitLeg(l) {
  return !!l && l.__typename === "PlanTransitLeg";
}

/**
 * Ordered tripIds of a plan's transit legs — the journey's "trip sequence", used
 * to dedup onboard alternatives and detect the stay-on plan. Walk/bike legs do
 * not contribute (they carry no tripId).
 */
export function planTripSequence(plan) {
  if (!plan || !Array.isArray(plan.legs)) return [];
  return plan.legs.filter(isTransitLeg).map((l) => l.tripId);
}

/** Two plans are "the same" when their transit trip sequences are identical. */
export function isSamePlan(a, b) {
  return planTripSequence(a).join("|") === planTripSequence(b).join("|");
}

/**
 * Filter raw onboard `raptor` alternatives down to the DISTINCT switch plans.
 * Every onboard alternative starts on the rider's current trip, so the stay-on
 * plan's trip sequence equals `excludeSeq` (the journey's transit suffix from the
 * boarded leg onward) — that one is dropped, as is any plan with no transit leg
 * and any later duplicate trip sequence. Pure: returns a new array of the kept
 * plan objects in input order.
 */
export function dedupeSwitchPlans(plans, excludeSeq) {
  const ex = Array.isArray(excludeSeq) ? excludeSeq.join("|") : "";
  const seen = new Set([ex]);
  const out = [];
  (Array.isArray(plans) ? plans : []).forEach((p) => {
    const seq = planTripSequence(p);
    if (!seq.length) return;
    const key = seq.join("|");
    if (seen.has(key)) return;
    seen.add(key);
    out.push(p);
  });
  return out;
}

/**
 * Render-ready view-model for one switch (Card 2+) of the live alternatives rail.
 * Pure: carries raw `routeColor`/`textColor`/`mode` per badge so the DOM layer
 * builds the line badges (maas.js `routeColors`). The card's discriminator is the
 * stop where the rider leaves the current vehicle (`transit[0].to`): titled
 * "Reroute via X" when another vehicle follows, "Alight at X + walk" when the
 * rest is on foot. `arrSec` is the plan's final arrival; reliability is the
 * product of per-leg `transferRisk.reliability` (null when none is known).
 */
export function switchCardModel(plan) {
  if (!plan || !Array.isArray(plan.legs)) return null;
  const transit = plan.legs.filter(isTransitLeg);
  const badges = transit.map((l) => ({
    line: l.trip?.route?.shortName || l.trip?.route?.mode || "?",
    mode: l.trip?.route?.mode ?? null,
    routeColor: l.trip?.route?.color ?? null,
    textColor: l.trip?.route?.textColor ?? null,
  }));
  const key = transit.length ? transit[0].to?.node?.name || null : null;
  const last = plan.legs[plan.legs.length - 1];
  const endsWalk = !!last && last.__typename === "PlanWalkLeg";
  let title;
  if (!transit.length) title = "Walk the rest of the way";
  else if (transit.length >= 2) title = key ? `Reroute via ${key}` : "Reroute";
  else if (key) title = endsWalk ? `Alight at ${key} + walk` : `Continue to ${key}`;
  else title = "Alternative route";
  let rel = 1;
  let known = false;
  transit.forEach((l) => {
    const r = l.transferRisk?.reliability;
    if (r != null && Number.isFinite(Number(r))) {
      rel *= Number(r);
      known = true;
    }
  });
  const reliability = known ? rel : null;
  const arrSec = last && Number.isFinite(Number(last.end)) ? Number(last.end) : null;
  return {
    title,
    arrSec,
    reliability,
    relClass: reliabilityClass(reliability),
    relPct: reliabilityPct(reliability),
    badges,
    tripSeq: transit.map((l) => l.tripId),
  };
}

/** Human one-liner for the change_event history row when a switch is confirmed. */
export function switchEventSummary(model, boardName) {
  if (!model) return "Switched route";
  const where = boardName ? ` at ${boardName}` : "";
  const arr = model.arrSec != null && Number.isFinite(Number(model.arrSec)) ? `, arr ${secOfDayToHHMM(model.arrSec)}` : "";
  return `Switched${where} — ${model.title}${arr}`;
}

/** Haversine distance in metres between two {lat,lng} points. */
function haversineM(a, b) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

/**
 * Interpolate the live vehicle position along a polyline between two stops.
 * segment = { points: [{lat,lng}...] (>=2), tPrev, tNext } with REALTIME times.
 * Returns { lat, lng, fraction }. Distance uses haversine (accurate for the
 * short inter-stop shapes here). tNext==tPrev → first point, fraction 0.
 */
export function interpolatePosition(segment, now) {
  const points = (segment && segment.points) || [];
  if (points.length === 0) return { lat: 0, lng: 0, fraction: 0 };
  if (points.length === 1) {
    return { lat: points[0].lat, lng: points[0].lng, fraction: 0 };
  }

  const tPrev = Number(segment.tPrev);
  const tNext = Number(segment.tNext);
  let fraction;
  if (tNext === tPrev) {
    fraction = 0;
  } else {
    fraction = (Number(now) - tPrev) / (tNext - tPrev);
    fraction = Math.max(0, Math.min(1, fraction));
  }

  const segLens = [];
  let total = 0;
  for (let i = 1; i < points.length; i++) {
    const d = haversineM(points[i - 1], points[i]);
    segLens.push(d);
    total += d;
  }
  if (total === 0) {
    return { lat: points[0].lat, lng: points[0].lng, fraction };
  }

  let target = fraction * total;
  for (let i = 0; i < segLens.length; i++) {
    if (target <= segLens[i] || i === segLens.length - 1) {
      const segFrac = segLens[i] === 0 ? 0 : target / segLens[i];
      const a = points[i];
      const b = points[i + 1];
      return {
        lat: a.lat + (b.lat - a.lat) * segFrac,
        lng: a.lng + (b.lng - a.lng) * segFrac,
        fraction,
      };
    }
    target -= segLens[i];
  }
  const last = points[points.length - 1];
  return { lat: last.lat, lng: last.lng, fraction };
}

/**
 * Short label for one or more service alerts on a transit leg. Returns the
 * first alert's header; when multiple alerts are present appends "+N more".
 * Returns null when the list is empty or the first header is blank.
 */
export function alertSummary(alerts) {
  if (!Array.isArray(alerts) || !alerts.length) return null;
  const first = alerts[0].header || null;
  if (alerts.length === 1) return first;
  return first != null ? `${first} +${alerts.length - 1} more` : null;
}

export function chooseVehiclePosition(vehicle, interpolated) {
  if (
    vehicle != null &&
    vehicle.stale === false &&
    Number.isFinite(vehicle.lat) &&
    Number.isFinite(vehicle.lng)
  ) {
    return { lat: vehicle.lat, lng: vehicle.lng, source: 'real' };
  }
  if (interpolated == null) return null;
  return { ...interpolated, source: 'interpolated' };
}
