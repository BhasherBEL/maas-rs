export function secOfDayToHHMM(sec) {
  const s = Math.max(0, Math.floor(Number(sec) || 0));
  const total = Math.min(s, 47 * 3600 + 59 * 60 + 59);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

export function relDelta(oldP, newP) {
  const o = Number(oldP);
  const n = Number(newP);
  let dir = "same";
  if (n > o) dir = "up";
  else if (n < o) dir = "down";
  return { dir, oldP: o, newP: n };
}

// `null` when unknown so callers render a neutral chip, never a false "low".
export function reliabilityClass(p) {
  if (p == null || !Number.isFinite(Number(p))) return null;
  const v = Number(p);
  return v >= 0.8 ? "high" : v >= 0.5 ? "mid" : "low";
}

export function reliabilityPct(p) {
  if (p == null || !Number.isFinite(Number(p))) return "—";
  return Math.round(Number(p) * 100) + "%";
}

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

export function backupConfirmLabel(mainLine, model) {
  if (!model) return "Pick a backup";
  return `Keep ${mainLine || "your plan"} · back up with ${secOfDayToHHMM(model.depSec)} ${model.line} →`;
}

export function backupSummary(boardName, mainLine, model) {
  if (!model) return "Backup cleared";
  const where = boardName ? ` at ${boardName}` : "";
  return `Keep ${mainLine || "your plan"}, backup ${secOfDayToHHMM(model.depSec)} ${model.line} (${model.relPct})${where}`;
}

// Relative drop: true when newP <= oldP * (1 - threshold).
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

export function isLiveStale(consecutiveFails, online, threshold = 2) {
  if (!online) return false;
  return Number(consecutiveFails) >= threshold;
}

export function etaFromLegs(legs) {
  if (!Array.isArray(legs)) return null;
  for (let i = legs.length - 1; i >= 0; i--) {
    const e = legs[i] && legs[i].realtimeEnd;
    if (e != null && Number.isFinite(Number(e))) return Number(e);
  }
  return null;
}

// Returns a NEW array; never mutates input. Callers MUST start from the pristine
// plan each poll so durations don't drift. A walk leg after a transit leg shifts
// its start to the preceding realtime end, keeping its original duration.
export function applyRealtime(legs, refresh, isTransit) {
  const src = Array.isArray(legs) ? legs : [];
  const rt = (refresh && Array.isArray(refresh.legs)) ? refresh.legs : [];
  const num = (v) => (v != null && Number.isFinite(Number(v)) ? Number(v) : null);

  let ti = 0;
  let shift = null;
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

// A transfer at `fromLegIndex=k` is the connection INTO transit leg `k+1`, so its
// baseline is that leg's transferRisk.reliability.
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

// `descriptor.legs` is indexed in TRANSIT order. Returns a NEW descriptor; never
// mutates the old one so the pristine journey stays intact for re-render.
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

function isTransitLeg(l) {
  return !!l && l.__typename === "PlanTransitLeg";
}

export function planTripSequence(plan) {
  if (!plan || !Array.isArray(plan.legs)) return [];
  return plan.legs.filter(isTransitLeg).map((l) => l.tripId);
}

export function isSamePlan(a, b) {
  return planTripSequence(a).join("|") === planTripSequence(b).join("|");
}

// Drops the stay-on plan (trip sequence == excludeSeq), plans with no transit
// leg, and later duplicate trip sequences.
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

export function switchEventSummary(model, boardName) {
  if (!model) return "Switched route";
  const where = boardName ? ` at ${boardName}` : "";
  const arr = model.arrSec != null && Number.isFinite(Number(model.arrSec)) ? `, arr ${secOfDayToHHMM(model.arrSec)}` : "";
  return `Switched${where} — ${model.title}${arr}`;
}

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

// segment = { points: [{lat,lng}...] (>=2), tPrev, tNext } with REALTIME times.
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
