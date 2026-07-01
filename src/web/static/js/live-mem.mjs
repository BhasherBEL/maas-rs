// In-memory fallback store (no SQL) for the Live (realtime) journey view.
//
// Adopted when `openLiveDb()` throws — typically on a LAN phone over plain http,
// where OPFS SAHPool has no secure context. It mirrors the journey/history
// surface the Live bridge needs (saveJourney/getJourney/clearJourney/
// appendEvent/listEvents) and matches the SQL store's OBSERVABLE contract:
// clearing the active journey keeps history, and listEvents returns
// most-recent-first. Imports NOTHING browser-specific, so it runs under Node.

// Detach a value from caller/stored references, mirroring the SQL store's
// JSON round-trip (it persists `payload` as TEXT and re-parses on read), so a
// mutation of a returned object can never corrupt internal state.
function clonePayload(p) {
  return p == null ? null : JSON.parse(JSON.stringify(p));
}

export function makeMemoryStore() {
  let journey = null;
  let events = [];
  let seq = 0;
  return {
    saveJourney(j, { logSelect = true } = {}) {
      journey = {
        id: String(j.id),
        createdAt: j.createdAt ?? new Date().toISOString(),
        payload: clonePayload(j.payload ?? null),
        originLabel: j.originLabel ?? null,
        destinationLabel: j.destinationLabel ?? null,
        status: j.status ?? null,
      };
      if (logSelect) {
        events.push({
          id: ++seq,
          journeyId: journey.id,
          ts: journey.createdAt,
          kind: "select",
          summary: j.summary ?? null,
          payload: clonePayload(j.payload ?? null),
        });
      }
      return journey.id;
    },
    getJourney() {
      return journey ? { ...journey, payload: clonePayload(journey.payload) } : null;
    },
    clearJourney() {
      journey = null;
    },
    appendEvent(evt) {
      events.push({ id: ++seq, ts: new Date().toISOString(), ...evt });
      return seq;
    },
    listEvents({ limit, journeyId } = {}) {
      let rows = events.slice().reverse();
      if (journeyId != null) rows = rows.filter((e) => e.journeyId === String(journeyId));
      return limit != null ? rows.slice(0, limit) : rows;
    },
  };
}
