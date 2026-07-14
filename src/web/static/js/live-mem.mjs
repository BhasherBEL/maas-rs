// Must mirror the SQL store's observable contract: clearing the active journey
// keeps history, and listEvents returns most-recent-first.
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
