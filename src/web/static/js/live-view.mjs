// Live-view bridge: THIN glue between the classic inline planner script and the
// tested pure modules (live-logic, live-store, live-db).
//
// A module script is DEFERRED and runs AFTER the classic inline <script>, so the
// classic code must not touch `window.MaaSLive` synchronously at load — it gates
// on `window.MaaSLive.ready` / the `maaslive:ready` event dispatched here.
//
// Persistence degrades gracefully: OPFS SAHPool needs a SECURE CONTEXT (https or
// localhost). Over plain http on a LAN phone it throws; we catch that and fall
// back to an in-memory journey store so the Live view keeps working — only the
// history (which needs a real DB) is unavailable (`MaaSLive.persistent === false`).

import * as liveLogic from "./live-logic.mjs";
import { openLiveDb } from "./live-db.mjs";
import {
  saveSelectedJourney,
  getSelectedJourney,
  clearSelectedJourney,
  appendChangeEvent,
  listChangeEvents,
} from "./live-store.mjs";
import { makeMemoryStore } from "./live-mem.mjs";

const logic = { ...liveLogic };

// ── DB-backed store (wraps live-store against the live-db adapter) ─
function makeDbStore(db) {
  return {
    saveJourney(j, opts) {
      return saveSelectedJourney(db, j, opts);
    },
    getJourney() {
      return getSelectedJourney(db);
    },
    clearJourney() {
      return clearSelectedJourney(db);
    },
    appendEvent(evt) {
      return appendChangeEvent(db, evt);
    },
    listEvents(opts) {
      return listChangeEvents(db, opts);
    },
  };
}

const MaaSLive = {
  ready: false,
  persistent: false,
  logic,
  // Filled in once a store (durable or in-memory) is available.
  saveJourney() {
    throw new Error("MaaSLive not ready");
  },
  getJourney() {
    return null;
  },
  clearJourney() {},
  appendEvent() {},
  listEvents() {
    return [];
  },
};
window.MaaSLive = MaaSLive;

function adopt(store, persistent) {
  MaaSLive.persistent = persistent;
  MaaSLive.saveJourney = (j, opts) => store.saveJourney(j, opts);
  MaaSLive.getJourney = () => store.getJourney();
  MaaSLive.clearJourney = () => store.clearJourney();
  MaaSLive.appendEvent = (e) => store.appendEvent(e);
  MaaSLive.listEvents = (o) => store.listEvents(o);
  MaaSLive.ready = true;
  window.dispatchEvent(new CustomEvent("maaslive:ready", { detail: { persistent } }));
}

(async () => {
  try {
    const db = await openLiveDb();
    adopt(makeDbStore(db), true);
  } catch (err) {
    // Secure-context / OPFS failure (typical on LAN http) — never brick the view.
    console.warn("MaaSLive: persistent storage unavailable, using in-memory store:", err);
    adopt(makeMemoryStore(), false);
  }
})();
