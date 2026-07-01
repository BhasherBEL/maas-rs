// Browser bootstrap: persistent SQLite via sqlite-wasm + OPFS SAHPool VFS.
//
// SAHPool runs entirely on the main thread and needs NO COOP/COEP
// cross-origin-isolation headers (which would break map-tile loading) — that is
// the whole reason we use installOpfsSAHPoolVfs rather than the worker OPFS VFS.
//
// Exposes the exec/run/all adapter that live-store.mjs is written against, so
// the same store + schema run here and under node:sqlite in the tests.

import sqlite3InitModule from "./vendor/sqlite-wasm/sqlite3.mjs";
import * as store from "./live-store.mjs";

const WASM_BASE = "/static/js/vendor/sqlite-wasm/";
const DB_NAME = "/maas-live.sqlite3";
const VFS_NAME = "maas-live";

let _adapterPromise = null;

// Wrap an sqlite3 oo1.DB into the exec/run/all adapter contract.
function wrap(oodb) {
  return {
    exec(sql) {
      oodb.exec(sql);
    },
    run(sql, params = []) {
      oodb.exec({ sql, bind: params });
      return {
        lastInsertRowid: oodb.exec({
          sql: "SELECT last_insert_rowid() AS id",
          rowMode: "array",
          returnValue: "resultRows",
        })[0][0],
        changes: oodb.changes(),
      };
    },
    all(sql, params = []) {
      return oodb.exec({
        sql,
        bind: params,
        rowMode: "object",
        returnValue: "resultRows",
      });
    },
    _raw: oodb,
  };
}

// Initialise sqlite-wasm, install the SAHPool VFS, open the persistent DB and
// run the store schema. Idempotent: repeated calls share one DB instance.
export function openLiveDb() {
  if (_adapterPromise) return _adapterPromise;
  _adapterPromise = (async () => {
    const sqlite3 = await sqlite3InitModule({
      locateFile: (path) => WASM_BASE + path,
    });
    const poolUtil = await sqlite3.installOpfsSAHPoolVfs({
      name: VFS_NAME,
      initialCapacity: 6,
    });
    const oodb = new poolUtil.OpfsSAHPoolDb(DB_NAME);
    const db = wrap(oodb);
    store.initSchema(db);
    db._poolUtil = poolUtil;
    return db;
  })();
  return _adapterPromise;
}

// Convenience re-exports so callers can `import { openLiveDb, saveSelectedJourney }`.
export {
  saveSelectedJourney,
  getSelectedJourney,
  clearSelectedJourney,
  appendChangeEvent,
  listChangeEvents,
  schemaVersion,
} from "./live-store.mjs";
