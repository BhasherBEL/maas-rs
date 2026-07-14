// SAHPool VFS (not the worker OPFS VFS): needs no COOP/COEP headers, which would
// break map-tile loading.

import sqlite3InitModule from "./vendor/sqlite-wasm/sqlite3.mjs";
import * as store from "./live-store.mjs";

const WASM_BASE = "/static/js/vendor/sqlite-wasm/";
const DB_NAME = "/maas-live.sqlite3";
const VFS_NAME = "maas-live";

let _adapterPromise = null;

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

export {
  saveSelectedJourney,
  getSelectedJourney,
  clearSelectedJourney,
  appendChangeEvent,
  listChangeEvents,
  schemaVersion,
} from "./live-store.mjs";
