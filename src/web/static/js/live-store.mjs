// DB-agnostic persistence/history store for the Live (realtime) journey view.
//
// Adapter contract (both node:sqlite and sqlite-wasm SAHPool satisfy it):
//   db.exec(sql)            -> void              raw, possibly multi-statement
//   db.run(sql, params=[])  -> {lastInsertRowid, changes}
//   db.all(sql, params=[])  -> row objects[]     positional `?` params
// Single-row / scalar reads use all(sql, params)[0]. Synchronous only.

export const schemaVersion = 1;

export function initSchema(db) {
  const current = userVersion(db);
  if (current > schemaVersion) {
    throw new Error(
      `live-store: db user_version ${current} is newer than supported ${schemaVersion}`,
    );
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS selected_journey (
      id                TEXT PRIMARY KEY,
      created_at        TEXT NOT NULL,
      payload           TEXT NOT NULL,
      origin_label      TEXT,
      destination_label TEXT,
      status            TEXT
    );
    CREATE TABLE IF NOT EXISTS change_events (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      journey_id TEXT,
      ts         TEXT NOT NULL,
      kind       TEXT NOT NULL,
      summary    TEXT,
      payload    TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_change_events_ts ON change_events (ts DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_change_events_journey ON change_events (journey_id);
  `);
  db.exec(`PRAGMA user_version = ${schemaVersion}`);
  return schemaVersion;
}

export function saveSelectedJourney(db, journey, { logSelect = true } = {}) {
  if (!journey || journey.id == null) {
    throw new Error("saveSelectedJourney: journey.id is required");
  }
  const createdAt = journey.createdAt ?? nowIso();
  const payload = JSON.stringify(journey.payload ?? null);
  // Single active journey: replacing overwrites whatever was there. The
  // DELETE + INSERT + change-event must commit together or not at all.
  db.exec("BEGIN");
  try {
    db.exec("DELETE FROM selected_journey");
    db.run(
      `INSERT INTO selected_journey
         (id, created_at, payload, origin_label, destination_label, status)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        String(journey.id),
        createdAt,
        payload,
        journey.originLabel ?? null,
        journey.destinationLabel ?? null,
        journey.status ?? null,
      ],
    );
    if (logSelect) {
      appendChangeEvent(db, {
        journeyId: String(journey.id),
        ts: createdAt,
        kind: "select",
        summary:
          journey.summary ??
          `Started ${journey.originLabel ?? "?"} → ${journey.destinationLabel ?? "?"}`,
        payload: journey.payload ?? null,
      });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return String(journey.id);
}

export function getSelectedJourney(db) {
  const row = db.all(
    `SELECT id, created_at, payload, origin_label, destination_label, status
       FROM selected_journey LIMIT 1`,
  )[0];
  if (!row) return null;
  return {
    id: row.id,
    createdAt: row.created_at,
    payload: JSON.parse(row.payload),
    originLabel: row.origin_label,
    destinationLabel: row.destination_label,
    status: row.status,
  };
}

export function clearSelectedJourney(db) {
  const res = db.run("DELETE FROM selected_journey");
  return res ? res.changes : undefined;
}

export function appendChangeEvent(db, evt) {
  if (!evt || !evt.kind) {
    throw new Error("appendChangeEvent: evt.kind is required");
  }
  const res = db.run(
    `INSERT INTO change_events (journey_id, ts, kind, summary, payload)
     VALUES (?, ?, ?, ?, ?)`,
    [
      evt.journeyId ?? null,
      evt.ts ?? nowIso(),
      String(evt.kind),
      evt.summary ?? null,
      evt.payload === undefined ? null : JSON.stringify(evt.payload),
    ],
  );
  return res ? Number(res.lastInsertRowid) : undefined;
}

export function listChangeEvents(db, { limit, journeyId } = {}) {
  const where = [];
  const params = [];
  if (journeyId != null) {
    where.push("journey_id = ?");
    params.push(String(journeyId));
  }
  let sql =
    `SELECT id, journey_id, ts, kind, summary, payload FROM change_events`;
  if (where.length) sql += ` WHERE ${where.join(" AND ")}`;
  sql += ` ORDER BY ts DESC, id DESC`;
  if (limit != null) {
    sql += ` LIMIT ?`;
    params.push(Number(limit));
  }
  return db.all(sql, params).map((row) => ({
    id: row.id,
    journeyId: row.journey_id,
    ts: row.ts,
    kind: row.kind,
    summary: row.summary,
    payload: row.payload == null ? null : JSON.parse(row.payload),
  }));
}

function userVersion(db) {
  const row = db.all("PRAGMA user_version")[0];
  if (!row) return 0;
  return Number(row.user_version ?? Object.values(row)[0] ?? 0);
}

function nowIso() {
  return new Date().toISOString();
}
