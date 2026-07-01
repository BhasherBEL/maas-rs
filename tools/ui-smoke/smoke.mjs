// Headless-browser smoke/screenshot harness for the maas-rs web UI.
//
// Drives a REAL Chromium over the DevTools Protocol (CDP) using node built-ins
// only (global `WebSocket`, `fetch`, `child_process`, `fs`). No npm, no
// playwright. It loads the actual served page, drives the planner into a live
// journey, screenshots each stage, and FAILS on any uncaught JS exception or
// console.error — the class of bug (missing export, CSS-var scope) that has
// shipped "all green" because nothing executed the page.
//
// Usage:
//   nix run nixpkgs#nodejs -- tools/ui-smoke/smoke.mjs [--base URL] [--out DIR]
//                                                      [--scenario live|planner]
//
// Env:
//   CHROMIUM_BIN   path to a chromium binary (else falls back to `nix run`).
//
// Exit code 0 = PASS, 1 = FAIL.

import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

// ── args ──────────────────────────────────────────────────────────
function arg(name, def) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : def;
}
const BASE = arg("base", "http://127.0.0.1:8000").replace(/\/$/, "");
const OUT = resolve(arg("out", "for_you/realtime_ideas/real/smoke"));
const SCENARIO = arg("scenario", "live"); // live | planner
const CDP_PORT = parseInt(arg("port", "9333"), 10);

// Low-precision Brussels → Antwerp coords (geo precision policy — never high
// precision). A 2-transit OD so the live spine has a real transfer, which the
// alternatives rail (Card 1 backups) needs to exercise.
const QS =
  "fromLat=50.846&fromLng=4.351&toLat=51.221&toLng=4.401" +
  "&modes=WALK_TRANSIT&date=2026-06-29&time=08:00";
const PAGE_URL = `${BASE}/?${QS}`;

mkdirSync(OUT, { recursive: true });

// ── tiny helpers ──────────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function log(...a) {
  console.log("[smoke]", ...a);
}

// Poll `fn` (may be async) until it returns truthy or timeout. Returns the
// truthy value, or null on timeout.
async function until(fn, { timeout = 30000, interval = 250, label = "" } = {}) {
  const t0 = Date.now();
  for (;;) {
    let v;
    try {
      v = await fn();
    } catch {
      v = null;
    }
    if (v) return v;
    if (Date.now() - t0 > timeout) {
      log(`timeout waiting for ${label} (${timeout}ms)`);
      return null;
    }
    await sleep(interval);
  }
}

// ── server reuse / detection ──────────────────────────────────────
async function serverAlive() {
  try {
    const r = await fetch(`${BASE}/graphql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: "{ ping }" }),
      signal: AbortSignal.timeout(4000),
    });
    const j = await r.json();
    return j?.data?.ping === "pong";
  } catch {
    return false;
  }
}

// ── chromium launch ───────────────────────────────────────────────
function launchChromium(userDataDir) {
  const chromeArgs = [
    "--headless=new",
    "--disable-gpu",
    "--no-sandbox",
    "--hide-scrollbars",
    "--window-size=1280,1600",
    `--remote-debugging-port=${CDP_PORT}`,
    `--user-data-dir=${userDataDir}`,
    "about:blank",
  ];
  const bin = process.env.CHROMIUM_BIN;
  let proc;
  if (bin) {
    proc = spawn(bin, chromeArgs, { stdio: "ignore" });
  } else {
    proc = spawn("nix", ["run", "nixpkgs#chromium", "--", ...chromeArgs], {
      stdio: "ignore",
      detached: true, // own process group so we can kill nix + child
    });
  }
  return proc;
}

async function cdpVersion() {
  try {
    const r = await fetch(`http://127.0.0.1:${CDP_PORT}/json/version`, {
      signal: AbortSignal.timeout(2000),
    });
    return await r.json();
  } catch {
    return null;
  }
}

// ── CDP flat-session client over a single WebSocket ───────────────
function makeCdp(ws) {
  let nextId = 1;
  const pending = new Map(); // id -> {resolve, reject}
  const listeners = []; // {method, sessionId, cb}

  ws.addEventListener("message", (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    if (msg.id != null && pending.has(msg.id)) {
      const { resolve: res, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(JSON.stringify(msg.error)));
      else res(msg.result);
      return;
    }
    if (msg.method) {
      for (const l of listeners) {
        if (l.method !== msg.method) continue;
        if (l.sessionId && l.sessionId !== msg.sessionId) continue;
        l.cb(msg.params || {}, msg.sessionId);
      }
    }
  });

  function send(method, params = {}, sessionId) {
    const id = nextId++;
    const payload = { id, method, params };
    if (sessionId) payload.sessionId = sessionId;
    return new Promise((res, reject) => {
      pending.set(id, { resolve: res, reject });
      ws.send(JSON.stringify(payload));
    });
  }
  function on(method, cb, sessionId) {
    listeners.push({ method, cb, sessionId });
  }
  return { send, on };
}

// Evaluate JS in the page; returns the value (returnByValue).
async function evaluate(cdp, sid, expression) {
  const r = await cdp.send(
    "Runtime.evaluate",
    { expression, returnByValue: true, awaitPromise: true },
    sid,
  );
  if (r.exceptionDetails) {
    throw new Error(
      "evaluate threw: " +
        (r.exceptionDetails.exception?.description ||
          r.exceptionDetails.text),
    );
  }
  return r.result?.value;
}

// Click the first `.it-stops` pill under `scope` and report whether its hidden
// intermediate-stop rows (`.it-midstop`) appeared and the pill gained `.open`.
async function pillExpand(cdp, sid, scope) {
  const q = JSON.stringify(scope);
  return await evaluate(
    cdp,
    sid,
    `(() => {
      // Re-query the scope after the click: the planner re-render REPLACES
      // .card-detail, so a held reference would point at a detached subtree.
      const at = () => document.querySelector(${q});
      const count = () => (at() ? at().querySelectorAll('.it-midstop').length : 0);
      const pill = at() && at().querySelector('.it-stops');
      if (!pill) return { havePill: false, before: 0, after: 0, open: false, expanded: false };
      const before = count();
      pill.click();
      const after = count();
      const open = !!(at() && at().querySelector('.it-stops.open'));
      return { havePill: true, before, after, open, expanded: after > before };
    })()`,
  );
}

async function screenshot(cdp, sid, name) {
  const r = await cdp.send(
    "Page.captureScreenshot",
    { format: "png", captureBeyondViewport: true },
    sid,
  );
  const file = resolve(OUT, name);
  writeFileSync(file, Buffer.from(r.data, "base64"));
  log("screenshot →", file);
  return file;
}

// ── main ──────────────────────────────────────────────────────────
async function main() {
  const errors = []; // fail-gating: uncaught exceptions + console.error
  const logEntries = []; // captured Log.entryAdded (report only)
  const shots = [];
  let pass = true;
  const fail = (msg) => {
    pass = false;
    log("FAIL:", msg);
  };

  // 1. Server.
  const reused = await serverAlive();
  if (!reused) {
    fail("server not reachable at " + BASE + " (not started by harness)");
    // We intentionally do not boot a 2.5GB server here when probing finds
    // none reachable — the task expects reuse of the running instance. Bail.
    report(errors, logEntries, shots, pass);
    return pass ? 0 : 1;
  }
  log("reusing running server at", BASE);

  // 2. Chromium.
  const userDataDir = `/tmp/maas-smoke-${process.pid}`;
  const chrome = launchChromium(userDataDir);
  let killed = false;
  const killChrome = () => {
    if (killed) return;
    killed = true;
    try {
      if (process.env.CHROMIUM_BIN) chrome.kill("SIGKILL");
      else process.kill(-chrome.pid, "SIGKILL"); // process group
    } catch {
      try {
        chrome.kill("SIGKILL");
      } catch {}
    }
  };

  try {
    const ver = await until(cdpVersion, {
      timeout: 30000,
      label: "CDP /json/version",
    });
    if (!ver?.webSocketDebuggerUrl) {
      fail("chromium CDP endpoint never came up");
      report(errors, logEntries, shots, false);
      return 1;
    }
    log("chromium", ver.Browser);

    const ws = new WebSocket(ver.webSocketDebuggerUrl);
    await new Promise((res, rej) => {
      ws.addEventListener("open", res, { once: true });
      ws.addEventListener("error", rej, { once: true });
    });
    const cdp = makeCdp(ws);

    // Run BOTH viewports; the overall run fails if EITHER does.
    const passes = [];
    passes.push(await runPass(cdp, { mobile: false }));
    passes.push(await runPass(cdp, { mobile: true }));
    const ok = passes.every((p) => p.pass);
    reportMulti(passes);
    return ok ? 0 : 1;
  } finally {
    killChrome();
  }
}

// Drive one full viewport pass (desktop or mobile) in its own CDP target so the
// console/error capture is isolated per pass. Returns { label, pass, errors, shots }.
async function runPass(cdp, { mobile }) {
  const label = mobile ? "mobile" : "desktop";
  const suffix = mobile ? "-mobile" : "";
  const errors = [];
  const logEntries = [];
  const shots = [];
  let pass = true;
  const fail = (msg) => {
    pass = false;
    log(`[${label}] FAIL:`, msg);
  };
  log(`──── pass: ${label} ────`);

  // Fresh target (and session) per pass → isolated console capture.
  const { targetId } = await cdp.send("Target.createTarget", {
    url: "about:blank",
  });
  const { sessionId: sid } = await cdp.send("Target.attachToTarget", {
    targetId,
    flatten: true,
  });

  await cdp.send("Page.enable", {}, sid);
  await cdp.send("Runtime.enable", {}, sid);
  await cdp.send("Log.enable", {}, sid);

  cdp.on(
    "Runtime.exceptionThrown",
    (p) => {
      const d = p.exceptionDetails || {};
      const loc = `${d.url || "?"}:${d.lineNumber ?? "?"}:${d.columnNumber ?? "?"}`;
      const text = d.exception?.description || d.text || "uncaught exception";
      errors.push({ kind: "exception", text, location: loc });
      log(`[${label}] EXCEPTION:`, text, "@", loc);
    },
    sid,
  );
  cdp.on(
    "Runtime.consoleAPICalled",
    (p) => {
      if (p.type !== "error") return;
      const frame = p.stackTrace?.callFrames?.[0];
      const loc = frame
        ? `${frame.url}:${frame.lineNumber}:${frame.columnNumber}`
        : "?";
      const text = (p.args || [])
        .map((a) => a.value ?? a.description ?? a.type)
        .join(" ");
      errors.push({ kind: "console.error", text, location: loc });
      log(`[${label}] CONSOLE.ERROR:`, text, "@", loc);
    },
    sid,
  );
  cdp.on(
    "Log.entryAdded",
    (p) => {
      const e = p.entry || {};
      logEntries.push({ level: e.level, source: e.source, text: e.text, url: e.url });
      if (e.level === "error" && e.source === "javascript") {
        errors.push({ kind: "log.javascript", text: e.text, location: e.url || "?" });
      }
    },
    sid,
  );

  // Emulate the phone BEFORE navigating so layout/media queries apply.
  if (mobile) {
    await cdp.send(
      "Emulation.setDeviceMetricsOverride",
      { width: 402, height: 858, deviceScaleFactor: 2, mobile: true },
      sid,
    );
    await cdp.send("Emulation.setTouchEmulationEnabled", { enabled: true }, sid);
  }

  // Navigate.
  log(`[${label}] navigate →`, PAGE_URL);
  await cdp.send("Page.navigate", { url: PAGE_URL }, sid);
  await until(() => evaluate(cdp, sid, "document.readyState === 'complete'"), {
    timeout: 20000,
    label: "document load",
  });

  // Wait for plan result cards.
  const haveCards = await until(
    () =>
      evaluate(
        cdp,
        sid,
        "!!document.querySelector('#plans-list .pg-opt, #plans-list .rc')",
      ),
    { timeout: 45000, label: "plan result cards" },
  );
  if (!haveCards) fail("plan result cards never rendered");
  shots.push(await screenshot(cdp, sid, `planner${suffix}.png`));

  // Open a plan's detail.
  await evaluate(
    cdp,
    sid,
    "(document.querySelector('#plans-list .pg-opt, #plans-list .rc'))?.click()",
  );
  const haveDetail = await until(
    () => evaluate(cdp, sid, "!!document.querySelector('.card-detail')"),
    { timeout: 15000, label: "plan-detail spine" },
  );
  if (!haveDetail) fail("plan-detail (.card-detail) never rendered after click");
  shots.push(await screenshot(cdp, sid, `plan-detail${suffix}.png`));

  // ── Regression guard: the expandable intermediate-stops pill (.it-stops)
  // must EXPAND its hidden stop rows on click, in the planner plan-detail. ──
  {
    const exp = await pillExpand(cdp, sid, ".card-detail");
    log(`[${label}] planner pill-expand:`, JSON.stringify(exp));
    if (!exp.havePill) fail("planner: no .it-stops pill in plan-detail (expected a 2-transit OD)");
    else if (!exp.expanded)
      fail(`planner: .it-stops pill did not expand (midstops ${exp.before}->${exp.after}, open=${exp.open})`);
  }

  if (SCENARIO === "planner") {
    await cdp.send("Target.closeTarget", { targetId });
    return { label, pass, errors, shots };
  }

  // Start the live journey.
  const enabled = await until(
    () =>
      evaluate(
        cdp,
        sid,
        "(() => { const b = document.querySelector('.lv-start'); return !!b && !b.disabled; })()",
      ),
    { timeout: 15000, label: ".lv-start enabled (MaaSLive.ready)" },
  );
  if (!enabled) {
    const ready = await evaluate(cdp, sid, "!!(window.MaaSLive && window.MaaSLive.ready)");
    fail(`.lv-start stayed disabled — MaaSLive.ready=${ready}`);
  } else {
    await evaluate(cdp, sid, "document.querySelector('.lv-start').click()");
  }

  const liveVisible = await until(
    () =>
      evaluate(
        cdp,
        sid,
        "(() => { const v = document.getElementById('live-view'); return !!v && !v.hidden; })()",
      ),
    { timeout: 15000, label: "#live-view visible" },
  );
  if (!liveVisible) fail("#live-view never became visible");
  await sleep(700); // let the spine/map paint + invalidateSize settle
  shots.push(await screenshot(cdp, sid, `live-view${suffix}.png`));

  // ── Regression guard: the .it-stops pill must EXPAND in the LIVE spine too.
  // It re-renders #lv-spine (not the hidden planner), so the toggled stops show. ──
  if (liveVisible) {
    const havePill = await until(
      () => evaluate(cdp, sid, "!!document.querySelector('#lv-spine .it-stops')"),
      { timeout: 15000, label: "#lv-spine .it-stops pill" },
    );
    if (!havePill) {
      fail("live: no .it-stops pill in #lv-spine (expected a 2-transit OD)");
    } else {
      const exp = await pillExpand(cdp, sid, "#lv-spine");
      log(`[${label}] live pill-expand:`, JSON.stringify(exp));
      if (!exp.expanded)
        fail(`live: .it-stops pill did not expand (midstops ${exp.before}->${exp.after}, open=${exp.open})`);
      shots.push(await screenshot(cdp, sid, `live-stops-expanded${suffix}.png`));
    }
  }

  await evaluate(
    cdp,
    sid,
    "(() => { const s = document.getElementById('lv-spine'); if (s) s.scrollTop = s.scrollHeight; })()",
  );
  await sleep(400);
  shots.push(await screenshot(cdp, sid, `live-view-2${suffix}.png`));

  // ── Alternatives rail (Card 1 — same-station backups) ──────────────
  // Click a "Find alternatives"/"Backups" affordance on a transit boarding and
  // assert Card 1 renders with at least one backup row OR the empty state. Closes
  // the sheet afterwards so the open overlay can't intercept the drag test below.
  if (liveVisible) {
    const haveAltBtn = await until(
      () => evaluate(cdp, sid, "!!document.querySelector('#lv-spine .lv-alt-btn')"),
      { timeout: 15000, label: ".lv-alt-btn present" },
    );
    if (!haveAltBtn) {
      fail("no .lv-alt-btn rendered on any transit boarding (expected a 2-transit OD)");
    } else {
      await evaluate(cdp, sid, "document.querySelector('#lv-spine .lv-alt-btn').click()");
      const railUp = await until(
        () =>
          evaluate(
            cdp,
            sid,
            "(() => { const s = document.getElementById('lv-sheet'); const c = document.querySelector('#lv-sheet-card .lv-railcard.keep'); const rows = document.querySelectorAll('#lv-sheet-card .lv-bk-row').length; const band = !!document.querySelector('#lv-sheet-card .lv-band'); return (s && !s.hidden && c && (rows > 0 || band)); })()",
          ),
        { timeout: 20000, label: "alternatives rail Card 1" },
      );
      if (!railUp) fail("alternatives rail (Card 1) never rendered after clicking .lv-alt-btn");
      const railDiag = await evaluate(
        cdp,
        sid,
        "(() => ({ rows: document.querySelectorAll('#lv-sheet-card .lv-bk-row').length, band: (document.querySelector('#lv-sheet-card .lv-band')||{}).textContent || '', confirm: !!document.querySelector('#lv-sheet-card .lv-bk-confirm') }))()",
      );
      log(`[${label}] alternatives:`, JSON.stringify(railDiag));
      shots.push(await screenshot(cdp, sid, `alternatives${suffix}.png`));

      // Exercise the BACKUP-VS-SWITCH flow: select a row (must NOT replace the
      // main plan, only highlight + enable confirm), then confirm (records a
      // change_event, persists, drops a backup chip on the spine).
      if (railUp && railDiag.rows > 0) {
        const afterSelect = await evaluate(
          cdp,
          sid,
          "(() => { const r = document.querySelector('#lv-sheet-card .lv-bk-row'); if (!r) return null; r.click(); const c = document.querySelector('#lv-sheet-card .lv-bk-confirm'); const sel = document.querySelector('#lv-sheet-card .lv-bk-row.sel'); return { selected: !!sel, confirmEnabled: !!c && !c.disabled, confirmLabel: c ? c.textContent : '' }; })()",
        );
        log(`[${label}] select:`, JSON.stringify(afterSelect));
        if (!afterSelect || !afterSelect.selected) fail("selecting a backup row did not highlight it (.sel)");
        if (!afterSelect || !afterSelect.confirmEnabled) fail("confirm stayed disabled after selecting a backup");
        if (afterSelect && !/^Keep /.test(afterSelect.confirmLabel)) fail(`confirm label not 'Keep …' (got '${afterSelect?.confirmLabel}')`);
        shots.push(await screenshot(cdp, sid, `alternatives-selected${suffix}.png`));

        const afterConfirm = await evaluate(
          cdp,
          sid,
          "(() => { const c = document.querySelector('#lv-sheet-card .lv-bk-confirm'); if (c) c.click(); const chip = document.querySelector('#lv-spine .lv-bk-chip'); let events = []; try { events = window.MaaSLive.listEvents({ limit: 5 }) || []; } catch (e) {} const sheetHidden = document.getElementById('lv-sheet').hidden; return { chip: !!chip, chipText: chip ? chip.textContent : '', backupEvent: events.some(e => e.kind === 'backup_set'), sheetHidden }; })()",
        );
        log(`[${label}] confirm:`, JSON.stringify(afterConfirm));
        if (!afterConfirm.chip) fail("no .lv-bk-chip on the spine after confirming a backup");
        if (!afterConfirm.backupEvent) fail("no 'backup_set' change_event recorded after confirm");
        if (!afterConfirm.sheetHidden) fail("sheet did not close after confirming a backup");
        shots.push(await screenshot(cdp, sid, `alternatives-confirmed${suffix}.png`));
      }

      // ── Alternatives rail (Card 2+ — SWITCH cards via onboard requery) ──
      // Reopen the rail (the backup confirm closed it), wait for the lazy onboard
      // requery to resolve the switch area, and assert it renders EITHER >=1 switch
      // card OR the quiet empty state. If a switch card is present, select it →
      // assert a dimmed preview + Confirm appear → confirm → assert the journey
      // changed (descriptor trip sequence differs + a 'switch' history event).
      await evaluate(cdp, sid, "document.querySelector('#lv-spine .lv-alt-btn')?.click()");
      // Resolve once the lazy onboard requery has settled: either real switch cards,
      // or the quiet empty state (NOT the transient loading card).
      const switchArea = await until(
        () =>
          evaluate(
            cdp,
            sid,
            "(() => { const cards = document.querySelectorAll('#lv-sheet-card .lv-railcard.switch:not(.quiet)').length; const empty = !!document.querySelector('#lv-sheet-card .lv-railcard.switch.quiet.empty'); return (cards > 0) ? { cards } : (empty ? { cards: 0 } : false); })()",
          ),
        { timeout: 30000, label: "switch area (cards or empty state)" },
      );
      if (!switchArea) {
        fail("switch area never resolved (no Card 2+ and no quiet empty state)");
      } else {
        log(`[${label}] switch area:`, JSON.stringify(switchArea));
        shots.push(await screenshot(cdp, sid, `switch-cards${suffix}.png`));

        if (switchArea.cards > 0) {
          // Snapshot the live spine BEFORE the switch (transit line-badge count +
          // app-bar arrival ETA) so we can prove the journey actually changed.
          const before = await evaluate(
            cdp,
            sid,
            "(() => ({ badges: document.querySelectorAll('#lv-spine .it-badge').length, eta: (document.getElementById('lv-eta')||{}).textContent || '' }))()",
          );
          const afterPick = await evaluate(
            cdp,
            sid,
            "(() => { const c = document.querySelector('#lv-sheet-card .lv-railcard.switch:not(.quiet)'); if (!c) return null; c.click(); const preview = !!document.querySelector('#lv-sheet-card .lv-preview .card-detail'); const confirm = document.querySelector('#lv-sheet-card .lv-bk-confirm'); return { preview, confirmLabel: confirm ? confirm.textContent : '' }; })()",
          );
          log(`[${label}] switch select:`, JSON.stringify(afterPick));
          if (!afterPick || !afterPick.preview) fail("selecting a switch card did not render a .lv-preview spine");
          if (!afterPick || !/Confirm/.test(afterPick.confirmLabel)) fail(`switch confirm label not 'Confirm …' (got '${afterPick?.confirmLabel}')`);
          shots.push(await screenshot(cdp, sid, `switch-preview${suffix}.png`));

          const afterSwitch = await evaluate(
            cdp,
            sid,
            "(() => { const c = document.querySelector('#lv-sheet-card .lv-bk-confirm'); if (c) c.click(); let events = []; try { events = window.MaaSLive.listEvents({ limit: 6 }) || []; } catch (e) {} const sheetHidden = document.getElementById('lv-sheet').hidden; return { sheetHidden, switchEvent: events.some(e => e.kind === 'switch'), badges: document.querySelectorAll('#lv-spine .it-badge').length, eta: (document.getElementById('lv-eta')||{}).textContent || '' }; })()",
          );
          log(`[${label}] switch confirm:`, JSON.stringify(afterSwitch), "before:", JSON.stringify(before));
          if (!afterSwitch.sheetHidden) fail("sheet did not close after confirming a switch");
          if (!afterSwitch.switchEvent) fail("no 'switch' change_event recorded after confirm");
          if (afterSwitch.badges === before.badges && afterSwitch.eta === before.eta)
            fail("live spine did not change after switch confirm (same transit badges AND same ETA)");
          shots.push(await screenshot(cdp, sid, `switch-confirmed${suffix}.png`));
        }
      }

      // Tidy up: close the sheet (× button) before the drag/diagnostics block.
      await evaluate(
        cdp,
        sid,
        "(() => { const x = document.querySelector('#lv-sheet-card .lv-sheet-h .lv-icon-btn'); if (x) x.click(); const s = document.getElementById('lv-sheet'); if (s && !s.hidden) s.hidden = true; })()",
      );
      await sleep(200);
    }
  }

  // FIX 3: exercise the resize handle — drag down → must snap to the top anchor
  // (0.9), grow the map, persist the anchor, and throw no console errors.
  if (mobile && liveVisible) {
    const before = await evaluate(
      cdp,
      sid,
      "document.getElementById('lv-map-host').offsetHeight",
    );
    await evaluate(
      cdp,
      sid,
      `(() => {
        const g = document.getElementById('lv-grab');
        const r = g.getBoundingClientRect();
        const y0 = r.top + r.height / 2;
        const o = (y) => ({ pointerId: 1, clientX: r.left + 20, clientY: y, bubbles: true });
        g.dispatchEvent(new PointerEvent('pointerdown', o(y0)));
        g.dispatchEvent(new PointerEvent('pointermove', o(y0 + 400)));
        g.dispatchEvent(new PointerEvent('pointerup',   o(y0 + 400)));
      })()`,
    );
    await sleep(450); // let the snap transition + invalidateSize settle
    const after = await evaluate(
      cdp,
      sid,
      `(() => ({
        h: document.getElementById('lv-map-host').offsetHeight,
        anchor: localStorage.getItem('maas.live.mapAnchor'),
      }))()`,
    );
    log(`[${label}] drag: before=${before} after=${JSON.stringify(after)}`);
    if (!(after.h > before)) fail(`map drag did not grow map (before=${before}, after=${after.h})`);
    if (after.anchor !== "0.9") fail(`map drag did not snap to top anchor (anchor=${after.anchor})`);
    shots.push(await screenshot(cdp, sid, `live-view-snap${suffix}.png`));
  }

  // Diagnostics + assertions.
  const diag = await evaluate(
    cdp,
    sid,
    `(() => {
      const lv = document.querySelector('.lv');
      const bg = lv ? getComputedStyle(lv).backgroundColor : null;
      const v = document.getElementById('live-view');
      const m = document.querySelector('.lv-map > #map') || document.getElementById('map');
      const cs = m ? getComputedStyle(m) : null;
      return {
        lvBackground: bg,
        liveVisible: !!v && !v.hidden,
        maasLiveReady: !!(window.MaaSLive && window.MaaSLive.ready),
        maasLivePersistent: !!(window.MaaSLive && window.MaaSLive.persistent),
        mapFound: !!m,
        mapDisplay: cs ? cs.display : null,
        mapHeight: m ? m.offsetHeight : 0,
      };
    })()`,
  );
  log(`[${label}] diagnostics:`, JSON.stringify(diag));

  const bg = (diag.lvBackground || "").trim();
  const transparent =
    bg === "" ||
    bg === "transparent" ||
    bg.replace(/\s/g, "") === "rgba(0,0,0,0)";
  if (transparent) {
    fail(`.lv background is transparent ("${bg}") — CSS var scope regression`);
  }
  if (!diag.liveVisible) fail("diagnostics: live view not visible");

  // The map element must be visible AND have real height in the live view.
  if (!diag.mapFound) fail("live map element (#map) not found");
  else if (diag.mapDisplay === "none")
    fail(`live map is display:none (${diag.mapDisplay})`);
  else if (!(diag.mapHeight > 0))
    fail(`live map has zero height (offsetHeight=${diag.mapHeight})`);

  if (errors.length) {
    fail(`${errors.length} console error(s)/exception(s) captured`);
  }

  await cdp.send("Target.closeTarget", { targetId });
  return { label, pass, errors, shots };
}

function report(errors, logEntries, shots, pass) {
  console.log("\n================ UI SMOKE REPORT ================");
  console.log("RESULT:", pass ? "PASS ✅" : "FAIL ❌");
  console.log(`\nConsole errors / uncaught exceptions: ${errors.length}`);
  if (errors.length === 0) {
    console.log("  (none)");
  } else {
    errors.forEach((e, i) =>
      console.log(`  ${i + 1}. [${e.kind}] ${e.text}\n     @ ${e.location}`),
    );
  }
  const logErrs = logEntries.filter((e) => e.level === "error");
  const logWarns = logEntries.filter((e) => e.level === "warning");
  console.log(
    `\nLog.entryAdded: ${logEntries.length} (errors=${logErrs.length}, warnings=${logWarns.length}) [non-JS errors do NOT fail the run]`,
  );
  logEntries
    .filter((e) => e.level === "error" || e.level === "warning")
    .forEach((e) =>
      console.log(`  - [${e.level}/${e.source}] ${e.text}${e.url ? " @ " + e.url : ""}`),
    );
  console.log("\nScreenshots:");
  shots.forEach((s) => console.log("  " + s));
  console.log("================================================\n");
}

// Aggregate report across viewport passes; overall PASS requires every pass.
function reportMulti(passes) {
  const ok = passes.every((p) => p.pass);
  console.log("\n================ UI SMOKE REPORT ================");
  console.log("RESULT:", ok ? "PASS ✅" : "FAIL ❌");
  for (const p of passes) {
    console.log(`\n── pass: ${p.label} — ${p.pass ? "PASS ✅" : "FAIL ❌"} ──`);
    console.log(`Console errors / uncaught exceptions: ${p.errors.length}`);
    if (p.errors.length === 0) console.log("  (none)");
    else
      p.errors.forEach((e, i) =>
        console.log(`  ${i + 1}. [${e.kind}] ${e.text}\n     @ ${e.location}`),
      );
    console.log("Screenshots:");
    p.shots.forEach((s) => console.log("  " + s));
  }
  console.log("================================================\n");
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error("[smoke] FATAL", err);
    process.exit(1);
  });
