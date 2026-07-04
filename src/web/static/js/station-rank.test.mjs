import { test } from "node:test";
import assert from "node:assert/strict";
import {
  MATCH_TIER,
  normalizeStationText,
  scoreStationName,
  rankStations,
} from "./station-rank.mjs";

test("normalizeStationText lowercases, strips accents and punctuation", () => {
  assert.equal(normalizeStationText("Libràmont-Gare!"), "libramont gare");
  assert.equal(normalizeStationText("  Bruxelles   Midi "), "bruxelles midi");
  assert.equal(normalizeStationText(null), "");
});

test("scoreStationName tiers: exact > prefix > word-prefix > substring", () => {
  assert.equal(scoreStationName("Libramont", "libramont"), MATCH_TIER.EXACT);
  assert.equal(scoreStationName("Libramont Gare", "libramont"), MATCH_TIER.PREFIX);
  assert.equal(scoreStationName("Rue de Libramont", "libramont"), MATCH_TIER.WORD_PREFIX);
  assert.equal(scoreStationName("MALIBRAN", "libra"), MATCH_TIER.SUBSTRING);
  assert.equal(scoreStationName("Namur", "libramont"), MATCH_TIER.NONE);

  // Strict ordering of the numeric tiers.
  assert.ok(MATCH_TIER.EXACT > MATCH_TIER.PREFIX);
  assert.ok(MATCH_TIER.PREFIX > MATCH_TIER.WORD_PREFIX);
  assert.ok(MATCH_TIER.WORD_PREFIX > MATCH_TIER.SUBSTRING);
  assert.ok(MATCH_TIER.SUBSTRING > MATCH_TIER.NONE);
});

test("accents the user omits still match", () => {
  assert.equal(scoreStationName("Liège-Guillemins", "liege"), MATCH_TIER.PREFIX);
});

test("rankStations ranks exact SNCB Libramont first among Libramont candidates", () => {
  const stations = [
    { id: "b1", name: "Libramont Gare (TEC)", lat: 49.92, lon: 5.38 },
    { id: "b2", name: "Rue de Libramont", lat: 50.0, lon: 5.0 },
    { id: "s1", name: "Libramont", lat: 49.921, lon: 5.381 }, // SNCB station
    { id: "b3", name: "MALIBRAN", lat: 50.83, lon: 4.37 },    // no "libramont" substring
  ];
  const ranked = rankStations(stations, "Libramont");
  assert.equal(ranked[0].id, "s1");                 // exact wins
  assert.equal(ranked[0].name, "Libramont");
  // MALIBRAN does not contain "libramont" at all → excluded entirely.
  assert.deepEqual(ranked.map(s => s.id), ["s1", "b1", "b2"]);
});

test("rankStations: word-prefix beats mid-word substring for a shared query", () => {
  // "libr": "Libramont" is a word-prefix; "MALIBRAN" is only a substring.
  const stations = [
    { id: "a", name: "MALIBRAN" },
    { id: "b", name: "Libramont" },
  ];
  const ranked = rankStations(stations, "libr");
  assert.deepEqual(ranked.map(s => s.id), ["b", "a"]);
});

test("rankStations tie-breaks shorter name, then id (deterministic)", () => {
  // Same tier (all exact-prefix), same distance context (no focus): shorter
  // normalized name first, then id.
  const stations = [
    { id: "z", name: "Gare Centrale" },
    { id: "a", name: "Gare Centrale" }, // identical name → id tie-break
    { id: "m", name: "Gare" },          // shorter → first
  ];
  const ranked = rankStations(stations, "gare");
  assert.deepEqual(ranked.map(s => s.id), ["m", "a", "z"]);
});

test("rankStations uses geo distance to focus as a tie-break", () => {
  // Two identical names, same tier & length; nearer to focus wins.
  const focus = { flat: 50.0, flng: 5.0 };
  const stations = [
    { id: "far", name: "Gare", lat: 51.0, lon: 6.0 },
    { id: "near", name: "Gare", lat: 50.01, lon: 5.01 },
  ];
  const ranked = rankStations(stations, "gare", focus);
  assert.deepEqual(ranked.map(s => s.id), ["near", "far"]);
});

test("rankStations returns [] for empty/whitespace query", () => {
  assert.deepEqual(rankStations([{ id: "a", name: "Gare" }], "   "), []);
});
