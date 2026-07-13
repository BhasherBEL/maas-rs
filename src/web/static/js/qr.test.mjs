// Round-trip tests for the self-contained QR encoder (vendor/qr.mjs).
// Encodes byte-mode data, then reverses masking + interleaving and RS-checks
// every block, proving the produced matrix is a valid, decodable QR symbol.
//
// The inline copy in index.html (window.QRCode) is a mechanical transcription of
// this module; keeping this module correct guards both.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { qrMatrix, qrSvg } from './vendor/qr.mjs';

// ── GF(256) log/antilog (primitive 0x11D), independent of the encoder ─────────
const EXP = new Array(512).fill(0);
const LOG = new Array(256).fill(0);
{
  let x = 1;
  for (let i = 0; i < 255; i++) { EXP[i] = x; LOG[x] = i; x <<= 1; if (x & 0x100) x ^= 0x11D; }
  for (let i = 255; i < 512; i++) EXP[i] = EXP[i - 255];
}

const ECC_M = [-1,10,16,26,18,24,16,18,22,22,26,30,22,22,24,24,28,28,26,26,26];
const BLK_M = [-1,1,1,1,2,2,4,4,4,5,5,5,8,9,9,10,10,11,13,14,16];
function gfMul(a, b) { return (a === 0 || b === 0) ? 0 : EXP[LOG[a] + LOG[b]]; }
function rawModules(ver) {
  let r = (16 * ver + 128) * ver + 64;
  if (ver >= 2) { const na = Math.floor(ver / 7) + 2; r -= (25 * na - 10) * na - 55; if (ver >= 7) r -= 36; }
  return r;
}
function rsDivisor(degree) {
  const result = new Array(degree).fill(0); result[degree - 1] = 1; let root = 1;
  for (let i = 0; i < degree; i++) {
    for (let j = 0; j < result.length; j++) { result[j] = gfMul(result[j], root); if (j + 1 < result.length) result[j] ^= result[j + 1]; }
    root = gfMul(root, 2);
  }
  return result;
}
function rsRemainder(data, div) {
  const result = div.map(() => 0);
  for (const b of data) { const f = b ^ result.shift(); result.push(0); for (let i = 0; i < result.length; i++) result[i] ^= gfMul(div[i], f); }
  return result;
}

// ── Decode: version from size, read format mask, unmask, de-interleave, RS-check
function decode(text) {
  const modules = qrMatrix(text);
  const size = modules.length;
  const version = (size - 17) / 4;

  // rebuild isFunction map by replaying the fixed-pattern reservation
  const isFunc = Array.from({ length: size }, () => new Array(size).fill(false));
  const setF = (x, y) => { if (x >= 0 && x < size && y >= 0 && y < size) isFunc[y][x] = true; };
  for (let i = 0; i < size; i++) { setF(6, i); setF(i, 6); }
  const finder = (x, y) => { for (let dy = -4; dy <= 4; dy++) for (let dx = -4; dx <= 4; dx++) setF(x + dx, y + dy); };
  finder(3, 3); finder(size - 4, 3); finder(3, size - 4);
  const align = (function (ver) {
    if (ver === 1) return [];
    const na = Math.floor(ver / 7) + 2, step = Math.ceil((ver * 4 + 4) / (na * 2 - 2)) * 2, res = [6];
    for (let pos = ver * 4 + 10; res.length < na; pos -= step) res.splice(1, 0, pos);
    return res;
  })(version);
  const na = align.length;
  for (let a = 0; a < na; a++) for (let b = 0; b < na; b++) {
    if ((a === 0 && b === 0) || (a === 0 && b === na - 1) || (a === na - 1 && b === 0)) continue;
    for (let dy = -2; dy <= 2; dy++) for (let dx = -2; dx <= 2; dx++) setF(align[a] + dx, align[b] + dy);
  }
  for (let f = 0; f <= 5; f++) setF(8, f);
  setF(8, 7); setF(8, 8); setF(7, 8);
  for (let f = 9; f < 15; f++) setF(14 - f, 8);
  for (let f = 0; f < 8; f++) setF(size - 1 - f, 8);
  for (let f = 8; f < 15; f++) setF(8, size - 15 + f);
  setF(8, size - 8);
  if (version >= 7) for (let v = 0; v < 18; v++) { setF(size - 11 + (v % 3), Math.floor(v / 3)); setF(Math.floor(v / 3), size - 11 + (v % 3)); }

  // read 15 format bits (first copy), unmask, extract mask number
  const fpos = [];
  for (let i = 0; i <= 5; i++) fpos.push([8, i]);
  fpos.push([8, 7], [8, 8], [7, 8]);
  for (let i = 9; i < 15; i++) fpos.push([14 - i, 8]);
  let fbits = 0;
  fpos.forEach(([x, y], idx) => { if (modules[y][x]) fbits |= (1 << idx); });
  fbits ^= 0x5412;
  const mask = (fbits >>> 10) & 0x7;

  // unmask data modules
  const m = modules.map((row) => row.slice());
  for (let y = 0; y < size; y++) for (let x = 0; x < size; x++) {
    if (isFunc[y][x]) continue;
    let inv;
    switch (mask) {
      case 0: inv = (x + y) % 2 === 0; break;
      case 1: inv = y % 2 === 0; break;
      case 2: inv = x % 3 === 0; break;
      case 3: inv = (x + y) % 3 === 0; break;
      case 4: inv = (Math.floor(x / 3) + Math.floor(y / 2)) % 2 === 0; break;
      case 5: inv = ((x * y) % 2) + ((x * y) % 3) === 0; break;
      case 6: inv = (((x * y) % 2) + ((x * y) % 3)) % 2 === 0; break;
      case 7: inv = (((x + y) % 2) + ((x * y) % 3)) % 2 === 0; break;
    }
    if (inv) m[y][x] = !m[y][x];
  }

  // read codewords in zig-zag order
  const bits = [];
  for (let right = size - 1; right >= 1; right -= 2) {
    if (right === 6) right = 5;
    for (let vert = 0; vert < size; vert++) for (let j = 0; j < 2; j++) {
      const cx = right - j, upward = ((right + 1) & 2) === 0, y = upward ? size - 1 - vert : vert;
      if (!isFunc[y][cx]) bits.push(modules[y] && m[y][cx] ? 1 : 0);
    }
  }
  const cws = [];
  for (let i = 0; i + 8 <= bits.length; i += 8) { let b = 0; for (let j = 0; j < 8; j++) b = (b << 1) | bits[i + j]; cws.push(b); }

  // de-interleave into blocks and verify RS remainder is zero
  const nb = BLK_M[version], bel = ECC_M[version], rawCw = Math.floor(rawModules(version) / 8);
  const ns = nb - (rawCw % nb), sl = Math.floor(rawCw / nb);
  const dataLens = Array.from({ length: nb }, (_, i) => sl - bel + (i < ns ? 0 : 1));
  const totalData = dataLens.reduce((a, b) => a + b, 0);
  const dataPart = cws.slice(0, totalData), eccPart = cws.slice(totalData, totalData + bel * nb);
  const maxDat = Math.max(...dataLens);
  const blocksData = Array.from({ length: nb }, () => []);
  let idx = 0;
  for (let i = 0; i < maxDat; i++) for (let b = 0; b < nb; b++) if (i < dataLens[b]) blocksData[b].push(dataPart[idx++]);
  const blocksEcc = Array.from({ length: nb }, () => []);
  idx = 0;
  for (let i = 0; i < bel; i++) for (let b = 0; b < nb; b++) blocksEcc[b].push(eccPart[idx++]);
  const rsDiv = rsDivisor(bel);
  for (let b = 0; b < nb; b++) {
    const rem = rsRemainder(blocksData[b].concat(blocksEcc[b]), rsDiv);
    assert.ok(rem.every((r) => r === 0), `block ${b} RS remainder nonzero`);
  }

  // reassemble payload
  const data = [];
  for (let b = 0; b < nb; b++) data.push(...blocksData[b]);
  const dbits = [];
  for (const cw of data) for (let i = 7; i >= 0; i--) dbits.push((cw >>> i) & 1);
  let pos = 0;
  const take = (n) => { let v = 0; for (let i = 0; i < n; i++) v = (v << 1) | dbits[pos++]; return v; };
  assert.equal(take(4), 0x4, 'expected byte mode');
  const cc = take(version <= 9 ? 8 : 16);
  const out = [];
  for (let i = 0; i < cc; i++) out.push(take(8));
  return new TextDecoder().decode(new Uint8Array(out));
}

const cases = [
  'A',
  'hello world',
  'Ünïcödé € test',
  'http://127.0.0.1:8000/?fromLat=50.85&fromLng=4.35&toLat=51.21&toLng=4.42&fromName=Brussels&toName=Antwerp&date=2026-07-11&time=09:30&window=60&modes=WALK_TRANSIT,BIKE_TO_TRANSIT',
  'x'.repeat(200),
  'https://example.com/' + 'a'.repeat(300),
];

for (const s of cases) {
  test(`round-trips (${s.length} chars)`, () => {
    assert.equal(decode(s), s);
  });
}

test('qrSvg returns self-contained inline SVG with no external refs', () => {
  const svg = qrSvg('https://example.com/x?y=1', { border: 2 });
  assert.match(svg, /^<svg /);
  assert.match(svg, /<\/svg>$/);
  assert.doesNotMatch(svg, /https?:\/\/(?!www\.w3\.org)/); // no external asset URLs
  assert.match(svg, /<rect /);
  assert.match(svg, /<path /);
});
