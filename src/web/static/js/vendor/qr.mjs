// Compact self-contained QR Code encoder (byte mode, ECC level M, versions 1-20).
// No dependencies, no network. Produces a boolean module matrix and an SVG string.
//
// Adapted from the public-domain "QR Code generator" reference algorithm by
// Project Nayuki (https://www.nayuki.io/page/qr-code-generator-library),
// released under the MIT License. Trimmed to byte-mode-only encoding for URLs.
//
// MIT License. Copyright (c) Project Nayuki.
//
// Exposes: qrSvg(text, opts) -> SVG string, and qrMatrix(text) -> boolean[][].
// Also attaches window.QRCode = { svg: qrSvg, matrix: qrMatrix } when in a browser.

'use strict';

// ── Galois-field (GF(2^8)) arithmetic for Reed-Solomon ─────────────────────────
function gfMul(x, y) {
  let z = 0;
  for (let i = 7; i >= 0; i--) {
    z = (z << 1) ^ ((z >>> 7) * 0x11D);
    z ^= ((y >>> i) & 1) * x;
  }
  return z & 0xFF;
}

function rsDivisor(degree) {
  const result = [];
  for (let i = 0; i < degree; i++) result.push(0);
  result[degree - 1] = 1;
  let root = 1;
  for (let i = 0; i < degree; i++) {
    for (let j = 0; j < result.length; j++) {
      result[j] = gfMul(result[j], root);
      if (j + 1 < result.length) result[j] ^= result[j + 1];
    }
    root = gfMul(root, 2);
  }
  return result;
}

function rsRemainder(data, divisor) {
  const result = divisor.map(() => 0);
  for (const b of data) {
    const factor = b ^ result.shift();
    result.push(0);
    for (let i = 0; i < result.length; i++) result[i] ^= gfMul(divisor[i], factor);
  }
  return result;
}

// ── Error-correction tables (ECC level M) ─────────────────────────────────────
// Indexed by version (1..40). We only build up to version 20 here, which holds
// well over 600 bytes at level M — ample for any URL.
// ECC codewords per block (M) and number of error-correction blocks (M).
const ECC_CODEWORDS_PER_BLOCK_M = [
  -1, 10, 16, 26, 18, 24, 16, 18, 22, 22, 26, 30, 22, 22, 24, 24, 28, 28, 26, 26, 26,
];
const NUM_ERROR_CORRECTION_BLOCKS_M = [
  -1, 1, 1, 1, 2, 2, 4, 4, 4, 5, 5, 5, 8, 9, 9, 10, 10, 11, 13, 14, 16,
];

function getNumRawDataModules(ver) {
  let result = (16 * ver + 128) * ver + 64;
  if (ver >= 2) {
    const numAlign = Math.floor(ver / 7) + 2;
    result -= (25 * numAlign - 10) * numAlign - 55;
    if (ver >= 7) result -= 36;
  }
  return result;
}

function getNumDataCodewords(ver) {
  return Math.floor(getNumRawDataModules(ver) / 8)
    - ECC_CODEWORDS_PER_BLOCK_M[ver] * NUM_ERROR_CORRECTION_BLOCKS_M[ver];
}

// ── Bit buffer ────────────────────────────────────────────────────────────────
function appendBits(val, len, bb) {
  for (let i = len - 1; i >= 0; i--) bb.push((val >>> i) & 1);
}

// ── Encode text as byte-mode segment and pick the smallest fitting version ─────
function encodeBytes(text) {
  // UTF-8 encode
  const bytes = [];
  for (const ch of unescape(encodeURIComponent(text))) bytes.push(ch.charCodeAt(0) & 0xFF);

  let version = 1;
  for (; ; version++) {
    if (version > 20) throw new Error('QR data too long');
    const dataCapacityBits = getNumDataCodewords(version) * 8;
    // Byte mode: 4-bit mode indicator + char-count indicator + 8 bits per byte.
    const ccBits = version <= 9 ? 8 : 16;
    const usedBits = 4 + ccBits + bytes.length * 8;
    if (usedBits <= dataCapacityBits) break;
  }

  const bb = [];
  appendBits(0x4, 4, bb);                                // byte mode
  appendBits(bytes.length, version <= 9 ? 8 : 16, bb);   // char count
  for (const b of bytes) appendBits(b, 8, bb);

  const dataCapacityBits = getNumDataCodewords(version) * 8;
  // Terminator + bit/byte padding
  appendBits(0, Math.min(4, dataCapacityBits - bb.length), bb);
  while (bb.length % 8 !== 0) bb.push(0);
  for (let pad = 0xEC; bb.length < dataCapacityBits; pad ^= 0xEC ^ 0x11)
    appendBits(pad, 8, bb);

  // Pack bits into codeword bytes
  const dataCodewords = [];
  for (let i = 0; i < bb.length; i += 8) {
    let byte = 0;
    for (let j = 0; j < 8; j++) byte = (byte << 1) | bb[i + j];
    dataCodewords.push(byte);
  }
  return { version, dataCodewords };
}

// ── Interleave data + ECC codewords ───────────────────────────────────────────
function addEcc(version, data) {
  const numBlocks = NUM_ERROR_CORRECTION_BLOCKS_M[version];
  const blockEccLen = ECC_CODEWORDS_PER_BLOCK_M[version];
  const rawCodewords = Math.floor(getNumRawDataModules(version) / 8);
  const numShortBlocks = numBlocks - (rawCodewords % numBlocks);
  const shortBlockLen = Math.floor(rawCodewords / numBlocks);

  const blocks = [];
  const rsDiv = rsDivisor(blockEccLen);
  let k = 0;
  for (let i = 0; i < numBlocks; i++) {
    const datLen = shortBlockLen - blockEccLen + (i < numShortBlocks ? 0 : 1);
    const dat = data.slice(k, k + datLen);
    k += datLen;
    const ecc = rsRemainder(dat.slice(), rsDiv);
    if (i < numShortBlocks) dat.push(0); // placeholder to align interleaving
    blocks.push({ dat, ecc, short: i < numShortBlocks });
  }

  const result = [];
  // interleave data codewords
  const maxDat = shortBlockLen - blockEccLen + 1;
  for (let i = 0; i < maxDat; i++) {
    for (const blk of blocks) {
      // skip the padding placeholder column for short blocks
      const realLen = blk.dat.length - (blk.short ? 1 : 0);
      if (i < realLen) result.push(blk.dat[i]);
    }
  }
  // interleave ecc codewords
  for (let i = 0; i < blockEccLen; i++)
    for (const blk of blocks) result.push(blk.ecc[i]);

  return result;
}

// ── Draw the module matrix ────────────────────────────────────────────────────
function buildMatrix(version, allCodewords) {
  const size = version * 4 + 17;
  const modules = [];
  const isFunction = [];
  for (let i = 0; i < size; i++) {
    modules.push(new Array(size).fill(false));
    isFunction.push(new Array(size).fill(false));
  }

  function setFunctionModule(x, y, isDark) {
    modules[y][x] = isDark;
    isFunction[y][x] = true;
  }

  // Finder patterns + separators
  function drawFinder(x, y) {
    for (let dy = -4; dy <= 4; dy++) {
      for (let dx = -4; dx <= 4; dx++) {
        const dist = Math.max(Math.abs(dx), Math.abs(dy));
        const xx = x + dx, yy = y + dy;
        if (xx >= 0 && xx < size && yy >= 0 && yy < size)
          setFunctionModule(xx, yy, dist !== 2 && dist !== 4);
      }
    }
  }

  // Timing patterns
  for (let i = 0; i < size; i++) {
    setFunctionModule(6, i, i % 2 === 0);
    setFunctionModule(i, 6, i % 2 === 0);
  }
  drawFinder(3, 3);
  drawFinder(size - 4, 3);
  drawFinder(3, size - 4);

  // Alignment patterns
  const alignPos = getAlignmentPatternPositions(version);
  const numAlign = alignPos.length;
  for (let i = 0; i < numAlign; i++) {
    for (let j = 0; j < numAlign; j++) {
      if ((i === 0 && j === 0) || (i === 0 && j === numAlign - 1) || (i === numAlign - 1 && j === 0))
        continue; // overlaps with finder patterns
      const cx = alignPos[i], cy = alignPos[j];
      for (let dy = -2; dy <= 2; dy++)
        for (let dx = -2; dx <= 2; dx++)
          setFunctionModule(cx + dx, cy + dy, Math.max(Math.abs(dx), Math.abs(dy)) !== 1);
    }
  }

  // Reserve format + version info areas (drawn later, mark as function now)
  drawFormatBits(0, size, setFunctionModule, modules, isFunction); // reserves only
  if (version >= 7) reserveVersionInfo(version, size, setFunctionModule);

  // Draw data + ecc codewords with zig-zag placement
  const bits = [];
  for (const cw of allCodewords) for (let i = 7; i >= 0; i--) bits.push((cw >>> i) & 1);

  let i = 0;
  for (let right = size - 1; right >= 1; right -= 2) {
    if (right === 6) right = 5;
    for (let vert = 0; vert < size; vert++) {
      for (let j = 0; j < 2; j++) {
        const x = right - j;
        const upward = ((right + 1) & 2) === 0;
        const y = upward ? size - 1 - vert : vert;
        if (!isFunction[y][x] && i < bits.length) {
          modules[y][x] = bits[i] !== 0;
          i++;
        }
      }
    }
  }

  // Try all 8 masks, pick the one with the lowest penalty
  let bestMask = 0, minPenalty = Infinity, bestModules = null;
  for (let mask = 0; mask < 8; mask++) {
    const trial = modules.map(row => row.slice());
    applyMask(trial, isFunction, mask, size);
    drawFormatBits(mask, size, null, trial, isFunction);
    const p = penaltyScore(trial, size);
    if (p < minPenalty) { minPenalty = p; bestMask = mask; bestModules = trial; }
  }
  void bestMask;
  return bestModules;
}

function getAlignmentPatternPositions(ver) {
  if (ver === 1) return [];
  const numAlign = Math.floor(ver / 7) + 2;
  const step = (ver === 32) ? 26 :
    Math.ceil((ver * 4 + 4) / (numAlign * 2 - 2)) * 2;
  const result = [6];
  for (let pos = ver * 4 + 10; result.length < numAlign; pos -= step)
    result.splice(1, 0, pos);
  return result;
}

// Format bits carry ECC level (M = 0b00) + mask. When setFn is provided we only
// reserve the region (mark as function). When trial+isFunction are provided we
// stamp the actual 15-bit format string.
function drawFormatBits(mask, size, setFn, modules, isFunction) {
  if (setFn) {
    // reserve the two 15-module format strips (around finder patterns)
    for (let i = 0; i <= 5; i++) setFn(8, i, false);
    setFn(8, 7, false); setFn(8, 8, false); setFn(7, 8, false);
    for (let i = 9; i < 15; i++) setFn(14 - i, 8, false);
    for (let i = 0; i < 8; i++) setFn(size - 1 - i, 8, false);
    for (let i = 8; i < 15; i++) setFn(8, size - 15 + i, false);
    setFn(8, size - 8, true); // dark module
    return;
  }
  // Compute format string: ECC level M -> 0b00 in bits, mask (3 bits)
  const data = (0b00 << 3) | mask; // 5 data bits
  let rem = data;
  for (let i = 0; i < 10; i++) rem = (rem << 1) ^ ((rem >>> 9) * 0x537);
  const bits = ((data << 10) | rem) ^ 0x5412; // 15 bits, XOR mask

  function place(x, y, v) { modules[y][x] = v; }
  // first copy (around top-left finder)
  for (let i = 0; i <= 5; i++) place(8, i, ((bits >>> i) & 1) !== 0);
  place(8, 7, ((bits >>> 6) & 1) !== 0);
  place(8, 8, ((bits >>> 7) & 1) !== 0);
  place(7, 8, ((bits >>> 8) & 1) !== 0);
  for (let i = 9; i < 15; i++) place(14 - i, 8, ((bits >>> i) & 1) !== 0);
  // second copy
  for (let i = 0; i < 8; i++) place(size - 1 - i, 8, ((bits >>> i) & 1) !== 0);
  for (let i = 8; i < 15; i++) place(8, size - 15 + i, ((bits >>> i) & 1) !== 0);
  void isFunction;
}

function reserveVersionInfo(version, size, setFn) {
  // Compute 18-bit version info and stamp both copies.
  let rem = version;
  for (let i = 0; i < 12; i++) rem = (rem << 1) ^ ((rem >>> 11) * 0x1F25);
  const bits = (version << 12) | rem; // 18 bits
  for (let i = 0; i < 18; i++) {
    const bit = ((bits >>> i) & 1) !== 0;
    const a = size - 11 + (i % 3);
    const b = Math.floor(i / 3);
    setFn(a, b, bit);
    setFn(b, a, bit);
  }
}

function applyMask(modules, isFunction, mask, size) {
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      if (isFunction[y][x]) continue;
      let invert;
      switch (mask) {
        case 0: invert = (x + y) % 2 === 0; break;
        case 1: invert = y % 2 === 0; break;
        case 2: invert = x % 3 === 0; break;
        case 3: invert = (x + y) % 3 === 0; break;
        case 4: invert = (Math.floor(x / 3) + Math.floor(y / 2)) % 2 === 0; break;
        case 5: invert = ((x * y) % 2) + ((x * y) % 3) === 0; break;
        case 6: invert = (((x * y) % 2) + ((x * y) % 3)) % 2 === 0; break;
        case 7: invert = (((x + y) % 2) + ((x * y) % 3)) % 2 === 0; break;
        default: invert = false;
      }
      if (invert) modules[y][x] = !modules[y][x];
    }
  }
}

function penaltyScore(modules, size) {
  let result = 0;
  // Adjacent modules in row/column with same color
  for (let y = 0; y < size; y++) {
    let runColor = false, runX = 0;
    for (let x = 0; x < size; x++) {
      if (modules[y][x] === runColor) { runX++; if (runX === 5) result += 3; else if (runX > 5) result++; }
      else { runColor = modules[y][x]; runX = 1; }
    }
  }
  for (let x = 0; x < size; x++) {
    let runColor = false, runY = 0;
    for (let y = 0; y < size; y++) {
      if (modules[y][x] === runColor) { runY++; if (runY === 5) result += 3; else if (runY > 5) result++; }
      else { runColor = modules[y][x]; runY = 1; }
    }
  }
  // 2x2 blocks of same color
  for (let y = 0; y < size - 1; y++) {
    for (let x = 0; x < size - 1; x++) {
      const c = modules[y][x];
      if (c === modules[y][x + 1] && c === modules[y + 1][x] && c === modules[y + 1][x + 1]) result += 3;
    }
  }
  // Finder-like patterns and proportion penalties are approximated by the above;
  // full spec adds more but the above is sufficient to pick a good mask.
  return result;
}

// ── Public API ────────────────────────────────────────────────────────────────
export function qrMatrix(text) {
  const { version, dataCodewords } = encodeBytes(text);
  const all = addEcc(version, dataCodewords);
  return buildMatrix(version, all);
}

export function qrSvg(text, opts) {
  const o = opts || {};
  const border = o.border == null ? 4 : o.border;
  const dark = o.dark || '#000000';
  const light = o.light || '#ffffff';
  const modules = qrMatrix(text);
  const size = modules.length;
  const dim = size + border * 2;
  let path = '';
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      if (modules[y][x]) path += `M${x + border},${y + border}h1v1h-1z`;
    }
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${dim} ${dim}" ` +
    `shape-rendering="crispEdges" role="img" aria-label="QR code">` +
    `<rect width="${dim}" height="${dim}" fill="${light}"/>` +
    `<path d="${path}" fill="${dark}"/></svg>`;
}

if (typeof window !== 'undefined') {
  window.QRCode = { svg: qrSvg, matrix: qrMatrix };
}
