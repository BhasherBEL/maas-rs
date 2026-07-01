const CACHE_VERSION = 'v3';
const CACHE_NAME = `maas-shell-${CACHE_VERSION}`;

const SHELL_URLS = [
  '/',
  '/maas.js',
  '/manifest.webmanifest',
  '/icon.svg',
  '/static/js/live-view.mjs',
  '/static/js/live-logic.mjs',
  '/static/js/live-db.mjs',
  '/static/js/live-store.mjs',
  '/static/js/live-mem.mjs',
  '/static/js/vendor/sqlite-wasm/sqlite3.mjs',
  '/static/js/vendor/sqlite-wasm/sqlite3.wasm',
];

const CDN_URLS = [
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
];

const NETWORK_ONLY_PREFIXES = ['/graphql', '/graphiql'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(async (cache) => {
      await cache.addAll(SHELL_URLS);
      await Promise.allSettled(
        CDN_URLS.map((url) =>
          fetch(url, { mode: 'cors' })
            .then((r) => { if (r.ok) return cache.put(url, r); })
            .catch(() => {})
        )
      );
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

// Immutable assets (vendored sqlite, cross-origin Leaflet) stay cache-first for
// speed + offline. The app shell (HTML + our JS) is network-first so UI changes
// are picked up immediately online, falling back to cache when offline.
function isImmutable(url) {
  return url.origin !== self.location.origin
    || url.pathname.startsWith('/static/js/vendor/');
}

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  if (NETWORK_ONLY_PREFIXES.some((p) => url.pathname.startsWith(p))) return;

  if (isImmutable(url)) {
    event.respondWith(caches.match(request).then((cached) => cached || fetch(request)));
    return;
  }

  event.respondWith(
    fetch(request)
      .then((resp) => {
        if (resp && resp.ok) {
          const copy = resp.clone();
          caches.open(CACHE_NAME).then((c) => c.put(request, copy));
        }
        return resp;
      })
      .catch(() => caches.match(request).then((cached) => cached || caches.match('/')))
  );
});
