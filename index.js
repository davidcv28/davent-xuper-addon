/**
 * xuper-addon — adaptador Stremio del portal Xuper/Magis TV.
 *
 * Implementa {base}/stream/{type}/{id}.json para que el server lo consuma como
 * remote-provider estándar. El protocolo cifrado del portal vive en
 * vendor/magis.js (MIT © cristianco7, portado de XUPER-HYDRA-PORT-WEBOS).
 *
 * El portal no entiende ids IMDb/TMDB: mapeamos id → título (Cinemeta gratis,
 * TMDB si hay key) → magis.search → detail → resolve → streams MP4/TS con
 * proxyHeaders firmados (Content-Auth/Content-License) + subtítulos.
 *
 * Uso: node index.js   (PORT=7801 por defecto)
 */
'use strict';
// Sin estos handlers, una promesa rechazada en el bootstrap de magis
// (sesión del portal, DoH, seeds) mata el proceso — en un host el servicio
// entra en crash-loop y el health check nunca pasa. El server HTTP debe
// sobrevivir aunque el portal no responda. Van ANTES del require.
process.on('unhandledRejection', (e) => console.log('[xuper] unhandledRejection:', e && e.message));
process.on('uncaughtException', (e) => console.log('[xuper] uncaughtException:', e && e.message));

const http = require('http');
const fs = require('fs');
const path = require('path');
const magis = require('./vendor/magis.js');

const PORT = Number(process.env.PORT || 7801);
const ADDON_TOKEN = (process.env.ADDON_TOKEN || '').replace(/^\/+|\/+$/g, '');
const ROOT_ENV = path.join(__dirname, '..', '.env');

// --- .env raíz (mismo formato que server/lib/env.js, sin dependencia) --------
function readEnvFile(file) {
  const out = {};
  try {
    for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
      const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/.exec(line);
      if (!m) continue;
      out[m[1]] = m[2].trim().replace(/^["']|["']$/g, '');
    }
  } catch { /* sin .env: Cinemeta alcanza para ids tt */ }
  return out;
}
const ENV = { ...readEnvFile(ROOT_ENV), ...process.env };
const TMDB_KEY = ENV.TMDB_API_KEY || '';
// Proxy saliente para TODO el tráfico del portal (CONNECT tunnel, soporta
// auth en la URL). El portal bloquea por región/IP (portal100024): con
// XUPER_PROXY=http://host:port o http://user:pass@host:port el addon sale
// por la IP del proxy. Aplica a magis (sesiones, catálogo, streams); las
// URLs de stream resultantes las fetchea directo el player, así que el
// proxy solo importa para el handshake con el portal.
// Sin XUPER_PROXY, proxyPool descubre y rota proxies HTTP LatAm públicos:
// la sesión del portal no está atada a la IP, así que solo hace falta
// mantener UNA ruta viva.
const proxyPool = require('./proxy-pool');
if (ENV.XUPER_PROXY || ENV.HTTPS_PROXY || ENV.https_proxy) {
  const upstream = require('./vendor/upstream');
  upstream.configure(ENV.XUPER_PROXY || ENV.HTTPS_PROXY || ENV.https_proxy);
  console.log('[xuper] proxy saliente:', JSON.stringify(upstream.describe()));
} else {
  proxyPool.ensureRoute(() => {});
}
// Deadline duro por pedido /stream: con el portal caído, magis intenta el
// rescate de semillas una por una (un roundtrip c/u) y el pedido cuelga
// minutos. Mejor cortar y devolver [] — el server ya tiene su propio timeout.
const STREAM_DEADLINE_MS = 18e3;
/* Espera REAL a la ruta de proxy: ensureRoute es callback-style (el await
   pelado no esperaba nada y las requests corrían sin proxy → geo-block).
   En cold-start el descubrimiento tarda ~10-30s; cap para no colgar el
   request para siempre — si vence, handleStream igual corre (falla a [] y
   el retry lo vuelve a intentar con la ruta que ya haya quedado). */
const ROUTE_WAIT_MS = 45e3;
const waitRoute = () =>
  Promise.race([
    new Promise((res) =>
      proxyPool.ensureRoute((e) => res(!e))
    ),
    new Promise((res) => setTimeout(() => res(false), ROUTE_WAIT_MS)),
  ]);
/* Auto-restauración: cuando la revalidación del pool detecta que la ruta
   murió, arranca el re-descubrimiento AHORA (en background) en vez de
   esperar a que lo dispare la request de un usuario — para cuando llegue,
   la ruta nueva ya está viva. */
proxyPool.onRouteLost(() => {
  console.log('[xuper] ruta caída → re-descubriendo en background');
  proxyPool.ensureRoute(() => {});
});
/* Render free duerme el servicio tras ~15 min sin requests entrantes: cada
   cold-start volvía a pagar el descubrimiento de proxies. Un self-ping cada
   10 min lo mantiene despierto y con la ruta viva (solo si corre en Render,
   que exporta RENDER_EXTERNAL_URL). */
const SELF_URL = (process.env.RENDER_EXTERNAL_URL || '').replace(/\/+$/, '');
if (SELF_URL && ADDON_TOKEN) {
  setInterval(() => {
    const u = SELF_URL + '/' + ADDON_TOKEN + '/manifest.json';
    const mod = u.startsWith('https') ? require('https') : http;
    mod
      .get(u, (res) => res.resume())
      .on('error', () => {})
      .setTimeout(8000, function () {
        this.destroy();
      });
  }, 10 * 60e3).unref();
}

// --- watchdog de salud del portal --------------------------------------------
/* El addon puede quedar "vivo pero vacío": el proceso responde manifest (200)
   pero la sesión del portal o la ruta de proxy murieron en silencio y TODO
   /stream devuelve [] — síntoma real observado: Xuper desapareció del menú
   con el proceso corriendo. Un probe real periódico (search de un título que
   siempre está en el catálogo) detecta el estado y dispara el mismo reset
   que haría un reinicio: sesión limpia + re-activación + re-descubrimiento
   de ruta. Costo: una búsqueda al portal cada XUPER_HEALTH_MS (default 10min). */
const HEALTH_MS = Number(process.env.XUPER_HEALTH_MS || 10 * 60e3);
const health = { at: 0, ok: null, fails: 0, heals: 0, lastError: '' };
let healthBusy = false;
function healthProbe() {
  return Promise.race([
    cb(magis.search, 'matrix')
      .then((res) => ((res && res.items) || []).length > 0)
      .catch((e) => {
        health.lastError = (e && e.message) || 'search falló';
        return false;
      }),
    new Promise((r) => setTimeout(() => r(false), 25e3)),
  ]);
}
async function healthHeal() {
  health.heals++;
  // Los tokens cacheados llevan auth de la sesión que se está descartando:
  // limpiarlos evita servir URLs muertas tras el reset.
  try { streamCache.clear(); } catch {}
  try { magis.resetSessionAfterLiveFailure(); } catch {}
  try {
    await Promise.race([
      new Promise((r) => magis.ensureSession(() => r())),
      new Promise((r) => setTimeout(r, 45e3)),
    ]);
  } catch {}
  // La ruta del pool también pudo morir: markBad fuerza re-descubrimiento.
  if (!(ENV.XUPER_PROXY || ENV.HTTPS_PROXY || ENV.https_proxy)) {
    try { proxyPool.markBad(); } catch {}
    await waitRoute();
  }
}
async function healthTick() {
  if (healthBusy) return;
  healthBusy = true;
  try {
    health.at = Date.now();
    let ok = await healthProbe();
    if (!ok) {
      health.fails++;
      console.log(`[xuper] probe de salud FALLÓ (${health.lastError || '0 ítems'}) → reseteando sesión/ruta`);
      await healthHeal();
      ok = await healthProbe();
      console.log(`[xuper] probe post-heal: ${ok ? 'OK' : 'SIGUE MAL — se reintenta en el próximo ciclo'}`);
    }
    health.ok = ok;
    if (ok) health.fails = 0;
  } catch (e) {
    console.log('[xuper] watchdog error:', e && e.message);
  } finally {
    healthBusy = false;
  }
}
setInterval(healthTick, HEALTH_MS).unref();
// Primer probe ~90s tras el boot: atrapa una ruta/sesión muerta de nacimiento
// (el bootstrap restaura sesión cacheada que pudo expirar en el portal).
setTimeout(healthTick, 90e3).unref();

// --- helpers ----------------------------------------------------------------
const httpGetJson = (u, timeoutMs = 12000) =>
  new Promise((resolve, reject) => {
    const mod = u.startsWith('https') ? require('https') : http;
    const req = mod.get(u, { timeout: timeoutMs, headers: { 'User-Agent': 'davent-xuper/1.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return resolve(httpGetJson(new URL(res.headers.location, u).href, timeoutMs));
      }
      let body = '';
      res.on('data', (c) => (body += c));
      res.on('end', () => {
        try { resolve(JSON.parse(body)); } catch { resolve(null); }
      });
    });
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);
  });

const cb = (fn, ...args) =>
  new Promise((resolve, reject) => {
    fn(...args, (err, res) => (err ? reject(err) : resolve(res)));
  });

const norm = (s) =>
  String(s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();

// --- id → título -------------------------------------------------------------
const titleCache = new Map(); // id -> {title, year, type} (5 min)
async function titleFor(type, id, s, e) {
  const key = `${type}:${id}`;
  const hit = titleCache.get(key);
  if (hit && Date.now() - hit.at < 300000) return hit.v;

  let meta = null;
  const tmdbType = type === 'series' ? 'tv' : 'movie';
  if (TMDB_KEY) {
    // TMDB cubre tt (vía /find) y tmdb: — Cinemeta queda viejo para estrenos.
    const url = id.startsWith('tmdb:')
      ? `https://api.themoviedb.org/3/${tmdbType}/${id.slice(5)}?api_key=${TMDB_KEY}&language=es-MX`
      : `https://api.themoviedb.org/3/find/${id}?api_key=${TMDB_KEY}&external_source=imdb_id&language=es-MX`;
    const j = await httpGetJson(url);
    const r = id.startsWith('tmdb:')
      ? j
      : j && ((j.movie_results || [])[0] || (j.tv_results || [])[0]);
    if (r) {
      meta = {
        name: r.title || r.name,
        original: r.original_title || r.original_name,
        year: String(r.release_date || r.first_air_date || '').slice(0, 4),
      };
    }
  }
  if (!meta && id.startsWith('tt')) {
    const j = await httpGetJson(
      `https://cinemeta-live.strem.io/meta/${tmdbType}/${id}.json`,
    );
    if (j && j.meta && j.meta.name) {
      meta = { name: j.meta.name, year: String(j.meta.year || j.meta.releaseInfo || '').slice(0, 4) };
    }
  }
  if (meta && meta.name) {
    const v = { title: meta.name, alt: meta.original, year: meta.year, type };
    titleCache.set(key, { v, at: Date.now() });
    return v;
  }
  return null;
}

// --- match de candidatos -----------------------------------------------------
function scoreCandidate(item, want, type) {
  if (!item) return -1;
  const kind = String(item.kind || '').toLowerCase();
  if (type === 'movie' && /serie/.test(kind)) return -1;
  if (type === 'series' && /pel/.test(kind)) return -1;
  const title = norm(String(item.title || '').replace(/\s+t\d+\s*$/i, '')); // "Breaking Bad T5" → "breaking bad"
  const w = norm(want.title);
  if (!title || !w) return -1;
  let score = 0;
  if (title === w) score += 60;
  else if (title.startsWith(w) || w.startsWith(title)) score += 40;
  else if (title.includes(w) || w.includes(title)) score += 20;
  else return -1;
  const y = parseInt(item.year, 10);
  const wy = parseInt(want.year, 10);
  if (y && wy && Math.abs(y - wy) <= 1) score += 25;
  if (item.score) score += Math.min(parseFloat(item.score) || 0, 10);
  return score;
}

// Caché título→ítem del portal (~6 h): cada episodio de una misma serie
// re-pagaba search+detail enteros. El ítem cambia solo si el portal re-sube
// el contenido, cosa rara dentro de la ventana.
const PORTAL_ITEM_MS = 6 * 60 * 60e3;
const portalItemCache = new Map(); // `${type}:${title|year}` -> {v:item, at}
// Caché de detail por ítem de temporada (~1 h): la lista de episodios cambia
// lento y detail() cuesta un roundtrip al portal por capítulo pedido.
const DETAIL_MS = 60 * 60e3;
const detailCache = new Map(); // itemId -> {v:det, at}

function detailCached(id) {
  const hit = detailCache.get(id);
  if (hit && Date.now() - hit.at < DETAIL_MS) return Promise.resolve(hit.v);
  return cb(magis.detail, id).then((det) => {
    if (det) detailCache.set(id, { v: det, at: Date.now() });
    return det;
  });
}

async function findPortalItem(want, type) {
  const ck = `${type}:${norm(want.title)}|${want.year || ''}`;
  const chit = portalItemCache.get(ck);
  if (chit && Date.now() - chit.at < PORTAL_ITEM_MS) return chit.v;
  for (const q of [want.title, want.alt].filter(Boolean)) {
    let res;
    try {
      res = await cb(magis.search, q);
      console.log(`[xuper] search "${q}" → ${((res && res.items) || []).length} ítems`);
    } catch (e) {
      console.log(`[xuper] search "${q}" falló:`, e && e.message);
      continue;
    }
    const items = (res && res.items) || [];
    let best = null,
      bestScore = 0;
    for (const it of items) {
      const s = scoreCandidate(it, want, type);
      if (s > bestScore) {
        best = it;
        bestScore = s;
      }
    }
    if (best) {
      portalItemCache.set(ck, { v: best, at: Date.now() });
      return best;
    }
  }
  return null;
}

// --- resolución --------------------------------------------------------------
function toStreams(r, providerTag) {
  const out = [];
  const audio = String(r.audio || '');
  const langTxt = /spa|es/i.test(audio) ? 'Latino' : 'Español';
  // El CDN solo firma HTTP (spared_addr/main_addr del token): en https el
  // certificado no coincide con el host y la sonda/players fallan.
  const httpify = (u) => String(u || '').replace(/^https:\/\//i, 'http://');
  const mk = (url, headers, tag) => ({
    name: `Xuper${tag ? ' · ' + tag : ''} · ${r.quality || r.selectedTrack?.quality || 'SD'}`,
    title: `${langTxt} · ${r.codec || ''} · ${audio}`.trim(),
    url: httpify(url),
    audio,
    mimeType: r.mime,
    subtitles: (r.subtitles || []).map((s) => ({
      lang: s.lang,
      label: s.lang === 'es' ? 'Español' : s.lang === 'en' ? 'English' : s.lang,
      url: s.url,
      format: s.format || 'srt',
    })),
    behaviorHints: {
      notWebReady: true, // los CDN exigen las cabeceras firmadas
      proxyHeaders: { request: headers },
      videoMimeType: r.mime,
    },
  });
  out.push(mk(r.url, r.headers));
  for (let i = 0; i < (r.backupUrls || []).length && i < 3; i++) {
    const b = r.backupUrls[i];
    out.push(mk(b.url, b.headers, `CDN${i + 2}`));
  }
  return out;
}

// --- fuente Ranger (app nativa) ----------------------------------------------
// Emite un stream `xuper://<b64url>` cuyo payload es {env,entries,program} del
// motor ranger. La APK nativa lo materializa en un proxy local vía
// libranger-jni.so; la web lo ignora (url no-http no es seleccionable).
async function rangerStream(contentId, seriesContentId) {
  try {
    const p = await cb(magis.xuperProgram, contentId, seriesContentId || '');
    const payload = Buffer.from(JSON.stringify({ env: p.env, entries: p.entries, program: p.program, extra: p.extra, name: p.program.name }), 'utf8')
      .toString('base64')
      .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    const langTxt = /spa|es/i.test(p.audio || '') ? 'Latino' : 'Español';
    return {
      name: `Xuper TV · ${p.quality || 'SD'}`,
      title: `${langTxt} · ${p.audio || ''}`.trim(),
      url: `xuper://${payload}`,
      // El motor Ranger sirve HLS por su proxy local; el token es opaco y solo
      // la APK nativa lo materializa (la web lo filtra por scheme).
      type: 'hls',
      audioLang: 'es',
      variant: 'latino',
      audio: p.audio,
      subtitles: (p.subtitles || []).map((s) => ({ lang: s.lang, label: s.lang === 'es' ? 'Español' : s.lang === 'en' ? 'English' : s.lang, url: s.url, format: s.format || 'srt' })),
    };
  } catch (e) {
    console.log('[xuper] ranger program falló:', e.message);
    return null;
  }
}

async function resolveTracks(contentId, seriesContentId, opts) {
  // Las 3 pistas se resuelven EN PARALELO (antes eran secuenciales: cada
  // resolve es un startPlayVOD al portal y sumaban varios segundos).
  const [first, ...rest] = await Promise.all([
    cb(magis.resolve, contentId, seriesContentId, { ...opts, trackIndex: 0 }),
    cb(magis.resolve, contentId, seriesContentId, { ...opts, trackIndex: 1 }).catch(() => null),
    cb(magis.resolve, contentId, seriesContentId, { ...opts, trackIndex: 2 }).catch(() => null),
  ]);
  const n = Math.min((first.availableTracks || []).length, 3);
  const results = [first, ...rest.slice(0, Math.max(0, n - 1)).filter(Boolean)];
  return results.flatMap((r, i) => toStreams(r, i > 0 ? `pista${i + 1}` : ''));
}

async function handleStream(type, rawId) {
  const m = /^(tt\d+|tmdb:\d+)(?::(\d+):(\d+))?$/.exec(rawId);
  if (!m) return { streams: [] };
  const [, id, sNum, eNum] = m;
  const want = await titleFor(type, id);
  if (!want) {
    console.log('[xuper] sin metadatos para', type, id);
    return { streams: [] };
  }
  const item = await findPortalItem(want, type);
  if (!item) {
    console.log('[xuper] sin match en portal:', want.title, `(${want.year})`, 'como', type);
    return { streams: [] };
  }
  const opts = { quality: 'auto', hevc: true, uhd: true };

  if (type === 'movie') {
    try {
      // tracks y program ranger son independientes (mismo contentId): en
      // paralelo se ahorra un roundtrip completo al portal por título.
      const [streams, ranger] = await Promise.all([
        resolveTracks(item.id, null, opts),
        rangerStream(item.id, null),
      ]);
      if (ranger) streams.unshift(ranger);
      return { streams };
    } catch (e) {
      console.log('[xuper] resolve movie falló:', e.message);
      return { streams: [] };
    }
  }

  // series: cada temporada es un ítem; episodes[] lleva id propio + number
  const det = await detailCached(item.id).catch((e) => {
    console.log('[xuper] detail falló:', e && e.message);
    return null;
  });
  if (!det) return { streams: [] };
  const season = Number(sNum || 1);
  const episode = Number(eNum || 1);
  let seasonItemId = item.id;
  let eps = det.episodes || [];
  const hasEp = eps.some((x) => Number(x.number) === episode);
  if (!hasEp) {
    const s = (det.seasons || []).find((x) => Number(x.number) === season);
    if (s) {
      seasonItemId = s.id;
      const d2 = await detailCached(s.id).catch(() => null);
      eps = (d2 && d2.episodes) || [];
    }
  }
  const ep = eps.find((x) => Number(x.number) === episode);
  if (!ep) {
    console.log(`[xuper] episodio ${episode} no está en la temporada ${season} (eps: ${eps.map((x) => x.number).join(',') || 'ninguno'})`);
    return { streams: [] };
  }
  try {
    const [streams, ranger] = await Promise.all([
      resolveTracks(ep.id, seasonItemId, opts),
      rangerStream(ep.id, seasonItemId),
    ]);
    if (ranger) streams.unshift(ranger);
    return { streams };
  } catch (e) {
    console.log('[xuper] resolve ep falló:', e.message);
    return { streams: [] };
  }
}

// --- server ------------------------------------------------------------------
// Caché de resoluciones: el server llama al mismo /stream/… varias veces en
// paralelo (oleada fast+slow + índice) y cada una pagaba la cadena entera al
// portal (search→detail→resolve×3→ranger program) por la ruta proxy — 15-30 s
// por request idéntico. Los auths del CDN duran ~4 h y el server ya cachea
// 2 h en su índice, así que 30 min acá es conservador. Solo se cachean
// respuestas CON streams (un [] puede ser ruta/sesión muerta, no miss real).
const STREAM_CACHE_MS = 30 * 60e3;
const streamCache = new Map(); // 'type:id' -> {v, at}
const inflight = new Map();    // 'type:id' -> Promise (dedup en vuelo)

const MANIFEST = {
  id: 'community.davent.xuper',
  version: '1.0.0',
  name: 'XuperTV (Magis) — latino',
  description: 'Catálogo VOD del portal Xuper/Magis: películas y series en español latino.',
  resources: ['stream'],
  types: ['movie', 'series'],
  idPrefixes: ['tt', 'tmdb'],
  catalogs: [],
};

http
  .createServer(async (req, res) => {
    const send = (code, obj) => {
      const b = JSON.stringify(obj);
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'access-control-allow-origin': '*' });
      res.end(b);
    };
    try {
      console.log('[xuper] req', req.method, req.url);
      const u = new URL(req.url, 'http://x');
      // ADDON_TOKEN (opcional, para la instancia pública hosteada): si está
      // seteado, todas las rutas exigen el prefijo /{token} — como hace
      // Addon Latam con su path secreto. Sin token el comportamiento es
      // el de siempre (desarrollo local).
      let pathname = u.pathname;
      if (ADDON_TOKEN) {
        if (!pathname.startsWith(`/${ADDON_TOKEN}/`)) return send(404, { streams: [] });
        pathname = pathname.slice(ADDON_TOKEN.length + 1);
      }
      if (pathname === '/manifest.json') return send(200, MANIFEST);
      if (pathname === '/seed-status') return send(200, magis.seedStatus());
      if (pathname === '/health') return send(200, {
        ...health,
        sessionReady: !!magis.session.ready,
        seeds: magis.seedStatus(),
      });
      const m = /^\/stream\/(movie|series)\/(.+)\.json$/.exec(pathname);
      if (!m) return send(404, { streams: [] });
      const t0 = Date.now();
      const cacheKey = `${m[1]}:${decodeURIComponent(m[2])}`;
      const hit = streamCache.get(cacheKey);
      if (hit && Date.now() - hit.at < STREAM_CACHE_MS) {
        console.log(`[xuper] ${cacheKey} → ${hit.v.length} streams (cache ${Math.round((Date.now() - hit.at) / 1000)}s)`);
        return send(200, { streams: hit.v });
      }
      // Deadline por request: si el portal está caído, magis quema semillas
      // una por una sin cortar (cada rescate = un roundtrip) y el pedido
      // cuelga minutos. Mejor responder [] rápido y que el caller falle solo.
      // Sin ruta viva el deadline se fuma en reintentos de DNS/timeout;
      // ensureRoute primero (corta si ya hay proxy sano).
      const doStream = () =>
        Promise.race([
          handleStream(m[1], decodeURIComponent(m[2])),
          new Promise((r) =>
            setTimeout(() => r({ streams: [], _deadline: true }), STREAM_DEADLINE_MS)
          ),
        ]);
      const resolveOnce = async () => {
        let out;
        if (ENV.XUPER_PROXY || ENV.HTTPS_PROXY || ENV.https_proxy) {
          out = await doStream();
        } else {
          await waitRoute();
          out = await doStream();
          // 0 streams por deadline (ruta colgada — la firma típica de proxy
          // muerto) o con pool de semillas agotado → ruta sospechosa: se marca
          // mala, se re-descubre y se reintenta UNA vez. Un miss legítimo
          // (título que no está en el catálogo) responde rápido y no reintenta.
          if (!out.streams.length) {
            const st = magis.seedStatus();
            if (out._deadline || (!st.pool && st.lastError)) {
              /* Primero rotación barata: si la sesión murió, otra cuenta del
                 stockpile privado se adopta al instante — sin proxy ni
                 descubrimiento. Solo si no hay stockpile se re-descubre ruta. */
              if (!st.pool && st.lastError) proxyPool.markSessionDead();
              if (proxyPool.nextPrivateSession()) out = await doStream();
              if (!out.streams.length) {
                proxyPool.markBad();
                await waitRoute();
                out = await doStream();
              }
            }
          }
        }
        if (out.streams.length) streamCache.set(cacheKey, { v: out.streams, at: Date.now() });
        return out;
      };
      // Dedup en vuelo: N requests concurrentes al mismo id comparten UNA
      // resolución (la segunda+ espera el resultado en vez de re-pagarla).
      let p = inflight.get(cacheKey);
      if (!p) {
        p = resolveOnce().finally(() => inflight.delete(cacheKey));
        inflight.set(cacheKey, p);
      }
      const out = await p;
      console.log(`[xuper] ${m[1]} ${m[2]} → ${out.streams.length} streams (${Date.now() - t0}ms)`);
      send(200, { streams: out.streams || [] });
    } catch (e) {
      console.log('[xuper] error:', e.message);
      send(200, { streams: [] });
    }
  })
  // Bind IPv4 explícito: .listen(PORT) sin host bindea '::' y en Render los
  // health checks IPv4 no llegan (el request entra y cuelga sin respuesta).
  .listen(PORT, '0.0.0.0', () => console.log(`[xuper-addon] :${PORT} listo`));
