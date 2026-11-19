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

async function findPortalItem(want, type) {
  for (const q of [want.title, want.alt].filter(Boolean)) {
    let res;
    try {
      res = await cb(magis.search, q);
    } catch { continue; }
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
    if (best) return best;
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

async function resolveTracks(contentId, seriesContentId, opts) {
  const first = await cb(magis.resolve, contentId, seriesContentId, { ...opts, trackIndex: 0 });
  const results = [first];
  const n = Math.min((first.availableTracks || []).length, 3);
  for (let i = 1; i < n; i++) {
    try {
      results.push(await cb(magis.resolve, contentId, seriesContentId, { ...opts, trackIndex: i }));
    } catch { /* una pista rota no tumba al resto */ }
  }
  return results.flatMap((r, i) => toStreams(r, i > 0 ? `pista${i + 1}` : ''));
}

async function handleStream(type, rawId) {
  const m = /^(tt\d+|tmdb:\d+)(?::(\d+):(\d+))?$/.exec(rawId);
  if (!m) return { streams: [] };
  const [, id, sNum, eNum] = m;
  const want = await titleFor(type, id);
  if (!want) return { streams: [] };
  const item = await findPortalItem(want, type);
  if (!item) return { streams: [] };
  const opts = { quality: 'auto', hevc: true, uhd: true };

  if (type === 'movie') {
    try {
      return { streams: await resolveTracks(item.id, null, opts) };
    } catch (e) {
      console.log('[xuper] resolve movie falló:', e.message);
      return { streams: [] };
    }
  }

  // series: cada temporada es un ítem; episodes[] lleva id propio + number
  const det = await cb(magis.detail, item.id).catch(() => null);
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
      const d2 = await cb(magis.detail, s.id).catch(() => null);
      eps = (d2 && d2.episodes) || [];
    }
  }
  const ep = eps.find((x) => Number(x.number) === episode);
  if (!ep) return { streams: [] };
  try {
    return { streams: await resolveTracks(ep.id, seasonItemId, opts) };
  } catch (e) {
    console.log('[xuper] resolve ep falló:', e.message);
    return { streams: [] };
  }
}

// --- server ------------------------------------------------------------------
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
      const m = /^\/stream\/(movie|series)\/(.+)\.json$/.exec(pathname);
      if (!m) return send(404, { streams: [] });
      const t0 = Date.now();
      const out = await handleStream(m[1], decodeURIComponent(m[2]));
      console.log(`[xuper] ${m[1]} ${m[2]} → ${out.streams.length} streams (${Date.now() - t0}ms)`);
      send(200, out);
    } catch (e) {
      console.log('[xuper] error:', e.message);
      send(200, { streams: [] });
    }
  })
  // Bind todas las interfaces: en la nube (Render/Koyeb) es obligatorio y
  // en local el server sigue alcanzándolo por 127.0.0.1 igual.
  .listen(PORT, () => console.log(`[xuper-addon] :${PORT} listo`));
