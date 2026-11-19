'use strict';
/* Pool de proxies HTTP LatAm con auto-descubrimiento.
   El portal Magis bloquea por región/IP (portal100024): Argentina, datacenters
   y exits Tor conocidos. Los proxies públicos LatAm sí entran, pero viven
   poco — hay que sondear, elegir uno vivo y re-sondear cuando muera.
   La sesión del portal NO está atada a la IP (las semillas lo demuestran),
   así que cambiar de proxy en caliente no mata la sesión: solo hay que
   mantener UNA ruta viva configurada en upstream. */
const net = require('net');
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const upstream = require('./vendor/upstream');
const magis = require('./vendor/magis');

const CACHE_FILE = path.join(__dirname, 'vendor', '.xuper-proxy.json');
const PROBE_HOST = 'cloudflare.com';
const CONNECT_TIMEOUT_MS = 5000;
const ACT_TIMEOUT_MS = 15000;
/* Cada 10 min se re-sondea la ruta viva: los proxies públicos mueren sin
   aviso y detectarlo acá (en vez de en la request del usuario) es lo que
   hace la restauración automática — ver onRouteLost en index.js. */
const REVALIDATE_MS = 10 * 60 * 1000;
const MAX_CANDIDATES = 150;

/* Las mismas fuentes que usa tools/mint-seeds.js del proyecto original. */
const LIST_URLS = [
  'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&country=CO,MX,PE,EC,BR,CL,UY,PY,BO,PA,CR,GT,DO,AR&anonymity=elite,anonymous&timeout=8000',
  'https://proxylist.geonode.com/api/proxy-list?country=CO&protocols=http%2Chttps&limit=30&sort_by=lastChecked&sort_type=desc',
  'https://proxylist.geonode.com/api/proxy-list?country=MX&protocols=http%2Chttps&limit=30&sort_by=lastChecked&sort_type=desc',
  'https://proxylist.geonode.com/api/proxy-list?country=PE&protocols=http%2Chttps&limit=25&sort_by=lastChecked&sort_type=desc',
  'https://proxylist.geonode.com/api/proxy-list?country=EC&protocols=http%2Chttps&limit=25&sort_by=lastChecked&sort_type=desc',
  'https://proxylist.geonode.com/api/proxy-list?country=BR&protocols=http%2Chttps&limit=25&sort_by=lastChecked&sort_type=desc',
  'https://proxylist.geonode.com/api/proxy-list?country=AR&protocols=http%2Chttps&limit=25&sort_by=lastChecked&sort_type=desc',
];

let current = null;
let inflight = null;
let revalTimer = null;
let onRouteLost = null;
let lastOkAt = 0;
const ROUTE_OK_MS = 120e3;

function fetchText(u, timeoutMs, cb) {
  const mod = u.startsWith('https:') ? https : http;
  const req = mod.get(u, { timeout: timeoutMs || 8000 }, (res) => {
    if (res.statusCode !== 200) {
      res.resume();
      return cb(new Error('HTTP ' + res.statusCode));
    }
    let d = '';
    res.on('data', (c) => (d += c));
    res.on('end', () => cb(null, d));
  });
  req.on('error', cb);
  req.on('timeout', () => {
    try {
      req.abort();
    } catch (e) {}
    cb(new Error('timeout'));
  });
}

function fetchCandidates(cb) {
  const seen = {};
  const out = [];
  let pending = LIST_URLS.length;
  LIST_URLS.forEach((u) => {
    fetchText(u, 9000, (err, d) => {
      if (!err && d) {
        if (d.trim()[0] === '{') {
          try {
            const j = JSON.parse(d);
            (j.data || []).forEach((p) => {
              const key = p.ip + ':' + p.port;
              if (!seen[key]) {
                seen[key] = 1;
                out.push(key);
              }
            });
          } catch (e) {}
        } else {
          d.split(/\r?\n/).forEach((l) => {
            const m = /^(?:https?|socks[45])?:?\/*([\d.]+:\d+)/.exec(l.trim());
            if (m && !seen[m[1]]) {
              seen[m[1]] = 1;
              out.push(m[1]);
            }
          });
        }
      }
      if (--pending === 0) cb(out);
    });
  });
}

/* testConnect puede lanzar sincrónicamente (puerto NaN, host inválido) y un
   throw dentro del forEach mata el descubrimiento entero: los waiters de
   ensureRoute quedan colgados para siempre. Blindamos cada candidato. */

function testConnect(proxy, cb) {
  const [h, po] = proxy.split(':');
  const port = parseInt(po, 10);
  if (!h || !port) return cb(false);
  let done = false;
  let s;
  try {
    s = net.connect(port, h, () => {
      s.write(
        'CONNECT ' + PROBE_HOST + ':443 HTTP/1.1\r\nHost: ' + PROBE_HOST + ':443\r\n\r\n'
      );
    });
  } catch (e) {
    return cb(false);
  }
  const fin = (ok) => {
    if (done) return;
    done = true;
    try {
      s.destroy();
    } catch (e) {}
    cb(ok);
  };
  s.setTimeout(CONNECT_TIMEOUT_MS, () => fin(false));
  s.on('error', () => fin(false));
  /* Un proxy que acepta el TCP y cierra sin responder emite 'end'/'close',
     no 'error' — sin este handler el socket muere silencioso y el contador
     de candidatos nunca completa: discovery colgado para siempre. */
  s.on('end', () => fin(false));
  s.on('close', () => fin(false));
  s.on('data', (d) => fin(/HTTP\/1\.[01] 200/.test(d.toString())));
}

/* La prueba de verdad: una llamada pre-auth del portal (geo-gated).
   snToken no pisa la sesión viva — es seguro sondear aunque ya haya una.
   El agente va atado al proxy del probe (no al config global) y corre fuera
   de la cola serial: así varios probes corren en paralelo y el descubrimiento
   en frío tarda segundos en vez de minutos. */
function probePortal(proxy, cb) {
  const agent = upstream.agentForUrl(true, '', 443, 'http://' + proxy);
  if (!agent) return cb(false);
  const t = setTimeout(() => cb(false), ACT_TIMEOUT_MS);
  magis.call(
    'v3/snToken',
    {
      androidId: 'probe' + Math.random().toString(16).slice(2, 12),
      cpuId: 'probe' + Math.random().toString(16).slice(2, 12),
    },
    { baseFields: false, urgent: true, probe: true, agent: agent },
    (e, r) => {
      clearTimeout(t);
      cb(!e && !!(r && r.snToken));
    }
  );
}

function activate(proxy, cb) {
  current = proxy;
  lastOkAt = Date.now();
  upstream.configure('http://' + proxy);
  try {
    fs.writeFileSync(
      CACHE_FILE,
      JSON.stringify({ proxy, at: Date.now() })
    );
  } catch (e) {}
  console.log('[xuper-proxy] ruta activa: ' + proxy);
  scheduleRevalidate();
  cb && cb(null, proxy);
}

function scheduleRevalidate() {
  if (revalTimer) clearTimeout(revalTimer);
  revalTimer = setTimeout(() => {
    if (!current) return;
    probePortal(current, (ok) => {
      if (!ok) {
        console.log('[xuper-proxy] ruta caída en revalidación: ' + current);
        current = null;
        onRouteLost && onRouteLost();
      } else {
        scheduleRevalidate();
      }
    });
  }, REVALIDATE_MS);
  if (revalTimer.unref) revalTimer.unref();
}

/* Sondeo en paralelo: cada probe lleva su propio agente de proxy, así que no
   se pisan entre sí. El primero que pasa activa la ruta; el resto queda
   descartado (sus sockets mueren solos al expirar). Serial era inviable en
   cold-start: 15s por candidato × N vivos = minutos sin ruta. */
const PARALLEL_PROBES = 30;
function tryCandidates(list, cb) {
  const cands = list.slice(0, PARALLEL_PROBES);
  let pending = cands.length,
    settled = false;
  if (!pending) return cb(new Error('sin proxies vivos'));
  cands.forEach((p) =>
    probePortal(p, (ok) => {
      if (settled) return;
      if (ok) {
        settled = true;
        return activate(p, cb);
      }
      if (--pending === 0) {
        settled = true;
        cb(new Error('sin proxies vivos'));
      }
    })
  );
}

/* Garantiza una ruta viva. Prioridad: XUPER_PROXY env > cache > descubrir. */
function ensureRoute(cb) {
  cb = cb || function () {};
  if (process.env.XUPER_PROXY || process.env.HTTPS_PROXY || process.env.https_proxy) {
    if (!upstream.enabled()) {
      upstream.configure(
        process.env.XUPER_PROXY || process.env.HTTPS_PROXY || process.env.https_proxy
      );
      console.log('[xuper-proxy] ruta fija por env:', JSON.stringify(upstream.describe()));
    }
    return cb(null, 'env');
  }
  if (inflight) {
    inflight.push(cb);
    return;
  }
  inflight = [cb];
  const done = (err, p) => {
    const waiters = inflight.slice();
    inflight = null;
    waiters.forEach((w) => {
      try {
        w(err, p);
      } catch (e) {}
    });
  };

  const tryCached = (next) => {
    let cached = null;
    try {
      const f = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      if (f && f.proxy && Date.now() - f.at < 12 * 3600e3) cached = f.proxy;
    } catch (e) {}
    if (!cached) return next();
    probePortal(cached, (ok) => {
      if (ok) return activate(cached, done);
      next();
    });
  };

  if (current && Date.now() - lastOkAt < ROUTE_OK_MS) return done(null, current);
  if (current) {
    return probePortal(current, (ok) => {
      if (ok) {
        lastOkAt = Date.now();
        return done(null, current);
      }
      current = null;
      tryCached(discover);
    });
  }
  tryCached(discover);

  function discover() {
    console.log('[xuper-proxy] descubriendo candidatos...');
    fetchCandidates((list) => {
      console.log('[xuper-proxy] ' + list.length + ' candidatos de las listas');
      if (!list.length) return done(new Error('listas de proxies vacías'));
      const candidates = list.slice(0, MAX_CANDIDATES);
      const live = [];
      let tested = 0;
      candidates.forEach((p) => {
        const onTested = (ok) => {
          if (ok) live.push(p);
          if (++tested === candidates.length) {
            console.log(
              '[xuper-proxy] ' +
                live.length +
                '/' +
                candidates.length +
                ' proxies con túnel CONNECT; probando portal...'
            );
            if (!live.length) return done(new Error('ningún proxy respondió CONNECT'));
            tryCandidates(live, done);
          }
        };
        try {
          testConnect(p, onTested);
        } catch (e) {
          onTested(false);
        }
      });
    });
  }
}

/* El portal dijo sesión muerta/geo-block en masa o las requests cuelgan:
   marca la ruta como mala para que el próximo ensureRoute descubra otra. */
function markBad() {
  if (process.env.XUPER_PROXY) return; // ruta fija: no hay a dónde rotar
  current = null;
}

module.exports = {
  ensureRoute,
  markBad,
  current: () => current,
  /* Hook de auto-restauración: se dispara cuando la revalidación encuentra
     la ruta muerta. index.js lo usa para re-descubrir en background, así
     la próxima request ya encuentra una ruta viva en vez de esperar el
     descubrimiento completo. */
  onRouteLost: (fn) => {
    onRouteLost = fn;
  },
};
