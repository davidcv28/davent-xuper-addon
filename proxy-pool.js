'use strict';
/* Pool de proxies HTTP LatAm con auto-descubrimiento.
   El portal Magis bloquea por región/IP (portal100024) SOLO en v8/active:
   Argentina, datacenters y exits Tor conocidos no pueden ACTIVAR un
   dispositivo, pero una sesión ya minteada funciona desde cualquier IP.
   Por eso la probe hace snToken+v8/active por cada proxy candidato: la
   ruta elegida sirve para mintear/renovar la sesión (adoptMint), y todo
   el tráfico restante (search, detail, resolve) va DIRECTO — el proxy
   público, que vive minutos, deja de ser el camino de las requests. */
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
/* Ventana de confianza ciega tras activar/sondear la ruta. Los proxies
   públicos duran segundos-minutos: 120s dejaba requests pudriéndose en una
   ruta ya muerta. 30s + el probe barato por request cuando vence. */
const ROUTE_OK_MS = 30e3;

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

/* La prueba de verdad: la MISMA cadena que hace ensureSession —
   v3/snToken + v8/active — porque el geo-fence (portal100024) vive en
   v8/active, no en snToken. Sondear solo pre-auth elegía proxies que
   pasaban el handshake pero no podían crear sesión.
   Bonus: v8/active exitosa mintea una cuenta REAL — el ganador adopta
   esas credenciales como sesión propia (ver adoptMint), independiente
   del pool de semillas públicas (que están quemadas para todo internet).
   El agente va atado al proxy del probe (no al config global) y corre
   fuera de la cola serial. */
const DEVICE_FILE = path.join(__dirname, 'vendor', '.xuper-device.json');
const ACT_RESERVE1 = '583442714b345572434a4d5769637461545a735936413d3d';
const SNTOKEN_SALT = 'ntFT65w6itH!lHCPw7D=@qnsFC5adD28';
const crypto = require('crypto');
function probeMac() {
  try {
    const f = JSON.parse(fs.readFileSync(DEVICE_FILE, 'utf8'));
    const m = String((f && f.macAddr) || '').trim().toLowerCase();
    if (/^([0-9a-f]{2}:){5}[0-9a-f]{2}$/.test(m)) return m;
  } catch (e) {}
  const mac =
    '02:' +
    Math.random().toString(16).slice(2, 12).padEnd(10, '0').slice(0, 10)
      .replace(/(..)(?=.)/g, '$1:');
  try {
    fs.writeFileSync(DEVICE_FILE, JSON.stringify({ macAddr: mac }));
  } catch (e) {}
  return mac;
}
function probePortal(proxy, cb) {
  const agent = upstream.agentForUrl(true, '', 443, 'http://' + proxy);
  if (!agent) return cb(false);
  /* Una sola salida: si el timeout gana y la respuesta llega después (o
     viceversa), un segundo cb() dispara done() dos veces →
     inflight.slice() sobre null → crash que deja a TODOS los waiters
     colgados. */
  let fired = false;
  const fin = (ok, mint) => {
    if (fired) return;
    fired = true;
    clearTimeout(t);
    cb(ok, mint);
  };
  const t = setTimeout(() => fin(false), ACT_TIMEOUT_MS);
  const opts = { baseFields: false, urgent: true, probe: true, agent: agent };
  /* Mismo fingerprint que manda ensureSession: un device parcial podría
     ser rechazado por el portal aunque el proxy sea bueno. */
  const fp = Object.assign({}, magis.FINGERPRINT_FIXED || {}, {
    androidId: crypto.randomBytes(8).toString('hex'),
    cpuId: crypto.randomBytes(8).toString('hex'),
    serialNumber: crypto.randomBytes(8).toString('hex'),
    etheMac: crypto.randomBytes(6).toString('hex').replace(/(..)(?=.)/g, '$1:'),
    gatewayMac: crypto.randomBytes(6).toString('hex').replace(/(..)(?=.)/g, '$1:'),
    wifiMac: crypto.randomBytes(6).toString('hex').replace(/(..)(?=.)/g, '$1:'),
  });
  magis.call('v3/snToken', fp, opts,
    (e, r) => {
      const snToken = String((r && r.snToken) || '').trim();
      if (e || !snToken) return fin(false);
      /* Misma derivación que ensureSession: si el portal no manda sn,
         se deriva del snToken con el salt del vendor. */
      const sn =
        String((r && r.sn) || '').trim() ||
        crypto.createHash('md5').update(snToken + SNTOKEN_SALT).digest('hex');
      magis.call(
        'v8/active',
        {
          snToken: snToken,
          authVersion: '',
          authCode: '',
          macAddr: probeMac(),
          reserve1: ACT_RESERVE1,
          openNum: 3,
          channel: 'default',
          matadata: '',
          signdata: '',
          sn: sn, // explícito: pisa el session.sn global de deviceDict
        },
        opts,
        (e2, r2) => {
          const userToken = String((r2 && r2.userToken) || '').trim();
          if (e2 || !userToken) return fin(false);
          fin(true, {
            userId: String((r2 && r2.userId) || '').trim(),
            userToken: userToken,
            snToken: snToken,
            sn: sn,
          });
        }
      );
    }
  );
}

/* Adopta una cuenta recién minteada como sesión viva del proceso: evita
   depender de las semillas públicas y resuelve el aaa100083 de sesiones
   compartidas. session es el objeto exportado por magis (misma referencia). */
function adoptMint(mint) {
  if (!mint || !mint.userToken) return;
  magis.session.sn = mint.sn || magis.session.sn;
  magis.session.snToken = mint.snToken || magis.session.snToken;
  magis.session.userId = mint.userId;
  magis.session.userToken = mint.userToken;
  magis.session.ready = true;
  magis.session.usesSeed = false;
  magis.session.seed = null;
  magis.session.at = Date.now();
  magis.session.lastError = '';
}

function activate(proxy, cb) {
  current = proxy;
  lastOkAt = Date.now();
  /* El geo-fence del portal solo está en v8/active: la probe ya minteó la
     sesión por el proxy (per-proxy agent) y el resto del tráfico (search,
     detail, resolve) pasa DIRECTO — comprobado: la sesión no está atada a
     IP y las llamadas autenticadas no revisan región. Dejar el proxy global
     puesto solo lo convertiría en cuello de botella: los públicos mueren
     en minutos y arrastran los resolves. */
  upstream.configure(null);
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
    probePortal(current, (ok, mint) => {
      if (!ok) {
        console.log('[xuper-proxy] ruta caída en revalidación: ' + current);
        current = null;
        onRouteLost && onRouteLost();
      } else {
        adoptMint(mint); // la revalidación mintea sesión fresca de paso
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
    probePortal(p, (ok, mint) => {
      if (settled) return;
      if (ok) {
        settled = true;
        adoptMint(mint);
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
  let doneFired = false;
  const done = (err, p) => {
    if (doneFired) return; // defensa: callbacks tardíos no re-disparan
    doneFired = true;
    const waiters = inflight ? inflight.slice() : [];
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
    probePortal(cached, (ok, mint) => {
      if (ok) {
        adoptMint(mint);
        return activate(cached, done);
      }
      next();
    });
  };

  if (current && Date.now() - lastOkAt < ROUTE_OK_MS) return done(null, current);
  if (current) {
    return probePortal(current, (ok, mint) => {
      if (ok) {
        adoptMint(mint);
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
