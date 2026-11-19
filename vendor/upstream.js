"use strict";
var http = require("http"),
  https = require("https"),
  net = require("net"),
  tls = require("tls"),
  urlmod = require("url"),
  config = null;
function parse(t) {
  var n = String(null == t ? "" : t).replace(/^\s+|\s+$/g, "");
  if (!n) return null;
  var socks = /^socks5h?:\/\//i.test(n);
  if (socks) n = "http://" + n.replace(/^[a-z0-9]+:\/\//i, "");
  /^https?:\/\//i.test(n) || (n = "http://" + n);
  var r = urlmod.parse(n);
  if (!r.hostname) return null;
  var e = Number(r.port);
  return (
    (e > 0 && e < 65536) || (e = 8080),
    {
      host: r.hostname,
      port: e,
      auth: r.auth || "",
      secure: "https:" === r.protocol,
      socks: socks,
    }
  );
}
function configure(t) {
  return (config = parse(t));
}
function enabled() {
  return !!config;
}
function describe() {
  return config
    ? { host: config.host, port: config.port, auth: !!config.auth }
    : null;
}
function basicAuth() {
  return config && config.auth
    ? "Proxy-Authorization: Basic " +
        (Buffer.from
          ? Buffer.from(config.auth, "utf8").toString("base64")
          : new Buffer(config.auth, "utf8").toString("base64")) +
        "\r\n"
    : "";
}
function tunnel(t, n, r) {
  var e,
    o = !1;
  function i(t, n, e) {
    o || ((o = !0), r(t, n, e));
  }
  function fail(m) {
    try {
      e.destroy();
    } catch (t) {}
    i(new Error(m));
  }
  try {
    e = net.connect(config.port, config.host);
  } catch (t) {
    return i(t);
  }
  e.setTimeout(15e3, function () {
    try {
      e.destroy();
    } catch (t) {}
    i(new Error("proxy timeout"));
  });
  e.on("error", function (t) {
    i(t);
  });

  if (config.socks) {
    /* SOCKS5: greeting (no-auth / user-pass), CONNECT con hostname (ATYP=3 →
       el exit resuelve el DNS remoto, no el nuestro envenenado). */
    var stage = 0,
      sbuf = Buffer.alloc(0);
    function s5Request() {
      var h = Buffer.from(t, "utf8");
      if (h.length > 255) return fail("socks5: host muy largo");
      var req = Buffer.concat([
        Buffer.from([5, 1, 0, 3, h.length]),
        h,
        Buffer.alloc(2),
      ]);
      req.writeUInt16BE(n, req.length - 2);
      e.write(req);
      stage = 2;
    }
    e.on("connect", function () {
      e.write(config.auth ? Buffer.from([5, 2, 0, 2]) : Buffer.from([5, 1, 0]));
    });
    e.on("data", function onData(d) {
      sbuf = Buffer.concat([sbuf, d]);
      for (;;) {
        if (stage === 0) {
          if (sbuf.length < 2) return;
          var m = sbuf[1];
          sbuf = sbuf.slice(2);
          if (m === 0) {
            s5Request();
            continue;
          }
          if (m === 2 && config.auth) {
            var a = String(config.auth).split(":"),
              u = Buffer.from(a[0] || "", "utf8"),
              p = Buffer.from(a.slice(1).join(":") || "", "utf8");
            e.write(
              Buffer.concat([
                Buffer.from([1, u.length]),
                u,
                Buffer.from([p.length]),
                p,
              ])
            );
            stage = 1;
            continue;
          }
          return fail("socks5: sin método aceptado (" + m + ")");
        }
        if (stage === 1) {
          if (sbuf.length < 2) return;
          var ok = sbuf[1] === 0;
          sbuf = sbuf.slice(2);
          if (!ok) return fail("socks5: auth rechazada");
          s5Request();
          continue;
        }
        if (stage === 2) {
          if (sbuf.length < 5) return;
          if (sbuf[1] !== 0)
            return fail("socks5: CONNECT " + sbuf[1]);
          var atyp = sbuf[3],
            need =
              atyp === 1
                ? 10
                : atyp === 3
                  ? 7 + sbuf[4]
                  : atyp === 4
                    ? 22
                    : 0;
          if (!need) return fail("socks5: ATYP inválido");
          if (sbuf.length < need) return;
          var rest = sbuf.slice(need);
          sbuf = Buffer.alloc(0);
          e.removeListener("data", onData);
          e.setTimeout(0);
          return i(null, e, rest.length ? rest : "");
        }
      }
    });
    return;
  }

  e.on("connect", function () {
    var r =
      "CONNECT " +
      t +
      ":" +
      n +
      " HTTP/1.1\r\nHost: " +
      t +
      ":" +
      n +
      "\r\n" +
      basicAuth() +
      "\r\n";
    e.write(r);
  });
  var u = "";
  e.on("data", function t(n) {
    var r = (u += n.toString("latin1")).indexOf("\r\n\r\n");
    if (-1 !== r) {
      (e.removeListener("data", t), e.setTimeout(0));
      var o = u.slice(0, r).split("\r\n")[0] || "",
        c = parseInt(o.split(" ")[1], 10),
        a = u.slice(r + 4);
      if (200 !== c) {
        try {
          e.destroy();
        } catch (t) {}
        return i(new Error("proxy CONNECT " + (c || "sin respuesta")));
      }
      i(null, e, a);
    }
  });
}
function connectionFor(t, n, r, e, o) {
  tunnel(n, r, function (r, i) {
    if (r) return o(r);
    if (!t) return o(null, i);
    var u;
    try {
      u = tls.connect(
        { socket: i, servername: e || n, rejectUnauthorized: !1 },
        function () {
          o(null, u);
        }
      );
    } catch (t) {
      try {
        i.destroy();
      } catch (t) {}
      return o(t);
    }
    u.on("error", function (t) {
      o(t);
    });
  });
}
function agentFor(t, n, r) {
  if (config) {
    var e = new (t ? https.Agent : http.Agent)({
      keepAlive: !1,
      maxSockets: 8,
    });
    return (
      (e.createConnection = function (e, o) {
        var i = (e && (e.host || e.hostname)) || n,
          u = (e && e.port) || r || (t ? 443 : 80),
          c = (e && e.servername) || i;
        connectionFor(!!t, i, u, c, o);
      }),
      e
    );
  }
}
module.exports = {
  configure: configure,
  enabled: enabled,
  describe: describe,
  agentFor: agentFor,
  parse: parse,
};
