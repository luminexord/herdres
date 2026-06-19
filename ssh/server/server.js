// Herdres cockpit server — Tailscale single-host edition.
//
// Runs on the Mac. Serves the Mini App, validates the Telegram initData of each
// viewer, and bridges WebSocket <-> a local herdr PTY. There is no broker: the
// phone reaches this directly over the tailnet, and `tailscale serve` fronts it
// with a valid ts.net HTTPS certificate. The PTY is spawned lazily on the first
// viewer and torn down after the last one leaves.
//
// Front it with, e.g.:
//   tailscale serve --bg https / http://127.0.0.1:8787
// then set the Mini App URL in BotFather to https://<mac>.<tailnet>.ts.net
//
// Env (see ssh/.env.example):
//   TELEGRAM_BOT_TOKEN   validate initData (falls back to ~/.config/herdres/herdres.env)
//   HERDRES_OWNER_ID     the only Telegram user id allowed to connect
//   PORT                 local listen port (default 8787; tailscale serve proxies to it)
//   HERDR_SHARE_CMD      command mirrored in the PTY (default "herdr")
//   HERDR_BIN            binary used for `pane list` status polling (default "herdr")

import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn as spawnProc } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const OWNER_ID = (process.env.HERDRES_OWNER_ID || '').trim();
const PORT = Number(process.env.PORT || 8787);
const WEB_DIR = path.resolve(HERE, process.env.HERDRES_WEB_DIR || '../web');
const SHARE_CMD = (process.env.HERDR_SHARE_CMD || 'herdr').trim();
const HERDR_BIN = (process.env.HERDR_BIN || 'herdr').trim();
const GRACE_MS = Number(process.env.HERDRES_IDLE_GRACE_MS || 8000);
const STATUS_INTERVAL_MS = Number(process.env.HERDRES_STATUS_INTERVAL_MS || 2500);
const INITDATA_MAX_AGE_S = Number(process.env.HERDRES_INITDATA_MAX_AGE || 86400);

function botToken() {
  const env = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
  if (env) return env;
  // Reuse the token Herdres already stores, so this needs no extra secret.
  try {
    const file = path.join(os.homedir(), '.config/herdres/herdres.env');
    for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
      const t = line.trim();
      if (t.startsWith('TELEGRAM_BOT_TOKEN=')) return t.slice('TELEGRAM_BOT_TOKEN='.length).trim();
    }
  } catch {}
  return '';
}

const TOKEN = botToken();
if (!TOKEN || !OWNER_ID) {
  console.error('[cockpit] missing TELEGRAM_BOT_TOKEN or HERDRES_OWNER_ID');
  process.exit(1);
}

const log = (...a) => console.log('[cockpit]', ...a);

// --- Telegram initData validation ----------------------------------------
function verifyInitData(initData) {
  let params;
  try {
    params = new URLSearchParams(initData);
  } catch {
    return { ok: false, reason: 'unparseable' };
  }
  const hash = params.get('hash');
  if (!hash) return { ok: false, reason: 'no_hash' };
  params.delete('hash');
  const dcs = [...params.entries()].map(([k, v]) => `${k}=${v}`).sort().join('\n');
  const secret = crypto.createHmac('sha256', 'WebAppData').update(TOKEN).digest();
  const expected = crypto.createHmac('sha256', secret).update(dcs).digest('hex');
  const a = Buffer.from(expected, 'hex');
  const b = Buffer.from(hash, 'hex');
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return { ok: false, reason: 'bad_signature' };
  const authDate = Number(params.get('auth_date') || 0);
  if (!authDate || Math.floor(Date.now() / 1000) - authDate > INITDATA_MAX_AGE_S) return { ok: false, reason: 'stale' };
  let user;
  try {
    user = JSON.parse(params.get('user') || '{}');
  } catch {
    return { ok: false, reason: 'bad_user' };
  }
  if (String(user.id) !== OWNER_ID) return { ok: false, reason: 'not_owner' };
  return { ok: true, user };
}

// --- the shared herdr PTY -------------------------------------------------
const viewers = new Set();
let term = null;
let idleTimer = null;
let statusTimer = null;
let cols = 80;
let rows = 24;

const broadcast = (data, binary) => {
  for (const v of viewers) if (v.readyState === 1) v.send(data, { binary });
};
const ctrl = (obj) => JSON.stringify(obj);
const broadcastCtrl = (obj) => broadcast(ctrl(obj), false);

async function ensureTerm() {
  if (term) return;
  let pty;
  try {
    pty = (await import('node-pty')).default;
  } catch (e) {
    log('node-pty unavailable:', e.message);
    broadcastCtrl({ t: 'host_exit', code: -1 });
    return;
  }
  const [cmd, ...args] = SHARE_CMD.split(' ');
  log(`spawning PTY: ${SHARE_CMD} (${cols}x${rows})`);
  try {
    const t = pty.spawn(cmd, args, {
      name: 'xterm-256color',
      cols,
      rows,
      cwd: os.homedir(),
      env: { ...process.env, TERM: 'xterm-256color' },
    });
    t.onData((d) => broadcast(Buffer.from(d, 'utf8'), true));
    t.onExit(({ exitCode }) => {
      log(`PTY exited (${exitCode})`);
      term = null;
      broadcastCtrl({ t: 'host_exit', code: exitCode });
    });
    term = t;
    setTimeout(() => term && term.resize(cols, rows), 150); // nudge a repaint
  } catch (e) {
    log(`failed to start "${SHARE_CMD}":`, e.message);
    term = null;
    broadcastCtrl({ t: 'host_exit', code: -1 });
  }
}

function killTerm() {
  if (!term) return;
  log('tearing down idle PTY');
  try {
    term.kill();
  } catch {}
  term = null;
}

function viewersChanged() {
  if (viewers.size > 0) {
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = null;
    }
    ensureTerm();
  } else if (term && !idleTimer) {
    idleTimer = setTimeout(() => {
      idleTimer = null;
      killTerm();
    }, GRACE_MS);
  }
}

function pollStatus() {
  if (viewers.size === 0) return;
  const p = spawnProc(HERDR_BIN, ['pane', 'list'], { timeout: 6000 });
  let out = '';
  p.stdout.on('data', (b) => (out += b));
  p.on('error', () => {});
  p.on('close', () => {
    try {
      const data = JSON.parse(out);
      const list = data.result?.panes || data.panes || [];
      const panes = list.map((x) => ({
        id: x.pane_id,
        label: x.label || x.title || x.agent || x.pane_id,
        status: x.agent_status || 'unknown',
        focused: !!x.focused,
      }));
      broadcastCtrl({ t: 'status', panes });
    } catch {}
  });
}
statusTimer = setInterval(pollStatus, STATUS_INTERVAL_MS);

// --- static serving -------------------------------------------------------
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.json': 'application/json',
};

const httpServer = http.createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  const rel = urlPath === '/' ? 'index.html' : urlPath.replace(/^\/+/, '');
  const full = path.resolve(WEB_DIR, rel);
  if (!full.startsWith(WEB_DIR)) return res.writeHead(403).end('forbidden');
  fs.readFile(full, (err, buf) => {
    if (err) return res.writeHead(404).end('not found');
    res.writeHead(200, {
      'content-type': MIME[path.extname(full)] || 'application/octet-stream',
      'content-security-policy': "frame-ancestors https://*.telegram.org https://web.telegram.org",
    });
    res.end(buf);
  });
});

// --- websocket: every client is the owner viewing the shared PTY ----------
const wss = new WebSocketServer({ server: httpServer, path: '/ws' });

wss.on('connection', (ws) => {
  ws.authed = false;
  const helloTimer = setTimeout(() => ws.close(4001, 'auth timeout'), 5000);

  ws.on('message', (data, isBinary) => {
    if (ws.authed) {
      if (isBinary) {
        if (term) term.write(data.toString('utf8')); // keystrokes
        return;
      }
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (msg.t === 'resize') {
        cols = Math.max(2, msg.cols | 0);
        rows = Math.max(2, msg.rows | 0);
        if (term) term.resize(cols, rows);
      } else if (msg.t === 'focus' && msg.id) {
        spawnProc(HERDR_BIN, ['agent', 'focus', String(msg.id)]);
      }
      return;
    }

    // First frame must be a viewer hello with valid initData.
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return ws.close(4002, 'expected hello');
    }
    if (msg.t !== 'hello' || msg.role !== 'viewer') return ws.close(4002, 'expected hello');
    const v = verifyInitData(msg.initData || '');
    if (!v.ok) return ws.close(4003, `auth: ${v.reason}`);
    clearTimeout(helloTimer);
    ws.authed = true;
    viewers.add(ws);
    ws.send(ctrl({ t: 'ready', host: true }));
    viewersChanged();
  });

  ws.on('close', () => {
    clearTimeout(helloTimer);
    viewers.delete(ws);
    viewersChanged();
  });
  ws.on('error', () => {});
});

process.on('SIGINT', () => {
  killTerm();
  process.exit(0);
});
process.on('SIGTERM', () => {
  killTerm();
  process.exit(0);
});

httpServer.listen(PORT, '127.0.0.1', () => {
  log(`listening on 127.0.0.1:${PORT} · owner ${OWNER_ID} · serving ${WEB_DIR}`);
  log('front with: tailscale serve --bg https / http://127.0.0.1:' + PORT);
});
