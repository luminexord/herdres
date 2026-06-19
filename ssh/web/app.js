/* herdres Mini App — viewer client.
   Mirrors the Mac's herdr TUI: relay bytes -> xterm, taps/keys -> relay. */

(() => {
  'use strict';

  const tg = window.Telegram && window.Telegram.WebApp;
  const $ = (id) => document.getElementById(id);
  const link = $('link');
  const linkLabel = $('link-label');
  const curtain = $('curtain');
  const curtainTitle = $('curtain-title');
  const curtainBody = $('curtain-body');
  const drove = $('drove');

  // --- Telegram chrome ----------------------------------------------------
  if (tg) {
    tg.ready();
    tg.expand();
    if (typeof tg.requestFullscreen === 'function') {
      try {
        tg.requestFullscreen();
      } catch (_) {}
    }
    tg.onEvent && tg.onEvent('viewportChanged', scheduleFit);
    tg.onEvent && tg.onEvent('fullscreenChanged', scheduleFit);
  }

  // --- terminal -----------------------------------------------------------
  const term = new window.Terminal({
    cursorBlink: true,
    fontFamily: "'JetBrains Mono', ui-monospace, Menlo, monospace",
    fontSize: 13,
    lineHeight: 1.0,
    letterSpacing: 0,
    scrollback: 2000,
    theme: {
      background: '#0f1117',
      foreground: '#e7e2d4',
      cursor: '#e8a14b',
      cursorAccent: '#0f1117',
      selectionBackground: '#2a2e3a',
      black: '#14161d',
      red: '#d9694a',
      green: '#88a07e',
      yellow: '#e8a14b',
      blue: '#6e8bb5',
      magenta: '#b07ea0',
      cyan: '#79a9a2',
      white: '#e7e2d4',
      brightBlack: '#5e6472',
      brightRed: '#e5805f',
      brightGreen: '#9db594',
      brightYellow: '#f2b86a',
      brightBlue: '#8aa6cc',
      brightMagenta: '#c99bbc',
      brightCyan: '#96c2ba',
      brightWhite: '#f4f0e4',
    },
  });
  const fit = new window.FitAddon.FitAddon();
  term.loadAddon(fit);
  term.open($('screen'));

  let ctrlArmed = false;
  const kCtrl = $('k-ctrl');

  // Local keystrokes (and pasted text) from the terminal -> relay.
  term.onData((data) => {
    if (ctrlArmed && data.length === 1) {
      const code = data.toLowerCase().charCodeAt(0);
      if (code >= 97 && code <= 122) data = String.fromCharCode(code & 0x1f);
      disarmCtrl();
    }
    sendInput(data);
  });

  // --- websocket ----------------------------------------------------------
  let ws = null;
  let reconnectTimer = null;
  let backoff = 1000;

  function wsUrl() {
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    return `${scheme}://${location.host}/ws`;
  }

  function connect() {
    setLink('linking', 'linking…');
    ws = new WebSocket(wsUrl());
    ws.binaryType = 'arraybuffer';

    ws.onopen = () => {
      backoff = 1000;
      const initData = tg ? tg.initData : '';
      if (!initData) {
        showCurtain('Open from Telegram', 'This cockpit only runs inside the Telegram app.');
        return;
      }
      ws.send(JSON.stringify({ t: 'hello', role: 'viewer', initData }));
    };

    ws.onmessage = (ev) => {
      if (ev.data instanceof ArrayBuffer) {
        term.write(new Uint8Array(ev.data));
        return;
      }
      let msg;
      try {
        msg = JSON.parse(ev.data);
      } catch (_) {
        return;
      }
      handleControl(msg);
    };

    ws.onclose = (ev) => {
      setLink('lost', 'lost');
      if (ev.code >= 4001 && ev.code <= 4003) {
        showCurtain('Not allowed', ev.reason || 'This session is for the herd owner only.');
        return; // auth failure — don't hammer the relay
      }
      showCurtain('The herd is out of reach', 'Reconnecting…');
      const wait = Math.min(backoff, 15000);
      backoff = Math.min(backoff * 2, 15000);
      reconnectTimer = setTimeout(connect, wait);
    };

    ws.onerror = () => {};
  }

  function handleControl(msg) {
    switch (msg.t) {
      case 'ready':
        setLink('live', 'live');
        if (msg.host) hostUp();
        else showCurtain('The herd is out of reach', null);
        break;
      case 'peer':
        if (msg.host) hostUp();
        else showCurtain('The herd is out of reach', null);
        break;
      case 'host_exit':
        showCurtain('Session ended', 'herdr closed on the Mac.');
        break;
      case 'status':
        renderDrove(msg.panes || []);
        break;
    }
  }

  function hostUp() {
    curtain.hidden = true;
    setLink('live', 'live');
    term.reset();
    term.focus();
    scheduleFit(); // re-sends size, which nudges herdr to repaint
  }

  function sendInput(data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(new TextEncoder().encode(data));
    }
  }

  function sendControl(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  // --- sizing -------------------------------------------------------------
  let fitTimer = null;
  function scheduleFit() {
    clearTimeout(fitTimer);
    fitTimer = setTimeout(() => {
      try {
        fit.fit();
      } catch (_) {}
      sendControl({ t: 'resize', cols: term.cols, rows: term.rows });
    }, 120);
  }
  window.addEventListener('resize', scheduleFit);
  window.addEventListener('orientationchange', scheduleFit);

  // --- link + curtain -----------------------------------------------------
  function setLink(state, label) {
    link.className = `link link--${state}`;
    linkLabel.textContent = label;
  }
  function showCurtain(title, body) {
    curtainTitle.textContent = title;
    if (body !== null) curtainBody.innerHTML = body
      ? body
      : 'Start <code>herdr-share</code> on your Mac to bring the session in.';
    curtain.hidden = false;
  }

  // --- drove line ---------------------------------------------------------
  const STATUS = { working: 'working', idle: 'idle', blocked: 'blocked', done: 'done' };
  function renderDrove(panes) {
    if (!panes.length) {
      drove.innerHTML = '<span class="drove__empty">no panes yet</span>';
      return;
    }
    drove.innerHTML = '';
    for (const p of panes) {
      const el = document.createElement('button');
      el.className = 'head' + (p.focused ? ' head--focused' : '');
      el.dataset.status = STATUS[p.status] || 'idle';
      el.innerHTML = `<span class="head__pip"></span><span class="head__name"></span>`;
      el.querySelector('.head__name').textContent = p.label;
      el.addEventListener('click', () => {
        sendControl({ t: 'focus', id: p.id });
        term.focus();
      });
      drove.appendChild(el);
    }
  }

  // --- key bar ------------------------------------------------------------
  function armCtrl() {
    ctrlArmed = true;
    kCtrl.classList.add('key--armed');
  }
  function disarmCtrl() {
    ctrlArmed = false;
    kCtrl.classList.remove('key--armed');
  }

  function seqFor(btn) {
    const raw = btn.getAttribute('data-seq');
    if (raw === '') return '\x1b'; // esc
    if (raw[0] === '[') return '\x1b' + raw; // CSI: arrows
    if (raw === '\\t') return '\t';
    if (raw === '\\r') return '\r';
    return raw;
  }

  $('keys').addEventListener('click', (ev) => {
    const btn = ev.target.closest('.key');
    if (!btn) return;
    if (btn.dataset.mod === 'ctrl') {
      ctrlArmed ? disarmCtrl() : armCtrl();
      return;
    }
    let out = seqFor(btn);
    if (ctrlArmed && out.length === 1) {
      const code = out.toLowerCase().charCodeAt(0);
      if (code >= 97 && code <= 122) out = String.fromCharCode(code & 0x1f);
      disarmCtrl();
    }
    sendInput(out);
    term.focus();
  });

  // --- go -----------------------------------------------------------------
  scheduleFit();
  connect();
})();
