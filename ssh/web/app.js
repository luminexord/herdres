/* herdres Mini App — viewer client.
   Mirrors the Mac's herdr TUI: relay bytes -> xterm, taps/keys -> relay. */

import { installTerminalTouchScrolling } from './touch-scroll.js';
import { renderSpacePaneNavigation } from './navigation.js';
import { hasCoarsePointer, shouldAutoFocusTerminal } from './focus-policy.js';
import * as spacePanesModule from './space-panes.js';
import { focusPaneInStatus } from './space-panes.js';
import { recoverTerminalAfterFit } from './terminal-recovery.js';
import { installScrollJoystick } from './scroll-joystick.js';
import { focusPayloadForPane, textSubmitPayload } from './app-state.js';

(() => {
  'use strict';

  const tg = window.Telegram && window.Telegram.WebApp;
  const $ = (id) => document.getElementById(id);
  const link = $('link');
  const linkLabel = $('link-label');
  const curtain = $('curtain');
  const curtainTitle = $('curtain-title');
  const curtainBody = $('curtain-body');
  const spaces = $('spaces');
  const panesBar = $('drove');
  const screen = $('screen');
  const keys = $('keys');
  const textInput = $('text-input');
  const kSend = $('k-send');
  const kKeyboard = $('k-keyboard');
  const inputRow = $('input-row');
  const kSelect = $('k-select');
  const kImage = $('k-image');
  const imageFile = $('image-file');
  const selectOverlay = $('select-overlay');
  const selectText = $('select-text');
  const kSelectDone = $('k-select-done');
  let selectedSpace = '';
  let latestStatus = { spaces: [], panes: [] };

  function focusTerminalIfAppropriate() {
    if (shouldAutoFocusTerminal({ coarsePointer: hasCoarsePointer(window) })) {
      term.focus();
    } else {
      // On mobile, focus the text input so the keyboard appears.
      textInput.focus();
    }
  }

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
  term.open(screen);

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
        latestStatus = { spaces: msg.spaces || [], panes: msg.panes || [] };
        renderNavigation();
        break;
    }
  }

  function hostUp() {
    curtain.hidden = true;
    setLink('live', 'live');
    term.reset();
    focusTerminalIfAppropriate();
    scheduleFit(); // re-sends size, which nudges herdr to repaint
  }

  function sendInput(data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(new TextEncoder().encode(data));
    }
  }

  function sendControl(obj) {
    if (!obj) return;
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  // --- sizing -------------------------------------------------------------
  let fitTimer = null;
  let fitSize = { cols: 0, rows: 0 };
  function scheduleFit() {
    clearTimeout(fitTimer);
    fitTimer = setTimeout(() => {
      try {
        fit.fit();
      } catch (_) {}
      fitSize = recoverTerminalAfterFit({ coarsePointer: hasCoarsePointer(window), previousSize: fitSize, term });
      sendControl({ t: 'resize', cols: term.cols, rows: term.rows });
    }, 120);
  }
  window.addEventListener('resize', scheduleFit);
  window.addEventListener('orientationchange', scheduleFit);

  installTerminalTouchScrolling({
    screen,
    strips: [spaces, panesBar],
    keys,
    term,
    focusOnTap: shouldAutoFocusTerminal({ coarsePointer: hasCoarsePointer(window) }),
  });
  installScrollJoystick({ root: $('scroll-joy'), onScroll: sendControl });

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
  function renderNavigation() {
    selectedSpace = renderSpacePaneNavigation({
      spacesEl: spaces,
      panesEl: panesBar,
      status: latestStatus,
      selected: selectedSpace,
      onSpace: (id) => {
        selectedSpace = id;
        sendControl({ t: 'focus_space', id });
        renderNavigation();
      },
      onPane: (id) => {
        latestStatus = focusPaneInStatus(latestStatus, id);
        renderNavigation();
        sendControl(focusPayloadForPane((latestStatus.panes || []).find((pane) => pane.id === id)));
      },
    });
  }

  // --- keyboard shortcuts (desktop) ---------------------------------------
  function cyclePane(direction) {
    const { panesForSpace, paneSpaceId } = spacePanesModule;
    const panes = latestStatus.panes || [];
    const visible = panes.filter((p) => paneSpaceId(p) === selectedSpace);
    if (!visible.length) return;
    const focusedIdx = visible.findIndex((p) => p.focused);
    const nextIdx = (focusedIdx + direction + visible.length) % visible.length;
    const target = visible[nextIdx];
    if (target) {
      latestStatus = focusPaneInStatus(latestStatus, target.id);
      renderNavigation();
      sendControl(focusPayloadForPane(target));
    }
  }

  function focusSpaceByIndex(index) {
    const { normalizeSpaces } = spacePanesModule;
    const panes = latestStatus.panes || [];
    const allSpaces = normalizeSpaces({ spaces: latestStatus.spaces || [], panes });
    if (index >= 0 && index < allSpaces.length) {
      const space = allSpaces[index];
      selectedSpace = space.id;
      sendControl({ t: 'focus_space', id: space.id });
      renderNavigation();
    }
  }

  document.addEventListener('keydown', (ev) => {
    if (ev.target !== document.body && ev.target !== screen) return;
    if (ev.metaKey || ev.ctrlKey || ev.altKey) return;
    if (ev.key === '[') { cyclePane(-1); ev.preventDefault(); return; }
    if (ev.key === ']') { cyclePane(1); ev.preventDefault(); return; }
    const digit = parseInt(ev.key, 10);
    if (digit >= 1 && digit <= 9) { focusSpaceByIndex(digit - 1); ev.preventDefault(); return; }
  });

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

  keys.addEventListener('click', (ev) => {
    const btn = ev.target.closest('.key');
    if (!btn) return;
    if (btn.dataset.mod === 'ctrl') {
      ctrlArmed ? disarmCtrl() : armCtrl();
      return;
    }
    if (btn.dataset.action === 'select') {
      toggleSelectOverlay();
      return;
    }
    if (btn.dataset.action === 'image') {
      imageFile.click();
      return;
    }
    let out = seqFor(btn);
    if (ctrlArmed && out.length === 1) {
      const code = out.toLowerCase().charCodeAt(0);
      if (code >= 97 && code <= 122) out = String.fromCharCode(code & 0x1f);
      disarmCtrl();
    }
    sendInput(out);
    focusTerminalIfAppropriate();
  });

  // --- text input row -----------------------------------------------------
  function sendTextInput() {
    const text = textInput.value;
    if (!text) return;
    const payload = textSubmitPayload({ status: latestStatus, selectedSpace, text });
    if (payload) sendControl(payload);
    else sendInput(text + '\r\n');
    textInput.value = '';
    focusTerminalIfAppropriate();
  }
  kSend.addEventListener('click', sendTextInput);
  kKeyboard.addEventListener('click', () => {
    textInput.blur();
    if (tg && typeof tg.hideKeyboard === 'function') {
      try { tg.hideKeyboard(); } catch {}
    }
  });
  textInput.addEventListener('focus', () => inputRow.classList.add('keyboard-open'));
  textInput.addEventListener('blur', () => inputRow.classList.remove('keyboard-open'));
  textInput.addEventListener('keydown', (ev) => {
    if (ev.key === 'Enter') {
      ev.preventDefault();
      sendTextInput();
    }
  });

  // --- image upload --------------------------------------------------------
  imageFile.addEventListener('change', () => {
    const file = imageFile.files && imageFile.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const b64 = btoa(String.fromCharCode(...new Uint8Array(reader.result)));
      const ext = file.name.split('.').pop() || 'jpg';
      sendControl({ t: 'image', ext, data: b64 });
    };
    reader.readAsArrayBuffer(file);
    imageFile.value = '';
  });

  // --- select overlay (native text copy) ----------------------------------
  function toggleSelectOverlay() {
    if (selectOverlay.hidden) {
      const text = serializeTerminal(term);
      selectText.textContent = text || '(terminal is empty)';
      selectOverlay.hidden = false;
      kSelect.classList.add('key--active');
    } else {
      selectOverlay.hidden = true;
      kSelect.classList.remove('key--active');
    }
  }
  kSelectDone.addEventListener('click', toggleSelectOverlay);

  function serializeTerminal(t) {
    try {
      const buf = t.buffer.active;
      if (!buf || typeof buf.length !== 'number') return '';
      const lines = [];
      for (let i = 0; i < buf.length; i++) {
        const line = buf.getLine(i);
        if (!line) continue;
        let text = '';
        try {
          text = line.translateToString(true);
        } catch {
          continue;
        }
        if (text.trim()) lines.push(text.replace(/\s+$/, ''));
      }
      while (lines.length && !lines[lines.length - 1].trim()) lines.pop();
      return lines.join('\n');
    } catch {
      return '';
    }
  }

  // --- go -----------------------------------------------------------------
  scheduleFit();
  connect();
})();
