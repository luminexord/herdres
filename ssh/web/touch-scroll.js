const MIN_TOUCH_SCROLL_PX = 8;

export function isTapGesture({ startX, startY, endX, endY, maxDistance = 12 }) {
  return Math.hypot(endX - startX, endY - startY) <= maxDistance;
}

export function terminalSwipeAction({ startY, currentY }) {
  return Math.abs(currentY - startY) < MIN_TOUCH_SCROLL_PX ? 'ignore' : 'suppress';
}

export function terminalTapAction({ moved, tapped, focusOnTap }) {
  if (moved || !tapped) return 'none';
  return focusOnTap ? 'focus' : 'suppress';
}

export function installTerminalTouchScrolling({
  screen,
  drove = null,
  strips = [],
  keys,
  term,
  focusOnTap = true,
}) {
  const state = {
    active: false,
    startX: 0,
    startY: 0,
    moved: false,
  };

  screen.style.touchAction = 'none';
  for (const strip of strips.length ? strips : [drove]) {
    if (strip) strip.style.touchAction = 'pan-x';
  }
  keys.style.touchAction = 'pan-x';

  screen.addEventListener('touchstart', (ev) => {
    if (ev.touches.length !== 1) return;
    const touch = ev.touches[0];
    state.active = true;
    state.startX = touch.clientX;
    state.startY = touch.clientY;
    state.moved = false;
  }, { passive: true });

  screen.addEventListener('touchmove', (ev) => {
    if (!state.active || ev.touches.length !== 1) return;
    const touch = ev.touches[0];
    if (terminalSwipeAction({ startY: state.startY, currentY: touch.clientY }) === 'ignore') return;
    state.moved = true;
    ev.preventDefault();
  }, { passive: false });

  screen.addEventListener('touchend', (ev) => {
    if (!state.active) return;
    const touch = ev.changedTouches[0];
    const tapped = touch && isTapGesture({
      startX: state.startX,
      startY: state.startY,
      endX: touch.clientX,
      endY: touch.clientY,
    });
    const action = terminalTapAction({ moved: state.moved, tapped: !!tapped, focusOnTap });
    state.active = false;
    if (action === 'focus') {
      term.focus();
    } else if (action === 'suppress') {
      ev.preventDefault();
    }
  }, { passive: false });

  screen.addEventListener('touchcancel', () => {
    state.active = false;
  }, { passive: true });
}
