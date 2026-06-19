export function scrollJoystickMessage(direction, step = 3) {
  const count = Math.max(1, Math.trunc(Number(step) || 1));
  if (direction === 'down' || direction === 'up') return { t: 'scroll', direction, units: count };
  return null;
}

export function installScrollJoystick({ root, onScroll, target = null, step = 3, repeatMs = 110 }) {
  if (!root) return;
  let repeatTimer = null;
  let activeButton = null;

  const stop = () => {
    if (repeatTimer) clearInterval(repeatTimer);
    repeatTimer = null;
    activeButton?.classList.remove('scroll-joy__button--active');
    activeButton = null;
  };

  const start = (button) => {
    const msg = scrollJoystickMessage(button.dataset.scroll, step);
    if (!msg) return;
    const send = () => onScroll({ ...msg, ...(target ? target() : {}) });
    stop();
    activeButton = button;
    activeButton.classList.add('scroll-joy__button--active');
    send();
    repeatTimer = setInterval(send, repeatMs);
  };

  root.addEventListener('pointerdown', (ev) => {
    const button = ev.target.closest('button[data-scroll]');
    if (!button || !root.contains(button)) return;
    ev.preventDefault();
    start(button);
  });
  root.addEventListener('pointerup', stop);
  root.addEventListener('pointercancel', stop);
  root.addEventListener('pointerleave', stop);
  root.addEventListener('lostpointercapture', stop);
  root.addEventListener('contextmenu', (ev) => ev.preventDefault());
}
