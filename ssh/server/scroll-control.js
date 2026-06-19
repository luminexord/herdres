import { focusedPaneWheelTarget, terminalMouseWheelInput } from './terminal-wheel.js';

export function createScrollWriter({ getSize, getTerm, loadLayout, log = () => {} }) {
  let cache = null;
  let pending = null;

  const invalidate = () => {
    cache = null;
  };

  const resolveTarget = async () => {
    const now = Date.now();
    if (cache && cache.expiresAt > now) return cache.target;
    if (!pending) {
      pending = Promise.resolve()
        .then(loadLayout)
        .then((layout) => {
          const { cols, rows } = getSize();
          const target = focusedPaneWheelTarget(layout, cols, rows);
          cache = { target, expiresAt: Date.now() + 750 };
          return target;
        })
        .finally(() => {
          pending = null;
        });
    }
    return pending;
  };

  const write = (msg) => resolveTarget()
    .then((target) => {
      if (!target) return;
      const { cols, rows } = getSize();
      const input = terminalMouseWheelInput(msg.direction, msg.units, cols, rows, target.x, target.y);
      const term = getTerm();
      if (input && term) term.write(input);
    })
    .catch((e) => log('scroll target failed:', e.message));

  return { invalidate, write };
}
