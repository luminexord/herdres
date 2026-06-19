const WHEEL_UP = 64;
const WHEEL_DOWN = 65;

function center(size) {
  return Math.max(1, Math.trunc(Number(size) / 2) || 1);
}

function cell(value, fallback, limit) {
  const max = Math.max(1, Math.trunc(Number(limit) || 1));
  if (value === null || value === undefined) return Math.min(max, fallback);
  const n = Math.trunc(Number(value));
  if (!Number.isFinite(n)) return Math.min(max, fallback);
  return Math.min(max, Math.max(1, n));
}

function rectCenter(rect, cols, rows) {
  const width = Math.max(1, Math.trunc(Number(rect?.width) || 1));
  const height = Math.max(1, Math.trunc(Number(rect?.height) || 1));
  const rawX = Math.trunc(Number(rect?.x) || 0) + Math.max(1, Math.trunc(width / 2));
  const rawY = Math.trunc(Number(rect?.y) || 0) + Math.max(1, Math.trunc(height / 2));
  return {
    x: cell(rawX, center(cols), cols),
    y: cell(rawY, center(rows), rows),
  };
}

export function focusedPaneWheelTarget(layoutData, cols = 80, rows = 24) {
  const layout = layoutData?.result?.layout || layoutData?.layout || layoutData;
  const panes = Array.isArray(layout?.panes) ? layout.panes : [];
  const focused = panes.find((pane) => pane.focused || pane.pane_id === layout?.focused_pane_id);
  if (!focused?.rect) return null;
  return rectCenter(focused.rect, cols, rows);
}

export function terminalMouseWheelInput(direction, units = 1, cols = 80, rows = 24, x = null, y = null) {
  const button = direction === 'up' ? WHEEL_UP : direction === 'down' ? WHEEL_DOWN : null;
  if (button === null) return '';
  const count = Math.max(1, Math.trunc(Number(units) || 1));
  const targetX = cell(x, center(cols), cols);
  const targetY = cell(y, center(rows), rows);
  return `\x1b[<${button};${targetX};${targetY}M`.repeat(count);
}
