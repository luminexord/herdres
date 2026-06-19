export function shouldAutoFocusTerminal({ coarsePointer }) {
  return !coarsePointer;
}

export function hasCoarsePointer(win) {
  return !!win.matchMedia?.('(pointer: coarse)').matches;
}
