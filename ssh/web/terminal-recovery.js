export function shouldRecoverTerminalViewport({
  coarsePointer,
  previousCols,
  previousRows,
  nextCols,
  nextRows,
}) {
  if (!coarsePointer || !previousCols || !previousRows) return false;
  return previousCols !== nextCols || previousRows !== nextRows;
}

export function recoverTerminalViewport(term) {
  term.clear?.();
  term.scrollToBottom?.();
}

export function recoverTerminalAfterFit({ coarsePointer, previousSize, term }) {
  const nextSize = { cols: term.cols, rows: term.rows };
  if (shouldRecoverTerminalViewport({
    coarsePointer,
    previousCols: previousSize.cols,
    previousRows: previousSize.rows,
    nextCols: nextSize.cols,
    nextRows: nextSize.rows,
  })) recoverTerminalViewport(term);
  return nextSize;
}
