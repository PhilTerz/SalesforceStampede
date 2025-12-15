import { writable } from 'svelte/store';

export type QueryStrategy = 'auto' | 'rest' | 'bulk';

export type RestResult = {
  kind: 'rest';
  records: unknown[];
  totalSize: number;
  done: boolean;
  truncated: boolean;
};

export type BulkExportResult = {
  kind: 'bulk';
  exportPath: string;
};

export type QueryResult = RestResult | BulkExportResult;

export type QueryProgress = {
  recordsFetched: number;
  totalExpected?: number;
  phase: string;
};

export type QueryUiState = {
  soql: string;
  strategy: QueryStrategy;
  running: boolean;
  progress: QueryProgress | null;
  lastResult: QueryResult | null;
  error: string | null;
};

const DEFAULT_SOQL = 'SELECT Id, Name FROM Account LIMIT 10';

const initialState: QueryUiState = {
  soql: DEFAULT_SOQL,
  strategy: 'auto',
  running: false,
  progress: null,
  lastResult: null,
  error: null
};

export const queryState = writable<QueryUiState>(initialState);

/**
 * Updates the SOQL in the query state.
 */
export function setSoql(soql: string): void {
  queryState.update((s) => ({ ...s, soql }));
}

/**
 * Updates the strategy in the query state.
 */
export function setStrategy(strategy: QueryStrategy): void {
  queryState.update((s) => ({ ...s, strategy }));
}

/**
 * Marks query execution as started.
 */
export function startQuery(): void {
  queryState.update((s) => ({
    ...s,
    running: true,
    progress: { recordsFetched: 0, phase: 'starting' },
    lastResult: null,
    error: null
  }));
}

/**
 * Updates query progress.
 */
export function updateProgress(progress: QueryProgress): void {
  queryState.update((s) => ({ ...s, progress }));
}

/**
 * Marks query execution as completed with result.
 */
export function completeQuery(result: QueryResult): void {
  queryState.update((s) => ({
    ...s,
    running: false,
    progress: null,
    lastResult: result,
    error: null
  }));
}

/**
 * Marks query execution as failed.
 */
export function failQuery(error: string): void {
  queryState.update((s) => ({
    ...s,
    running: false,
    progress: null,
    error
  }));
}

/**
 * Clears the error state.
 */
export function clearError(): void {
  queryState.update((s) => ({ ...s, error: null }));
}

/**
 * Resets to initial state.
 */
export function resetQuery(): void {
  queryState.set(initialState);
}
