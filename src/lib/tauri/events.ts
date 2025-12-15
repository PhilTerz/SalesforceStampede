import { listen, type UnlistenFn } from '@tauri-apps/api/event';

// ─────────────────────────────────────────────────────────────────────────────
// Event Payload Types
// ─────────────────────────────────────────────────────────────────────────────

export type QueryProgressEvent = {
  recordsFetched: number;
  totalExpected?: number;
  phase: 'starting' | 'counting' | 'fetching' | 'creating_job' | 'polling' | 'downloading' | 'complete';
  jobState?: string;
};

export type BulkProgressEvent = {
  group_id: string;
  current_part: number;
  total_parts: number;
  active_jobs: number;
  max_jobs: number;
  phase: string;
  message?: string;
};

// ─────────────────────────────────────────────────────────────────────────────
// Event Listeners
// ─────────────────────────────────────────────────────────────────────────────

export async function onQueryProgress(
  callback: (event: QueryProgressEvent) => void
): Promise<UnlistenFn> {
  return listen<QueryProgressEvent>('query:progress', (event) => {
    callback(event.payload);
  });
}

export async function onBulkProgress(
  callback: (event: BulkProgressEvent) => void
): Promise<UnlistenFn> {
  return listen<BulkProgressEvent>('bulk:progress', (event) => {
    callback(event.payload);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Convenience: Combined listener management
// ─────────────────────────────────────────────────────────────────────────────

export type EventListeners = {
  unlisten: () => void;
};

export async function setupEventListeners(handlers: {
  onQueryProgress?: (event: QueryProgressEvent) => void;
  onBulkProgress?: (event: BulkProgressEvent) => void;
}): Promise<EventListeners> {
  const unlisteners: UnlistenFn[] = [];

  if (handlers.onQueryProgress) {
    unlisteners.push(await onQueryProgress(handlers.onQueryProgress));
  }

  if (handlers.onBulkProgress) {
    unlisteners.push(await onBulkProgress(handlers.onBulkProgress));
  }

  return {
    unlisten: () => {
      unlisteners.forEach((fn) => fn());
    }
  };
}
