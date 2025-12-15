/**
 * Query execution Tauri helpers.
 * Wraps the execute_query command and provides state management integration.
 */

import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { openUrl } from '@tauri-apps/plugin-opener';
import {
  executeQuery as invokeExecuteQuery,
  type ExecuteQueryRequest,
  type ExecuteQueryResult
} from './invoke';
import {
  startQuery,
  updateProgress,
  completeQuery,
  failQuery,
  type QueryStrategy,
  type QueryResult,
  type QueryProgress
} from '$lib/stores/queryState';
import { flattenRecords } from '$lib/query/flatten';

const MAX_GRID_ROWS = 50_000;

/**
 * Query progress event from the backend.
 */
type QueryProgressEvent = {
  recordsFetched: number;
  totalExpected?: number;
  phase: string;
  jobState?: string;
};

/**
 * Converts backend result to QueryResult for the store.
 */
function toQueryResult(result: ExecuteQueryResult): QueryResult {
  if (result.exportPath && result.strategyUsed === 'bulk') {
    return {
      kind: 'bulk',
      exportPath: result.exportPath
    };
  }

  return {
    kind: 'rest',
    records: result.records,
    totalSize: result.totalSize,
    done: result.done,
    truncated: result.truncated
  };
}

/**
 * Executes a SOQL query with progress tracking.
 * Updates the query state store throughout execution.
 *
 * @param soql - The SOQL query to execute
 * @param strategy - The query strategy (auto/rest/bulk)
 * @returns The query result
 */
export async function runQuery(
  soql: string,
  strategy: QueryStrategy
): Promise<QueryResult> {
  // Set up progress listener
  let unlisten: UnlistenFn | null = null;

  try {
    // Mark query as started
    startQuery();

    // Listen for progress events
    unlisten = await listen<QueryProgressEvent>('query:progress', (event) => {
      const progress: QueryProgress = {
        recordsFetched: event.payload.recordsFetched,
        totalExpected: event.payload.totalExpected,
        phase: event.payload.phase
      };
      updateProgress(progress);
    });

    // Build the request
    const request: ExecuteQueryRequest = {
      soql,
      strategy,
      maxRows: MAX_GRID_ROWS
    };

    // Execute the query
    const result = await invokeExecuteQuery(request);

    // Convert to our result type
    const queryResult = toQueryResult(result);

    // Mark as complete
    completeQuery(queryResult);

    return queryResult;
  } catch (e) {
    const errorMessage = e instanceof Error ? e.message : 'Query execution failed';
    failQuery(errorMessage);
    throw e;
  } finally {
    // Clean up progress listener
    if (unlisten) {
      unlisten();
    }
  }
}

/**
 * Opens a Salesforce record in the system browser.
 *
 * @param instanceUrl - The Salesforce instance URL
 * @param recordId - The record ID to open
 */
export async function openRecordInBrowser(
  instanceUrl: string,
  recordId: string
): Promise<void> {
  // Ensure URL doesn't have trailing slash
  const baseUrl = instanceUrl.replace(/\/$/, '');
  const url = `${baseUrl}/${recordId}`;
  await openUrl(url);
}

/**
 * Flattens query results for grid display.
 * Returns both flattened records and column definitions.
 */
export function prepareRecordsForGrid(records: unknown[]): {
  flatRecords: Record<string, unknown>[];
  columns: string[];
} {
  const flatRecords = flattenRecords(records);

  // Extract columns from first 50 records
  const columnSet = new Set<string>();
  const sample = flatRecords.slice(0, 50);
  for (const record of sample) {
    for (const key of Object.keys(record)) {
      columnSet.add(key);
    }
  }

  // Sort columns: Id first, then alphabetically
  const columns = Array.from(columnSet).sort((a, b) => {
    if (a === 'Id') return -1;
    if (b === 'Id') return 1;
    return a.localeCompare(b);
  });

  return { flatRecords, columns };
}

/**
 * Copies text to clipboard.
 */
export async function copyToClipboard(text: string): Promise<void> {
  await navigator.clipboard.writeText(text);
}
