/**
 * Tauri bulk upload helpers.
 *
 * Provides functions to:
 * - Pick CSV files via dialog
 * - Validate CSV files
 * - Start and cancel bulk uploads
 * - Generate merged result files
 * - Listen to progress events
 */

import { invoke as tauriInvoke } from '@tauri-apps/api/core';
import { open } from '@tauri-apps/plugin-dialog';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import {
  type BulkOperation,
  type BatchSize,
  type CsvValidationResult,
  type BulkProgressEvent,
  type BulkGroupResults,
  startValidation,
  completeValidation,
  failValidation,
  startUpload,
  updateProgress,
  failUpload,
  startResultsGeneration,
  completeResultsGeneration,
  failResultsGeneration
} from '$lib/stores/bulkState';

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

/** Request to start a bulk upload */
export interface BulkUploadRequest {
  object: string;
  operation: Lowercase<BulkOperation>;
  csv_path: string;
  batch_size: BatchSize;
  external_id_field_name?: string;
}

/** Response when bulk upload starts */
export interface BulkUploadStarted {
  group_id: string;
  total_parts: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// File Picker
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Opens a file dialog to pick a CSV file.
 * Returns the file path or undefined if cancelled.
 */
export async function pickCsvFile(): Promise<string | undefined> {
  const result = await open({
    multiple: false,
    filters: [{ name: 'CSV', extensions: ['csv'] }]
  });

  // Result is null if cancelled, or a string path
  return result ?? undefined;
}

// ─────────────────────────────────────────────────────────────────────────────
// CSV Validation
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Validates a CSV file without holding the file open.
 * Updates the bulk state store.
 */
export async function validateCsv(path: string): Promise<CsvValidationResult> {
  startValidation();

  try {
    const result = await tauriInvoke<CsvValidationResult>('validate_csv', { path });
    completeValidation(result);
    return result;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    failValidation(message);
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk Upload
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Starts a bulk upload operation.
 * Updates the bulk state store and returns the group ID.
 */
export async function startBulkUploadWithStore(
  object: string,
  operation: BulkOperation,
  csvPath: string,
  batchSize: BatchSize,
  externalIdFieldName?: string
): Promise<string> {
  const request: BulkUploadRequest = {
    object,
    operation: operation.toLowerCase() as Lowercase<BulkOperation>,
    csv_path: csvPath,
    batch_size: batchSize,
    external_id_field_name: externalIdFieldName?.trim() || undefined
  };

  try {
    const result = await tauriInvoke<BulkUploadStarted>('start_bulk_upload', { request });
    startUpload(result.group_id, result.total_parts);
    return result.group_id;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    failUpload(message);
    throw error;
  }
}

/**
 * Cancels an in-progress bulk upload.
 * Does not update store - progress events will handle terminal state.
 */
export async function cancelBulkUpload(groupId: string): Promise<void> {
  await tauriInvoke<void>('cancel_bulk_upload', { group_id: groupId });
}

// ─────────────────────────────────────────────────────────────────────────────
// Results Generation
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generates merged result files for a completed bulk job group.
 * Updates the bulk state store.
 */
export async function generateBulkResults(groupId: string): Promise<BulkGroupResults> {
  startResultsGeneration();

  try {
    const result = await tauriInvoke<BulkGroupResults>('generate_bulk_results', {
      group_id: groupId
    });
    completeResultsGeneration(result);
    return result;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    failResultsGeneration(message);
    throw error;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event Listening
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Listens for bulk progress events and updates the store.
 * Returns an unlisten function.
 */
export async function listenToBulkProgress(): Promise<UnlistenFn> {
  return listen<BulkProgressEvent>('bulk-progress', (event) => {
    updateProgress(event.payload);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Clipboard
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Copies text to the clipboard.
 */
export async function copyToClipboard(text: string): Promise<void> {
  await navigator.clipboard.writeText(text);
}
