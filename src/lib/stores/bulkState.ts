/**
 * Bulk upload state management for the Bulk Upload UI.
 */
import { writable, get } from 'svelte/store';

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

/** Bulk operation types - matches Rust BulkOperation */
export type BulkOperation = 'Insert' | 'Update' | 'Upsert' | 'Delete';

/**
 * Batch size type - matches Rust BatchSizeDto with snake_case serde renaming.
 * Note: Custom is a tuple variant so it serializes as { custom: number }.
 */
export type BatchSize =
  | 'extra_small'
  | 'small'
  | 'medium'
  | 'large'
  | { custom: number };

/** CSV validation error from backend */
export interface CsvValidationError {
  code: string;
  message: string;
}

/** CSV validation warning from backend */
export interface CsvValidationWarning {
  code: string;
  message: string;
}

/** CSV validation stats from backend */
export interface CsvValidationStats {
  file_size: number;
  sample_bytes: number;
  headers: string[];
  inferred_columns: number;
  line_endings: 'lf' | 'crlf' | 'mixed';
  estimated_rows: number | null;
}

/** CSV validation result from backend */
export interface CsvValidationResult {
  errors: CsvValidationError[];
  warnings: CsvValidationWarning[];
  stats: CsvValidationStats | null;
}

/** Progress event from bulk-progress event */
export interface BulkProgressEvent {
  group_id: string;
  current_part: number;
  total_parts: number;
  active_jobs: number;
  max_jobs: number;
  phase: string;
  message?: string;
}

/** Merged result paths from generate_bulk_results */
export interface BulkGroupResults {
  success_path: string;
  failure_path: string;
  warnings_path?: string;
}

/** Bulk UI state */
export interface BulkUiState {
  // Form fields
  csvPath?: string;
  object: string;
  operation: BulkOperation;
  batchSize: BatchSize;
  externalIdFieldName: string;

  // Validation state
  validating: boolean;
  validation?: CsvValidationResult;

  // Upload state
  running: boolean;
  groupId?: string;
  totalParts?: number;

  // Progress (from events)
  progress?: {
    phase: string;
    currentPart: number;
    totalParts: number;
    activeJobs: number;
    maxJobs: number;
    message?: string;
  };

  // Terminal state
  terminalPhase?: 'completed' | 'cancelled' | 'failed';
  terminalMessage?: string;

  // Results (after generation)
  results?: BulkGroupResults;
  generatingResults: boolean;

  // Error state
  error?: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Initial State
// ─────────────────────────────────────────────────────────────────────────────

const initialState: BulkUiState = {
  csvPath: undefined,
  object: '',
  operation: 'Insert',
  batchSize: 'medium',
  externalIdFieldName: '',

  validating: false,
  validation: undefined,

  running: false,
  groupId: undefined,
  totalParts: undefined,

  progress: undefined,

  terminalPhase: undefined,
  terminalMessage: undefined,

  results: undefined,
  generatingResults: false,

  error: undefined
};

// ─────────────────────────────────────────────────────────────────────────────
// Store
// ─────────────────────────────────────────────────────────────────────────────

export const bulkState = writable<BulkUiState>({ ...initialState });

// ─────────────────────────────────────────────────────────────────────────────
// Actions
// ─────────────────────────────────────────────────────────────────────────────

/** Update CSV path and clear validation */
export function setCsvPath(path: string | undefined) {
  bulkState.update((s) => ({
    ...s,
    csvPath: path,
    validation: undefined,
    error: undefined
  }));
}

/** Update object name */
export function setObject(object: string) {
  bulkState.update((s) => ({ ...s, object }));
}

/** Update operation */
export function setOperation(operation: BulkOperation) {
  bulkState.update((s) => ({ ...s, operation }));
}

/** Update batch size */
export function setBatchSize(batchSize: BatchSize) {
  bulkState.update((s) => ({ ...s, batchSize }));
}

/** Update external ID field name */
export function setExternalIdFieldName(name: string) {
  bulkState.update((s) => ({ ...s, externalIdFieldName: name }));
}

/** Start validation */
export function startValidation() {
  bulkState.update((s) => ({
    ...s,
    validating: true,
    validation: undefined,
    error: undefined
  }));
}

/** Complete validation */
export function completeValidation(result: CsvValidationResult) {
  bulkState.update((s) => ({
    ...s,
    validating: false,
    validation: result
  }));
}

/** Fail validation */
export function failValidation(error: string) {
  bulkState.update((s) => ({
    ...s,
    validating: false,
    error
  }));
}

/** Start upload */
export function startUpload(groupId: string, totalParts: number) {
  bulkState.update((s) => ({
    ...s,
    running: true,
    groupId,
    totalParts,
    progress: {
      phase: 'starting',
      currentPart: 0,
      totalParts,
      activeJobs: 0,
      maxJobs: 3,
      message: undefined
    },
    terminalPhase: undefined,
    terminalMessage: undefined,
    results: undefined,
    error: undefined
  }));
}

/** Update progress from event */
export function updateProgress(event: BulkProgressEvent) {
  const state = get(bulkState);

  // Only apply events for the current group
  if (state.groupId && event.group_id !== state.groupId) {
    return;
  }

  // Check for terminal phases
  const terminalPhases = ['completed', 'cancelled', 'failed'];
  const isTerminal = terminalPhases.includes(event.phase);

  bulkState.update((s) => ({
    ...s,
    running: !isTerminal,
    progress: {
      phase: event.phase,
      currentPart: event.current_part,
      totalParts: event.total_parts,
      activeJobs: event.active_jobs,
      maxJobs: event.max_jobs,
      message: event.message
    },
    terminalPhase: isTerminal ? (event.phase as 'completed' | 'cancelled' | 'failed') : s.terminalPhase,
    terminalMessage: isTerminal ? event.message : s.terminalMessage
  }));
}

/** Fail upload */
export function failUpload(error: string) {
  bulkState.update((s) => ({
    ...s,
    running: false,
    terminalPhase: 'failed',
    terminalMessage: error,
    error
  }));
}

/** Start results generation */
export function startResultsGeneration() {
  bulkState.update((s) => ({
    ...s,
    generatingResults: true,
    error: undefined
  }));
}

/** Complete results generation */
export function completeResultsGeneration(results: BulkGroupResults) {
  bulkState.update((s) => ({
    ...s,
    generatingResults: false,
    results
  }));
}

/** Fail results generation */
export function failResultsGeneration(error: string) {
  bulkState.update((s) => ({
    ...s,
    generatingResults: false,
    error
  }));
}

/** Clear error */
export function clearError() {
  bulkState.update((s) => ({ ...s, error: undefined }));
}

/** Reset to initial state */
export function resetBulkState() {
  bulkState.set({ ...initialState });
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/** Check if form is valid for starting upload */
export function isFormValid(state: BulkUiState): boolean {
  // CSV path required
  if (!state.csvPath) return false;

  // Object name required
  if (!state.object.trim()) return false;

  // Validation must have passed with no errors
  if (!state.validation || state.validation.errors.length > 0) return false;

  // External ID required for Upsert
  if (state.operation === 'Upsert' && !state.externalIdFieldName.trim()) {
    return false;
  }

  // Not already running
  if (state.running) return false;

  return true;
}

/** Format batch size for display */
export function formatBatchSize(batchSize: BatchSize): string {
  if (typeof batchSize === 'string') {
    switch (batchSize) {
      case 'extra_small':
        return 'Extra Small (10)';
      case 'small':
        return 'Small (200)';
      case 'medium':
        return 'Medium (2,000)';
      case 'large':
        return 'Large (10,000)';
      default:
        return batchSize;
    }
  }
  return `Custom (${batchSize.custom.toLocaleString()})`;
}

/** Check if current state is terminal */
export function isTerminal(state: BulkUiState): boolean {
  return state.terminalPhase !== undefined;
}
