<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { currentOrg, type OrgSummary } from '$lib/stores/currentOrg';
  import {
    bulkState,
    setCsvPath,
    setObject,
    setOperation,
    setBatchSize,
    setExternalIdFieldName,
    clearError,
    resetBulkState,
    isTerminal,
    type BulkUiState
  } from '$lib/stores/bulkState';
  import {
    validateCsv,
    startBulkUploadWithStore,
    cancelBulkUpload,
    generateBulkResults,
    listenToBulkProgress
  } from '$lib/tauri/bulk';
  import BulkUploadForm from '$lib/components/bulk/BulkUploadForm.svelte';
  import CsvValidationPanel from '$lib/components/bulk/CsvValidationPanel.svelte';
  import BulkProgressPanel from '$lib/components/bulk/BulkProgressPanel.svelte';
  import BulkResultPanel from '$lib/components/bulk/BulkResultPanel.svelte';
  import BulkErrorBanner from '$lib/components/bulk/BulkErrorBanner.svelte';

  let org = $state<OrgSummary | null>(null);
  let state = $state<BulkUiState>({
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
  });

  currentOrg.subscribe((value) => {
    org = value;
  });

  bulkState.subscribe((value) => {
    state = value;
  });

  // Listen for progress events
  let unlistenProgress: (() => void) | null = null;

  onMount(async () => {
    try {
      unlistenProgress = await listenToBulkProgress();
    } catch (e) {
      console.error('Failed to start progress listener:', e);
    }
  });

  onDestroy(() => {
    if (unlistenProgress) {
      unlistenProgress();
    }
  });

  // State for cancel confirmation
  let showCancelConfirm = $state(false);
  let cancelling = $state(false);

  // Check if we're in a terminal state
  let terminal = $derived(isTerminal(state));

  // Handlers
  async function handleValidate() {
    if (!state.csvPath) return;
    try {
      await validateCsv(state.csvPath);
    } catch (e) {
      // Error already handled in store
      console.error('Validation failed:', e);
    }
  }

  async function handleStartUpload() {
    if (!state.csvPath || !state.object.trim()) return;

    try {
      await startBulkUploadWithStore(
        state.object.trim(),
        state.operation,
        state.csvPath,
        state.batchSize,
        state.operation === 'Upsert' ? state.externalIdFieldName.trim() : undefined
      );
    } catch (e) {
      // Error already handled in store
      console.error('Upload start failed:', e);
    }
  }

  function handleCancelClick() {
    showCancelConfirm = true;
  }

  async function handleCancelConfirm() {
    if (!state.groupId) return;

    showCancelConfirm = false;
    cancelling = true;

    try {
      await cancelBulkUpload(state.groupId);
      // Progress events will handle the terminal state
    } catch (e) {
      console.error('Cancel failed:', e);
    } finally {
      cancelling = false;
    }
  }

  function handleCancelDismiss() {
    showCancelConfirm = false;
  }

  async function handleGenerateResults() {
    if (!state.groupId) return;

    try {
      await generateBulkResults(state.groupId);
    } catch (e) {
      // Error already handled in store
      console.error('Results generation failed:', e);
    }
  }

  function handleReset() {
    resetBulkState();
  }

  function handleDismissError() {
    clearError();
  }

  let hasNoOrg = $derived(!org);
</script>

<div class="bulk-page">
  <header class="page-header">
    <h1>Bulk Upload</h1>
    {#if org}
      <span class="org-badge">{org.name}</span>
    {/if}
  </header>

  {#if hasNoOrg}
    <div class="no-org-message">
      <p>Select an org to run bulk uploads</p>
      <p class="hint">Use the org switcher in the sidebar to connect to a Salesforce org.</p>
    </div>
  {:else}
    <div class="bulk-content">
      <!-- Error Banner -->
      {#if state.error && !terminal}
        <BulkErrorBanner error={state.error} onDismiss={handleDismissError} />
      {/if}

      <!-- Cancel Confirmation Modal -->
      {#if showCancelConfirm}
        <div
          class="modal-overlay"
          onclick={handleCancelDismiss}
          onkeydown={(e) => e.key === 'Escape' && handleCancelDismiss()}
          role="button"
          tabindex="-1"
        >
          <div
            class="modal-dialog"
            onclick={(e) => e.stopPropagation()}
            onkeydown={(e) => e.stopPropagation()}
            role="dialog"
            aria-modal="true"
            tabindex="-1"
          >
            <h3 class="modal-title">Cancel Upload?</h3>
            <p class="modal-message">
              In-flight jobs will be aborted best-effort. This action cannot be undone.
            </p>
            <div class="modal-actions">
              <button class="btn btn-secondary" onclick={handleCancelDismiss}>
                Keep Running
              </button>
              <button class="btn btn-danger" onclick={handleCancelConfirm}>
                Cancel Upload
              </button>
            </div>
          </div>
        </div>
      {/if}

      <!-- Main content area with form on left, status on right -->
      <div class="content-grid">
        <!-- Left: Form (hidden when running or terminal) -->
        {#if !state.running && !terminal}
          <div class="form-panel">
            <BulkUploadForm
              {state}
              onCsvPathChange={setCsvPath}
              onObjectChange={setObject}
              onOperationChange={setOperation}
              onBatchSizeChange={setBatchSize}
              onExternalIdChange={setExternalIdFieldName}
              onValidate={handleValidate}
              onStartUpload={handleStartUpload}
              disabled={false}
            />
          </div>
        {/if}

        <!-- Right: Validation / Progress / Results -->
        <div class="status-panel">
          {#if state.validating}
            <CsvValidationPanel
              validation={{ errors: [], warnings: [], stats: null }}
              validating={true}
            />
          {:else if state.validation && !state.running && !terminal}
            <CsvValidationPanel validation={state.validation} />
          {/if}

          {#if state.running && state.progress && state.groupId}
            <BulkProgressPanel
              progress={state.progress}
              groupId={state.groupId}
              onCancel={handleCancelClick}
              {cancelling}
            />
          {/if}

          {#if terminal && state.groupId && state.terminalPhase}
            <BulkResultPanel
              results={state.results!}
              groupId={state.groupId}
              terminalPhase={state.terminalPhase}
              terminalMessage={state.terminalMessage}
              onGenerateResults={state.terminalPhase === 'completed' ? handleGenerateResults : undefined}
              generating={state.generatingResults}
              onReset={handleReset}
            />
          {/if}

          {#if !state.validation && !state.validating && !state.running && !terminal}
            <div class="empty-status">
              <p>Select a CSV file and click "Validate CSV" to begin.</p>
            </div>
          {/if}
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .bulk-page {
    height: 100%;
    display: flex;
    flex-direction: column;
  }

  .page-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1.5rem;
    flex-shrink: 0;
  }

  .page-header h1 {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
  }

  .org-badge {
    padding: 0.25rem 0.75rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
  }

  .no-org-message {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
    color: var(--text-secondary, #888);
  }

  .no-org-message p {
    margin: 0;
  }

  .no-org-message .hint {
    margin-top: 0.5rem;
    font-size: 0.875rem;
  }

  .bulk-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .content-grid {
    flex: 1;
    display: grid;
    grid-template-columns: 400px 1fr;
    gap: 1.5rem;
    min-height: 0;
  }

  .form-panel {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
    padding: 1.25rem;
    overflow-y: auto;
  }

  .status-panel {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    overflow-y: auto;
  }

  .empty-status {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
    padding: 2rem;
    color: var(--text-secondary, #888);
    text-align: center;
  }

  .empty-status p {
    margin: 0;
  }

  /* Modal styles */
  .modal-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.6);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
  }

  .modal-dialog {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
    padding: 1.5rem;
    max-width: 400px;
    width: 90%;
  }

  .modal-title {
    font-size: 1.1rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
    margin: 0 0 0.75rem 0;
  }

  .modal-message {
    font-size: 0.875rem;
    color: var(--text-secondary, #888);
    margin: 0 0 1.25rem 0;
    line-height: 1.5;
  }

  .modal-actions {
    display: flex;
    gap: 0.75rem;
    justify-content: flex-end;
  }

  .btn {
    padding: 0.5rem 1rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn-secondary {
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    color: var(--text-primary, #fff);
  }

  .btn-secondary:hover {
    background: var(--bg-hover, #2a2a2a);
    border-color: var(--border-hover, #444);
  }

  .btn-danger {
    background: var(--error-color, #f44);
    border: 1px solid var(--error-color, #f44);
    color: #fff;
  }

  .btn-danger:hover {
    background: #d32f2f;
    border-color: #d32f2f;
  }

  /* Responsive adjustments */
  @media (max-width: 900px) {
    .content-grid {
      grid-template-columns: 1fr;
    }
  }
</style>
