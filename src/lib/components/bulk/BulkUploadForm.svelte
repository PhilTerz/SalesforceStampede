<script lang="ts">
  import type { BulkOperation, BatchSize, BulkUiState } from '$lib/stores/bulkState';
  import FilePicker from './FilePicker.svelte';
  import BatchSizeSelect from './BatchSizeSelect.svelte';

  type Props = {
    state: BulkUiState;
    onCsvPathChange: (path: string | undefined) => void;
    onObjectChange: (object: string) => void;
    onOperationChange: (operation: BulkOperation) => void;
    onBatchSizeChange: (size: BatchSize) => void;
    onExternalIdChange: (name: string) => void;
    onValidate: () => void;
    onStartUpload: () => void;
    disabled?: boolean;
  };

  let {
    state,
    onCsvPathChange,
    onObjectChange,
    onOperationChange,
    onBatchSizeChange,
    onExternalIdChange,
    onValidate,
    onStartUpload,
    disabled = false
  }: Props = $props();

  // Form disabled when running or explicitly disabled
  let formDisabled = $derived(disabled || state.running);

  // Validate button enabled when path is set and not validating
  let canValidate = $derived(!!state.csvPath && !state.validating && !formDisabled);

  // Start enabled when form is valid
  let canStart = $derived.by(() => {
    if (formDisabled) return false;
    if (!state.csvPath) return false;
    if (!state.object.trim()) return false;
    if (!state.validation || state.validation.errors.length > 0) return false;
    if (state.operation === 'Upsert' && !state.externalIdFieldName.trim()) {
      return false;
    }
    return true;
  });

  function handleObjectInput(event: Event) {
    const target = event.target as HTMLInputElement;
    onObjectChange(target.value);
  }

  function handleOperationChange(event: Event) {
    const target = event.target as HTMLSelectElement;
    onOperationChange(target.value as BulkOperation);
  }

  function handleExternalIdInput(event: Event) {
    const target = event.target as HTMLInputElement;
    onExternalIdChange(target.value);
  }
</script>

<div class="upload-form">
  <!-- CSV File Selection -->
  <div class="form-group">
    <span class="form-label">CSV File</span>
    <FilePicker
      value={state.csvPath}
      onChange={onCsvPathChange}
      disabled={formDisabled}
    />
  </div>

  <!-- Validate Button -->
  {#if state.csvPath}
    <div class="form-group validate-group">
      <button
        class="btn btn-validate"
        onclick={onValidate}
        disabled={!canValidate}
      >
        {state.validating ? 'Validating...' : 'Validate CSV'}
      </button>
    </div>
  {/if}

  <!-- Object Name -->
  <div class="form-group">
    <label class="form-label" for="object-input">Object Name</label>
    <input
      id="object-input"
      type="text"
      class="form-input"
      value={state.object}
      oninput={handleObjectInput}
      disabled={formDisabled}
      placeholder="e.g., Account, Contact, Custom__c"
    />
  </div>

  <!-- Operation -->
  <div class="form-group">
    <label class="form-label" for="operation-select">Operation</label>
    <select
      id="operation-select"
      class="form-select"
      value={state.operation}
      onchange={handleOperationChange}
      disabled={formDisabled}
    >
      <option value="Insert">Insert</option>
      <option value="Update">Update</option>
      <option value="Upsert">Upsert</option>
      <option value="Delete">Delete</option>
    </select>
  </div>

  <!-- External ID Field (for Upsert only) -->
  {#if state.operation === 'Upsert'}
    <div class="form-group">
      <label class="form-label" for="external-id-input">
        External ID Field
        <span class="required">*</span>
      </label>
      <input
        id="external-id-input"
        type="text"
        class="form-input"
        value={state.externalIdFieldName}
        oninput={handleExternalIdInput}
        disabled={formDisabled}
        placeholder="e.g., External_Id__c"
      />
      <span class="form-hint">Required for Upsert operations</span>
    </div>
  {/if}

  <!-- Batch Size -->
  <div class="form-group">
    <span class="form-label">Batch Size</span>
    <BatchSizeSelect
      value={state.batchSize}
      onChange={onBatchSizeChange}
      disabled={formDisabled}
    />
    <span class="form-hint">Records per Salesforce job</span>
  </div>

  <!-- Start Upload Button -->
  <div class="form-group form-actions">
    <button
      class="btn btn-start"
      onclick={onStartUpload}
      disabled={!canStart}
    >
      Start Upload
    </button>
  </div>
</div>

<style>
  .upload-form {
    display: flex;
    flex-direction: column;
    gap: 1.25rem;
  }

  .form-group {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .form-label {
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--text-secondary, #888);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .required {
    color: var(--error-color, #f44);
  }

  .form-input,
  .form-select {
    padding: 0.6rem 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
  }

  .form-input:focus,
  .form-select:focus {
    outline: none;
    border-color: var(--accent-color, #4a9eff);
  }

  .form-input:disabled,
  .form-select:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .form-input::placeholder {
    color: var(--text-secondary, #888);
    opacity: 0.6;
  }

  .form-hint {
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
  }

  .validate-group {
    flex-direction: row;
    align-items: center;
  }

  .btn {
    padding: 0.6rem 1.25rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .btn-validate {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    color: var(--text-primary, #fff);
  }

  .btn-validate:hover:not(:disabled) {
    background: var(--bg-hover, #2a2a2a);
    border-color: var(--border-hover, #444);
  }

  .form-actions {
    padding-top: 0.5rem;
  }

  .btn-start {
    width: 100%;
    padding: 0.75rem 1.5rem;
    background: var(--accent-color, #4a9eff);
    border: 1px solid var(--accent-color, #4a9eff);
    color: #fff;
    font-size: 0.9rem;
  }

  .btn-start:hover:not(:disabled) {
    background: #3d8be6;
    border-color: #3d8be6;
  }
</style>
