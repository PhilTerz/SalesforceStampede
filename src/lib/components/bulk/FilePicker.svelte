<script lang="ts">
  import { pickCsvFile } from '$lib/tauri/bulk';

  type Props = {
    value?: string;
    onChange: (path: string | undefined) => void;
    disabled?: boolean;
  };

  let { value, onChange, disabled = false }: Props = $props();

  let picking = $state(false);
  let error = $state<string | null>(null);

  async function handlePick() {
    if (disabled || picking) return;

    picking = true;
    error = null;

    try {
      const path = await pickCsvFile();
      if (path !== undefined) {
        onChange(path);
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      console.error('Failed to pick file:', e);
    } finally {
      picking = false;
    }
  }

  function handleClear() {
    onChange(undefined);
    error = null;
  }

  // Extract filename from path for display
  function getFileName(path: string): string {
    const parts = path.split(/[/\\]/);
    return parts[parts.length - 1] || path;
  }
</script>

<div class="file-picker">
  <div class="file-input-row">
    {#if value}
      <div class="selected-file">
        <span class="file-name" title={value}>{getFileName(value)}</span>
        <button
          class="btn-clear"
          onclick={handleClear}
          disabled={disabled}
          aria-label="Clear selection"
        >
          &times;
        </button>
      </div>
    {:else}
      <span class="no-file">No file selected</span>
    {/if}
    <button
      class="btn btn-pick"
      onclick={handlePick}
      disabled={disabled || picking}
    >
      {picking ? 'Picking...' : 'Browse...'}
    </button>
  </div>

  {#if value}
    <div class="file-path" title={value}>
      {value}
    </div>
  {/if}

  {#if error}
    <div class="picker-error">
      {error}
    </div>
  {/if}
</div>

<style>
  .file-picker {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .file-input-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .selected-file {
    flex: 1;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    min-width: 0;
  }

  .file-name {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
  }

  .btn-clear {
    background: none;
    border: none;
    color: var(--text-secondary, #888);
    font-size: 1.25rem;
    cursor: pointer;
    padding: 0;
    line-height: 1;
    opacity: 0.7;
  }

  .btn-clear:hover:not(:disabled) {
    opacity: 1;
    color: var(--text-primary, #fff);
  }

  .btn-clear:disabled {
    cursor: not-allowed;
    opacity: 0.3;
  }

  .no-file {
    flex: 1;
    padding: 0.5rem 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-secondary, #888);
    font-size: 0.875rem;
    font-style: italic;
  }

  .btn {
    padding: 0.5rem 1rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn-pick {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    color: var(--text-primary, #fff);
    min-width: 90px;
  }

  .btn-pick:hover:not(:disabled) {
    background: var(--bg-hover, #2a2a2a);
    border-color: var(--border-hover, #444);
  }

  .btn-pick:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .file-path {
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: monospace;
  }

  .picker-error {
    font-size: 0.8rem;
    color: var(--error-color, #f44);
    padding: 0.25rem 0;
  }
</style>
