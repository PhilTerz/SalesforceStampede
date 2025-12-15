<script lang="ts">
  import type { BulkGroupResults } from '$lib/stores/bulkState';
  import { copyToClipboard } from '$lib/tauri/bulk';

  type Props = {
    results: BulkGroupResults;
    groupId: string;
    terminalPhase: 'completed' | 'cancelled' | 'failed';
    terminalMessage?: string;
    onGenerateResults?: () => void;
    generating?: boolean;
    onReset: () => void;
  };

  let {
    results,
    groupId,
    terminalPhase,
    terminalMessage,
    onGenerateResults,
    generating = false,
    onReset
  }: Props = $props();

  // Track which paths have been copied
  let copiedPath = $state<string | null>(null);

  async function handleCopy(path: string) {
    try {
      await copyToClipboard(path);
      copiedPath = path;
      setTimeout(() => {
        copiedPath = null;
      }, 2000);
    } catch (e) {
      console.error('Failed to copy:', e);
    }
  }

  // Get filename from path
  function getFileName(path: string): string {
    const parts = path.split(/[/\\]/);
    return parts[parts.length - 1] || path;
  }

  // Status styling
  let statusClass = $derived.by(() => {
    switch (terminalPhase) {
      case 'completed':
        return 'status-success';
      case 'cancelled':
        return 'status-warning';
      case 'failed':
        return 'status-error';
      default:
        return '';
    }
  });

  let statusIcon = $derived.by(() => {
    switch (terminalPhase) {
      case 'completed':
        return '\u2713'; // checkmark
      case 'cancelled':
        return '\u26A0'; // warning
      case 'failed':
        return '\u2717'; // x
      default:
        return '';
    }
  });

  let statusLabel = $derived.by(() => {
    switch (terminalPhase) {
      case 'completed':
        return 'Upload Completed';
      case 'cancelled':
        return 'Upload Cancelled';
      case 'failed':
        return 'Upload Failed';
      default:
        return '';
    }
  });
</script>

<div class="result-panel">
  <div class="result-header">
    <div class="status-badge {statusClass}">
      <span class="status-icon">{statusIcon}</span>
      <span>{statusLabel}</span>
    </div>
  </div>

  {#if terminalMessage}
    <div class="terminal-message {statusClass}">
      {terminalMessage}
    </div>
  {/if}

  {#if results}
    <div class="result-files">
      <h4 class="files-title">Result Files</h4>

      <div class="file-row">
        <div class="file-info">
          <span class="file-label success-label">Success:</span>
          <span class="file-name" title={results.success_path}>
            {getFileName(results.success_path)}
          </span>
        </div>
        <button
          class="btn btn-copy"
          onclick={() => handleCopy(results.success_path)}
        >
          {copiedPath === results.success_path ? 'Copied!' : 'Copy Path'}
        </button>
      </div>

      <div class="file-row">
        <div class="file-info">
          <span class="file-label failure-label">Failures:</span>
          <span class="file-name" title={results.failure_path}>
            {getFileName(results.failure_path)}
          </span>
        </div>
        <button
          class="btn btn-copy"
          onclick={() => handleCopy(results.failure_path)}
        >
          {copiedPath === results.failure_path ? 'Copied!' : 'Copy Path'}
        </button>
      </div>

      {#if results.warnings_path}
        <div class="file-row">
          <div class="file-info">
            <span class="file-label warning-label">Warnings:</span>
            <span class="file-name" title={results.warnings_path}>
              {getFileName(results.warnings_path)}
            </span>
          </div>
          <button
            class="btn btn-copy"
            onclick={() => handleCopy(results.warnings_path!)}
          >
            {copiedPath === results.warnings_path ? 'Copied!' : 'Copy Path'}
          </button>
        </div>
      {/if}
    </div>
  {:else if onGenerateResults && terminalPhase === 'completed'}
    <div class="generate-section">
      <p class="generate-hint">
        Generate merged result files to see success and failure records.
      </p>
      <button
        class="btn btn-generate"
        onclick={onGenerateResults}
        disabled={generating}
      >
        {generating ? 'Generating...' : 'Generate Merged Results'}
      </button>
    </div>
  {/if}

  <div class="result-footer">
    <div class="group-id">
      Group: <code>{groupId}</code>
    </div>
    <button class="btn btn-reset" onclick={onReset}>
      New Upload
    </button>
  </div>
</div>

<style>
  .result-panel {
    padding: 1.25rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
  }

  .result-header {
    margin-bottom: 1rem;
  }

  .status-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border-radius: 4px;
    font-size: 0.9rem;
    font-weight: 600;
  }

  .status-badge.status-success {
    background: rgba(76, 175, 80, 0.15);
    color: var(--success-color, #4caf50);
  }

  .status-badge.status-warning {
    background: rgba(255, 152, 0, 0.15);
    color: var(--warning-color, #ff9800);
  }

  .status-badge.status-error {
    background: rgba(244, 67, 54, 0.15);
    color: var(--error-color, #f44);
  }

  .status-icon {
    font-size: 1rem;
  }

  .terminal-message {
    font-size: 0.85rem;
    padding: 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
    margin-bottom: 1rem;
    border-left: 3px solid;
  }

  .terminal-message.status-success {
    border-color: var(--success-color, #4caf50);
    color: var(--text-primary, #fff);
  }

  .terminal-message.status-warning {
    border-color: var(--warning-color, #ff9800);
    color: var(--text-primary, #fff);
  }

  .terminal-message.status-error {
    border-color: var(--error-color, #f44);
    color: var(--error-color, #f44);
  }

  .result-files {
    margin-bottom: 1.25rem;
  }

  .files-title {
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--text-secondary, #888);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin: 0 0 0.75rem 0;
  }

  .file-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
    margin-bottom: 0.5rem;
  }

  .file-row:last-child {
    margin-bottom: 0;
  }

  .file-info {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    min-width: 0;
    flex: 1;
  }

  .file-label {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    flex-shrink: 0;
  }

  .success-label {
    color: var(--success-color, #4caf50);
  }

  .failure-label {
    color: var(--error-color, #f44);
  }

  .warning-label {
    color: var(--warning-color, #ff9800);
  }

  .file-name {
    font-family: monospace;
    font-size: 0.85rem;
    color: var(--text-primary, #fff);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .btn {
    padding: 0.4rem 0.8rem;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn-copy {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    color: var(--text-primary, #fff);
    min-width: 80px;
    flex-shrink: 0;
  }

  .btn-copy:hover {
    background: var(--bg-hover, #2a2a2a);
    border-color: var(--border-hover, #444);
  }

  .generate-section {
    padding: 1rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
    margin-bottom: 1.25rem;
    text-align: center;
  }

  .generate-hint {
    margin: 0 0 1rem 0;
    font-size: 0.85rem;
    color: var(--text-secondary, #888);
  }

  .btn-generate {
    background: var(--accent-color, #4a9eff);
    border: 1px solid var(--accent-color, #4a9eff);
    color: #fff;
    padding: 0.5rem 1.25rem;
    font-size: 0.875rem;
  }

  .btn-generate:hover:not(:disabled) {
    background: #3d8be6;
    border-color: #3d8be6;
  }

  .btn-generate:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .result-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-top: 1rem;
    border-top: 1px solid var(--border-color, #333);
  }

  .group-id {
    font-size: 0.7rem;
    color: var(--text-secondary, #888);
  }

  .group-id code {
    font-family: monospace;
    color: var(--text-primary, #fff);
    background: var(--bg-primary, #1a1a1a);
    padding: 0.15rem 0.35rem;
    border-radius: 3px;
  }

  .btn-reset {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--accent-color, #4a9eff);
    color: var(--accent-color, #4a9eff);
  }

  .btn-reset:hover {
    background: rgba(74, 158, 255, 0.1);
  }
</style>
