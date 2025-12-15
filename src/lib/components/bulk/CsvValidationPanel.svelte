<script lang="ts">
  import type { CsvValidationResult } from '$lib/stores/bulkState';

  type Props = {
    validation: CsvValidationResult;
    validating?: boolean;
  };

  let { validation, validating = false }: Props = $props();

  let hasErrors = $derived(validation.errors.length > 0);
  let hasWarnings = $derived(validation.warnings.length > 0);

  function formatBytes(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  }

  function formatLineEndings(endings: string): string {
    switch (endings) {
      case 'lf':
        return 'Unix (LF)';
      case 'crlf':
        return 'Windows (CRLF)';
      case 'mixed':
        return 'Mixed';
      default:
        return endings;
    }
  }
</script>

<div class="validation-panel" class:has-errors={hasErrors}>
  {#if validating}
    <div class="validating">
      <span class="spinner"></span>
      <span>Validating CSV...</span>
    </div>
  {:else}
    <!-- Errors -->
    {#if hasErrors}
      <div class="section errors-section">
        <h4 class="section-title error-title">
          <span class="icon error-icon">!</span>
          {validation.errors.length} Error{validation.errors.length !== 1 ? 's' : ''} - Cannot Start Upload
        </h4>
        <ul class="error-list">
          {#each validation.errors as error}
            <li class="error-item">
              <span class="code">[{error.code}]</span>
              <span class="message">{error.message}</span>
            </li>
          {/each}
        </ul>
      </div>
    {:else}
      <div class="section success-section">
        <h4 class="section-title success-title">
          <span class="icon success-icon">&#10003;</span>
          Validation Passed
        </h4>
      </div>
    {/if}

    <!-- Warnings -->
    {#if hasWarnings}
      <div class="section warnings-section">
        <h4 class="section-title warning-title">
          <span class="icon warning-icon">&#9888;</span>
          {validation.warnings.length} Warning{validation.warnings.length !== 1 ? 's' : ''}
        </h4>
        <ul class="warning-list">
          {#each validation.warnings as warning}
            <li class="warning-item">
              <span class="code">[{warning.code}]</span>
              <span class="message">{warning.message}</span>
            </li>
          {/each}
        </ul>
      </div>
    {/if}

    <!-- Stats -->
    {#if validation.stats}
      <div class="section stats-section">
        <h4 class="section-title">File Information</h4>
        <div class="stats-grid">
          <div class="stat">
            <span class="stat-label">File Size</span>
            <span class="stat-value">{formatBytes(validation.stats.file_size)}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Sample Read</span>
            <span class="stat-value">{formatBytes(validation.stats.sample_bytes)}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Columns</span>
            <span class="stat-value">{validation.stats.inferred_columns}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Line Endings</span>
            <span class="stat-value">{formatLineEndings(validation.stats.line_endings)}</span>
          </div>
          {#if validation.stats.estimated_rows !== null}
            <div class="stat">
              <span class="stat-label">Estimated Rows</span>
              <span class="stat-value">{validation.stats.estimated_rows.toLocaleString()}</span>
            </div>
          {/if}
        </div>

        <!-- Headers preview -->
        {#if validation.stats.headers.length > 0}
          <div class="headers-section">
            <span class="headers-label">Headers:</span>
            <div class="headers-list">
              {#each validation.stats.headers.slice(0, 10) as header}
                <span class="header-chip">{header}</span>
              {/each}
              {#if validation.stats.headers.length > 10}
                <span class="header-chip more">+{validation.stats.headers.length - 10} more</span>
              {/if}
            </div>
          </div>
        {/if}
      </div>
    {/if}
  {/if}
</div>

<style>
  .validation-panel {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
    padding: 1rem;
  }

  .validation-panel.has-errors {
    border-color: var(--error-color, #f44);
  }

  .validating {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    color: var(--text-secondary, #888);
    padding: 0.5rem 0;
  }

  .spinner {
    width: 16px;
    height: 16px;
    border: 2px solid var(--border-color, #333);
    border-top-color: var(--accent-color, #4a9eff);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  .section {
    margin-bottom: 1rem;
  }

  .section:last-child {
    margin-bottom: 0;
  }

  .section-title {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.875rem;
    font-weight: 600;
    margin: 0 0 0.75rem 0;
    color: var(--text-primary, #fff);
  }

  .icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    border-radius: 50%;
    font-size: 0.75rem;
    font-weight: 700;
    color: #fff;
  }

  .error-icon {
    background: var(--error-color, #f44);
  }

  .warning-icon {
    background: var(--warning-color, #ff9800);
    font-size: 0.85rem;
  }

  .success-icon {
    background: var(--success-color, #4caf50);
    font-size: 0.85rem;
  }

  .error-title {
    color: var(--error-color, #f44);
  }

  .warning-title {
    color: var(--warning-color, #ff9800);
  }

  .success-title {
    color: var(--success-color, #4caf50);
  }

  .error-list,
  .warning-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .error-item,
  .warning-item {
    display: flex;
    gap: 0.5rem;
    font-size: 0.8rem;
    padding: 0.5rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
  }

  .error-item {
    border-left: 3px solid var(--error-color, #f44);
  }

  .warning-item {
    border-left: 3px solid var(--warning-color, #ff9800);
  }

  .code {
    color: var(--text-secondary, #888);
    font-family: monospace;
    flex-shrink: 0;
  }

  .message {
    color: var(--text-primary, #fff);
    word-break: break-word;
  }

  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1rem;
  }

  .stat {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    padding: 0.5rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
  }

  .stat-label {
    font-size: 0.7rem;
    color: var(--text-secondary, #888);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .stat-value {
    font-size: 0.875rem;
    color: var(--text-primary, #fff);
    font-weight: 500;
  }

  .headers-section {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .headers-label {
    font-size: 0.7rem;
    color: var(--text-secondary, #888);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .headers-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .header-chip {
    padding: 0.25rem 0.5rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    font-size: 0.75rem;
    color: var(--text-primary, #fff);
    font-family: monospace;
  }

  .header-chip.more {
    color: var(--text-secondary, #888);
    font-family: inherit;
  }
</style>
