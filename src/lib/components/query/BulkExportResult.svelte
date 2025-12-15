<script lang="ts">
  import { copyToClipboard } from '$lib/tauri/query';

  type Props = {
    exportPath: string;
  };

  let { exportPath }: Props = $props();

  let copied = $state(false);

  async function handleCopy() {
    try {
      await copyToClipboard(exportPath);
      copied = true;
      setTimeout(() => {
        copied = false;
      }, 2000);
    } catch (e) {
      console.error('Failed to copy to clipboard:', e);
    }
  }
</script>

<div class="export-result">
  <div class="export-header">
    <span class="success-icon">&#10003;</span>
    <span class="success-text">Bulk Export Complete</span>
  </div>

  <div class="export-body">
    <span class="export-label">Export Path:</span>
    <div class="export-path-row">
      <code class="export-path">{exportPath}</code>
      <button
        class="btn btn-copy"
        onclick={handleCopy}
        aria-label="Copy path to clipboard"
      >
        {copied ? 'Copied!' : 'Copy'}
      </button>
    </div>
  </div>

  <p class="export-hint">
    The results have been saved to the file above. You can open it with any CSV viewer or spreadsheet application.
  </p>
</div>

<style>
  .export-result {
    padding: 1.5rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--success-color, #4caf50);
    border-radius: 8px;
  }

  .export-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 1.25rem;
  }

  .success-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    background: var(--success-color, #4caf50);
    color: #fff;
    border-radius: 50%;
    font-size: 1rem;
    font-weight: 700;
  }

  .success-text {
    font-size: 1.1rem;
    font-weight: 600;
    color: var(--success-color, #4caf50);
  }

  .export-body {
    margin-bottom: 1rem;
  }

  .export-label {
    display: block;
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
    margin-bottom: 0.5rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .export-path-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .export-path {
    flex: 1;
    padding: 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
    font-size: 0.85rem;
    color: var(--text-primary, #fff);
    word-break: break-all;
  }

  .btn {
    padding: 0.5rem 1rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn-copy {
    background: var(--accent-color, #4a9eff);
    border: 1px solid var(--accent-color, #4a9eff);
    color: #fff;
    min-width: 80px;
  }

  .btn-copy:hover {
    background: #3d8be6;
    border-color: #3d8be6;
  }

  .export-hint {
    margin: 0;
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
    line-height: 1.4;
  }
</style>
