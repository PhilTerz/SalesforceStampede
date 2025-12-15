<script lang="ts">
  import type { QueryStrategy } from '$lib/stores/queryState';
  import StrategySelect from './StrategySelect.svelte';

  type Props = {
    strategy: QueryStrategy;
    onStrategyChange: (strategy: QueryStrategy) => void;
    onRun: () => void;
    running: boolean;
    disabled: boolean;
  };

  let { strategy, onStrategyChange, onRun, running, disabled }: Props = $props();

  let isDisabled = $derived(running || disabled);
</script>

<div class="toolbar">
  <button
    class="btn btn-primary"
    onclick={onRun}
    disabled={isDisabled}
    aria-label="Run query"
  >
    {#if running}
      <span class="spinner"></span>
      Running...
    {:else}
      Run
    {/if}
  </button>

  <div class="toolbar-divider"></div>

  <div class="strategy-group">
    <label class="strategy-label" for="strategy-select">Strategy:</label>
    <StrategySelect
      value={strategy}
      onChange={onStrategyChange}
      disabled={isDisabled}
    />
  </div>

  <div class="toolbar-spacer"></div>

  <span class="keyboard-hint">
    Ctrl+Enter to run
  </span>
</div>

<style>
  .toolbar {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.75rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
  }

  .btn {
    padding: 0.5rem 1rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .btn-primary {
    background: var(--accent-color, #4a9eff);
    border: 1px solid var(--accent-color, #4a9eff);
    color: #fff;
    min-width: 100px;
    justify-content: center;
  }

  .btn-primary:hover:not(:disabled) {
    background: #3d8be6;
    border-color: #3d8be6;
  }

  .btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .spinner {
    display: inline-block;
    width: 14px;
    height: 14px;
    border: 2px solid rgba(255, 255, 255, 0.3);
    border-top-color: #fff;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  .toolbar-divider {
    width: 1px;
    height: 24px;
    background: var(--border-color, #333);
  }

  .strategy-group {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .strategy-label {
    font-size: 0.875rem;
    color: var(--text-secondary, #888);
  }

  .toolbar-spacer {
    flex: 1;
  }

  .keyboard-hint {
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
    padding: 0.25rem 0.5rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
  }
</style>
