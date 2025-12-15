<script lang="ts">
  import type { QueryProgress } from '$lib/stores/queryState';

  type Props = {
    progress: QueryProgress | null;
  };

  let { progress }: Props = $props();

  function formatPhase(phase: string): string {
    switch (phase) {
      case 'starting':
        return 'Starting...';
      case 'counting':
        return 'Counting records...';
      case 'fetching':
        return 'Fetching records...';
      case 'creating_job':
        return 'Creating bulk job...';
      case 'polling':
        return 'Waiting for bulk job...';
      case 'downloading':
        return 'Downloading results...';
      case 'complete':
        return 'Complete';
      default:
        return phase;
    }
  }
</script>

{#if progress}
  <div class="progress-container">
    <div class="progress-info">
      <span class="progress-phase">{formatPhase(progress.phase)}</span>
      <span class="progress-count">
        {progress.recordsFetched.toLocaleString()} fetched
        {#if progress.totalExpected}
          / {progress.totalExpected.toLocaleString()} total
        {/if}
      </span>
    </div>

    {#if progress.totalExpected && progress.totalExpected > 0}
      <div class="progress-bar-container">
        <div
          class="progress-bar"
          style="width: {Math.min((progress.recordsFetched / progress.totalExpected) * 100, 100)}%"
        ></div>
      </div>
    {:else}
      <div class="progress-bar-container">
        <div class="progress-bar indeterminate"></div>
      </div>
    {/if}
  </div>
{/if}

<style>
  .progress-container {
    padding: 1rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
  }

  .progress-info {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.75rem;
  }

  .progress-phase {
    font-size: 0.875rem;
    color: var(--text-primary, #fff);
    font-weight: 500;
  }

  .progress-count {
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
  }

  .progress-bar-container {
    height: 4px;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 2px;
    overflow: hidden;
  }

  .progress-bar {
    height: 100%;
    background: var(--accent-color, #4a9eff);
    transition: width 0.3s ease;
  }

  .progress-bar.indeterminate {
    width: 30%;
    animation: indeterminate 1.5s infinite linear;
  }

  @keyframes indeterminate {
    0% {
      transform: translateX(-100%);
    }
    100% {
      transform: translateX(400%);
    }
  }
</style>
