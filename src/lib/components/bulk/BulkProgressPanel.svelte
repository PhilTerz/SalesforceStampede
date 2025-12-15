<script lang="ts">
  type Progress = {
    phase: string;
    currentPart: number;
    totalParts: number;
    activeJobs: number;
    maxJobs: number;
    message?: string;
  };

  type Props = {
    progress: Progress;
    groupId: string;
    onCancel?: () => void;
    cancelling?: boolean;
  };

  let { progress, groupId, onCancel, cancelling = false }: Props = $props();

  // Calculate percentage
  let percent = $derived(
    progress.totalParts > 0
      ? Math.round((progress.currentPart / progress.totalParts) * 100)
      : 0
  );

  // Format phase for display
  function formatPhase(phase: string): string {
    switch (phase) {
      case 'starting':
        return 'Starting...';
      case 'chunking':
        return 'Splitting CSV...';
      case 'uploading':
        return 'Uploading chunks...';
      case 'processing':
        return 'Processing...';
      case 'waiting':
        return 'Waiting for jobs...';
      case 'completed':
        return 'Completed';
      case 'cancelled':
        return 'Cancelled';
      case 'failed':
        return 'Failed';
      default:
        return phase;
    }
  }

  // Check if we're in a terminal state
  let isTerminal = $derived(
    ['completed', 'cancelled', 'failed'].includes(progress.phase)
  );

  // Determine status color class
  let statusClass = $derived.by(() => {
    switch (progress.phase) {
      case 'completed':
        return 'status-success';
      case 'cancelled':
        return 'status-warning';
      case 'failed':
        return 'status-error';
      default:
        return 'status-active';
    }
  });
</script>

<div class="progress-panel" class:terminal={isTerminal}>
  <div class="progress-header">
    <div class="phase-info">
      <span class="phase-badge {statusClass}">{formatPhase(progress.phase)}</span>
      {#if !isTerminal}
        <span class="concurrency">
          {progress.activeJobs} / {progress.maxJobs} jobs active
        </span>
      {/if}
    </div>

    {#if !isTerminal && onCancel}
      <button
        class="btn btn-cancel"
        onclick={onCancel}
        disabled={cancelling}
      >
        {cancelling ? 'Cancelling...' : 'Cancel'}
      </button>
    {/if}
  </div>

  <div class="progress-stats">
    <span class="part-count">
      Part {progress.currentPart} of {progress.totalParts}
    </span>
    <span class="percent">{percent}%</span>
  </div>

  <div class="progress-bar-container">
    <div
      class="progress-bar {statusClass}"
      style="width: {percent}%"
    ></div>
  </div>

  {#if progress.message}
    <div class="progress-message">
      {progress.message}
    </div>
  {/if}

  <div class="group-id">
    Group: <code>{groupId}</code>
  </div>
</div>

<style>
  .progress-panel {
    padding: 1.25rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
  }

  .progress-panel.terminal {
    border-color: var(--border-color, #333);
  }

  .progress-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1rem;
  }

  .phase-info {
    display: flex;
    align-items: center;
    gap: 1rem;
  }

  .phase-badge {
    padding: 0.35rem 0.75rem;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 600;
  }

  .status-active {
    background: rgba(74, 158, 255, 0.15);
    color: var(--accent-color, #4a9eff);
  }

  .status-success {
    background: rgba(76, 175, 80, 0.15);
    color: var(--success-color, #4caf50);
  }

  .status-warning {
    background: rgba(255, 152, 0, 0.15);
    color: var(--warning-color, #ff9800);
  }

  .status-error {
    background: rgba(244, 67, 54, 0.15);
    color: var(--error-color, #f44);
  }

  .concurrency {
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
  }

  .btn {
    padding: 0.4rem 0.8rem;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn-cancel {
    background: transparent;
    border: 1px solid var(--error-color, #f44);
    color: var(--error-color, #f44);
  }

  .btn-cancel:hover:not(:disabled) {
    background: rgba(244, 67, 54, 0.1);
  }

  .btn-cancel:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .progress-stats {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
  }

  .part-count {
    font-size: 0.875rem;
    color: var(--text-primary, #fff);
    font-weight: 500;
  }

  .percent {
    font-size: 0.875rem;
    color: var(--text-secondary, #888);
  }

  .progress-bar-container {
    height: 6px;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 3px;
    overflow: hidden;
    margin-bottom: 1rem;
  }

  .progress-bar {
    height: 100%;
    transition: width 0.3s ease;
    border-radius: 3px;
  }

  .progress-bar.status-active {
    background: var(--accent-color, #4a9eff);
  }

  .progress-bar.status-success {
    background: var(--success-color, #4caf50);
  }

  .progress-bar.status-warning {
    background: var(--warning-color, #ff9800);
  }

  .progress-bar.status-error {
    background: var(--error-color, #f44);
  }

  .progress-message {
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
    margin-bottom: 0.75rem;
    padding: 0.5rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
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
</style>
