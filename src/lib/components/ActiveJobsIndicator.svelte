<script lang="ts">
  import { activeJobs, type ActiveJobsState } from '$lib/stores/activeJobs';

  let jobs: ActiveJobsState = $state({ bulkActive: 0, bulkMax: 3 });
  activeJobs.subscribe((value) => {
    jobs = value;
  });

  let hasActive = $derived(jobs.bulkActive > 0);
  let progressPercent = $derived(jobs.bulkMax > 0 ? (jobs.bulkActive / jobs.bulkMax) * 100 : 0);
</script>

{#if hasActive}
  <div class="jobs-indicator">
    <div class="jobs-header">
      <span class="jobs-icon">⏳</span>
      <span class="jobs-title">Active Jobs</span>
    </div>
    <div class="jobs-count">
      <span class="count">{jobs.bulkActive}</span>
      <span class="separator">/</span>
      <span class="max">{jobs.bulkMax}</span>
      <span class="label">bulk jobs</span>
    </div>
    <div class="jobs-progress">
      <div class="progress-bar" style="width: {progressPercent}%"></div>
    </div>
  </div>
{/if}

<style>
  .jobs-indicator {
    margin: 0.5rem;
    padding: 0.75rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
  }

  .jobs-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.5rem;
  }

  .jobs-icon {
    font-size: 0.9rem;
  }

  .jobs-title {
    font-size: 0.75rem;
    font-weight: 600;
    color: var(--text-secondary, #888);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .jobs-count {
    display: flex;
    align-items: baseline;
    gap: 0.25rem;
    margin-bottom: 0.5rem;
  }

  .count {
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--accent-color, #4a9eff);
  }

  .separator {
    color: var(--text-secondary, #888);
  }

  .max {
    font-size: 0.9rem;
    color: var(--text-secondary, #888);
  }

  .label {
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
    margin-left: 0.25rem;
  }

  .jobs-progress {
    height: 4px;
    background: var(--bg-primary, #252525);
    border-radius: 2px;
    overflow: hidden;
  }

  .progress-bar {
    height: 100%;
    background: var(--accent-color, #4a9eff);
    transition: width 0.3s ease;
  }
</style>
