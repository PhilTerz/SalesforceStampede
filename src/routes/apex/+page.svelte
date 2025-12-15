<script lang="ts">
  import { currentOrg } from '$lib/stores/currentOrg';

  let org = $state<{ orgId: string; name: string; instanceUrl: string } | null>(null);
  currentOrg.subscribe((value) => {
    org = value;
  });
</script>

<div class="apex-page">
  <header class="page-header">
    <h1>Apex</h1>
    {#if org}
      <span class="org-badge">{org.name}</span>
    {/if}
  </header>

  <div class="apex-content">
    {#if !org}
      <div class="no-org-message">
        <p>Select an org to run Apex</p>
      </div>
    {:else}
      <div class="apex-editor">
        <p class="placeholder">Apex execution interface coming soon...</p>
      </div>
    {/if}
  </div>
</div>

<style>
  .apex-page {
    height: 100%;
    display: flex;
    flex-direction: column;
  }

  .page-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1.5rem;
  }

  .page-header h1 {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--text-primary);
  }

  .org-badge {
    padding: 0.25rem 0.75rem;
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 4px;
    font-size: 0.75rem;
    color: var(--text-secondary);
  }

  .apex-content {
    flex: 1;
    display: flex;
    flex-direction: column;
  }

  .no-org-message {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--text-secondary);
  }

  .apex-editor {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .placeholder {
    color: var(--text-secondary);
    font-style: italic;
  }
</style>
