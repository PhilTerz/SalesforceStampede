<script lang="ts">
  import { onMount } from 'svelte';
  import { currentOrg, type OrgSummary } from '$lib/stores/currentOrg';
  import OrgList from '$lib/components/OrgList.svelte';
  import LoginModal from '$lib/components/LoginModal.svelte';
  import { refreshOrgList } from '$lib/tauri/orgs';

  let current: OrgSummary | null = $state(null);
  let showLoginModal = $state(false);

  currentOrg.subscribe((value) => {
    current = value;
  });

  // Load org list on mount
  onMount(() => {
    refreshOrgList();
  });

  function handleAddOrg() {
    showLoginModal = true;
  }

  function handleModalClose() {
    showLoginModal = false;
  }

  async function handleLoginSuccess() {
    await refreshOrgList();
  }
</script>

<div class="settings-page">
  <header class="page-header">
    <h1>Settings</h1>
  </header>

  <div class="settings-content">
    <section class="settings-section">
      <div class="section-header">
        <h2>Connected Orgs</h2>
        <button class="btn btn-primary" onclick={handleAddOrg}>
          Add Org
        </button>
      </div>
      <OrgList />
    </section>

    <section class="settings-section">
      <h2>Current Session</h2>
      {#if current}
        <div class="session-info">
          <div class="info-row">
            <span class="label">Org Name:</span>
            <span class="value">{current.name}</span>
          </div>
          <div class="info-row">
            <span class="label">Instance URL:</span>
            <span class="value">{current.instanceUrl}</span>
          </div>
          <div class="info-row">
            <span class="label">Org ID:</span>
            <span class="value monospace">{current.orgId}</span>
          </div>
        </div>
      {:else}
        <p class="no-session">No org selected. Use the org switcher in the sidebar or add an org above.</p>
      {/if}
    </section>

    <section class="settings-section">
      <h2>Application</h2>
      <p class="placeholder">Additional application settings coming soon...</p>
    </section>
  </div>
</div>

<LoginModal
  isOpen={showLoginModal}
  onClose={handleModalClose}
  onSuccess={handleLoginSuccess}
/>

<style>
  .settings-page {
    height: 100%;
    display: flex;
    flex-direction: column;
  }

  .page-header {
    margin-bottom: 1.5rem;
  }

  .page-header h1 {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
  }

  .settings-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 1.5rem;
    max-width: 800px;
  }

  .settings-section {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
    padding: 1.25rem;
  }

  .section-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1rem;
    padding-bottom: 0.75rem;
    border-bottom: 1px solid var(--border-color, #333);
  }

  .settings-section h2 {
    font-size: 1rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
    margin: 0;
  }

  .settings-section > h2 {
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border-color, #333);
  }

  .btn {
    padding: 0.5rem 1rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn-primary {
    background: var(--accent-color, #4a9eff);
    border: 1px solid var(--accent-color, #4a9eff);
    color: #fff;
  }

  .btn-primary:hover {
    background: #3d8be6;
    border-color: #3d8be6;
  }

  .session-info {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .info-row {
    display: flex;
    gap: 1rem;
  }

  .label {
    color: var(--text-secondary, #888);
    min-width: 100px;
    flex-shrink: 0;
  }

  .value {
    color: var(--text-primary, #fff);
    word-break: break-all;
  }

  .monospace {
    font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
    font-size: 0.85rem;
  }

  .no-session {
    color: var(--text-secondary, #888);
    margin: 0;
  }

  .placeholder {
    color: var(--text-secondary, #888);
    font-style: italic;
    margin: 0;
  }
</style>
