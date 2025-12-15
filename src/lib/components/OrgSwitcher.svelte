<script lang="ts">
  import { onMount } from 'svelte';
  import { currentOrg, type OrgSummary } from '$lib/stores/currentOrg';
  import { orgsState } from '$lib/stores/orgs';
  import { switchOrg, refreshOrgList, logout } from '$lib/tauri/orgs';
  import type { OrgInfo } from '$lib/tauri/invoke';
  import LoginModal from './LoginModal.svelte';

  let current: OrgSummary | null = $state(null);
  let orgs: OrgInfo[] = $state([]);
  let loading = $state(false);
  let error = $state<string | undefined>(undefined);

  let isSwitching = $state(false);
  let isLoggingOut = $state(false);
  let showLoginModal = $state(false);

  currentOrg.subscribe((value) => {
    current = value;
  });

  orgsState.subscribe((state) => {
    orgs = state.orgs;
    loading = state.loading;
    error = state.error;
  });

  // Refresh org list on mount
  onMount(() => {
    refreshOrgList();
  });

  async function handleOrgChange(e: Event) {
    const select = e.target as HTMLSelectElement;
    const orgId = select.value;

    if (!orgId || orgId === current?.orgId) return;

    isSwitching = true;
    try {
      await switchOrg(orgId);
    } catch (e) {
      console.error('Failed to switch org:', e);
      // Reset select to current org
      if (current) {
        select.value = current.orgId;
      }
    } finally {
      isSwitching = false;
    }
  }

  async function handleLogout() {
    if (!current || isLoggingOut || isSwitching) return;

    isLoggingOut = true;
    try {
      await logout(current.orgId);
    } catch (e) {
      console.error('Failed to logout:', e);
    } finally {
      isLoggingOut = false;
    }
  }

  function handleAddOrg() {
    showLoginModal = true;
  }

  function handleModalClose() {
    showLoginModal = false;
  }

  async function handleLoginSuccess() {
    // Refresh org list after successful login
    await refreshOrgList();
  }

  let isDisabled = $derived(loading || isSwitching || isLoggingOut);
</script>

<div class="org-switcher">
  <div class="switcher-row">
    <select
      class="org-select"
      value={current?.orgId ?? ''}
      onchange={handleOrgChange}
      disabled={isDisabled}
    >
      {#if !current}
        <option value="" disabled>No org selected</option>
      {/if}
      {#each orgs as org (org.id)}
        <option value={org.id}>
          {org.name} ({org.orgType})
        </option>
      {/each}
      {#if orgs.length === 0 && !loading}
        <option value="" disabled>No orgs available</option>
      {/if}
    </select>

    <button
      class="btn btn-icon"
      onclick={handleAddOrg}
      disabled={isDisabled}
      title="Add org"
      aria-label="Add new organization"
    >
      +
    </button>

    {#if current}
      <button
        class="btn btn-icon btn-danger-icon"
        onclick={handleLogout}
        disabled={isDisabled}
        title="Logout from current org"
        aria-label="Logout from current organization"
      >
        {isLoggingOut ? '...' : '×'}
      </button>
    {/if}
  </div>

  {#if current}
    <div class="current-org-info">
      <span class="instance-url" title={current.instanceUrl}>
        {current.instanceUrl}
      </span>
    </div>
  {/if}

  {#if error}
    <div class="error-text">{error}</div>
  {/if}

  {#if isSwitching}
    <div class="status-text">Switching...</div>
  {/if}
</div>

<LoginModal
  isOpen={showLoginModal}
  onClose={handleModalClose}
  onSuccess={handleLoginSuccess}
/>

<style>
  .org-switcher {
    padding: 0.75rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .switcher-row {
    display: flex;
    gap: 0.375rem;
  }

  .org-select {
    flex: 1;
    min-width: 0;
    padding: 0.5rem 0.625rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.8rem;
    cursor: pointer;
  }

  .org-select:hover:not(:disabled) {
    border-color: var(--accent-color, #4a9eff);
  }

  .org-select:focus {
    outline: none;
    border-color: var(--accent-color, #4a9eff);
  }

  .org-select:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .org-select option {
    background: var(--bg-secondary, #1e1e1e);
    color: var(--text-primary, #fff);
  }

  .btn {
    padding: 0.5rem 0.75rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn:hover:not(:disabled) {
    background: var(--bg-hover, #2a2a2a);
    border-color: var(--accent-color, #4a9eff);
  }

  .btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .btn-icon {
    padding: 0.5rem;
    width: 32px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 600;
  }

  .btn-danger-icon:hover:not(:disabled) {
    border-color: var(--error-color, #f44);
    color: var(--error-color, #f44);
  }

  .current-org-info {
    font-size: 0.7rem;
    color: var(--text-secondary, #888);
    overflow: hidden;
  }

  .instance-url {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .error-text {
    font-size: 0.7rem;
    color: var(--error-color, #f44);
  }

  .status-text {
    font-size: 0.7rem;
    color: var(--text-secondary, #888);
  }
</style>
