<script lang="ts">
  import { currentOrg, type OrgSummary } from '$lib/stores/currentOrg';
  import { orgsState } from '$lib/stores/orgs';
  import { switchOrg, removeOrg, refreshOrgList } from '$lib/tauri/orgs';
  import type { OrgInfo } from '$lib/tauri/invoke';

  let current: OrgSummary | null = $state(null);
  let orgs: OrgInfo[] = $state([]);
  let loading = $state(false);
  let error = $state<string | undefined>(undefined);

  let switchingOrgId = $state<string | null>(null);
  let removingOrgId = $state<string | null>(null);
  let confirmRemoveOrgId = $state<string | null>(null);

  currentOrg.subscribe((value) => {
    current = value;
  });

  orgsState.subscribe((state) => {
    orgs = state.orgs;
    loading = state.loading;
    error = state.error;
  });

  async function handleSwitch(orgId: string) {
    if (switchingOrgId || removingOrgId) return;

    switchingOrgId = orgId;
    try {
      await switchOrg(orgId);
    } catch (e) {
      console.error('Failed to switch org:', e);
    } finally {
      switchingOrgId = null;
    }
  }

  async function handleRemove(orgId: string) {
    if (switchingOrgId || removingOrgId) return;

    removingOrgId = orgId;
    confirmRemoveOrgId = null;

    try {
      await removeOrg(orgId);
    } catch (e) {
      console.error('Failed to remove org:', e);
    } finally {
      removingOrgId = null;
    }
  }

  function handleConfirmRemove(orgId: string) {
    confirmRemoveOrgId = orgId;
  }

  function handleCancelRemove() {
    confirmRemoveOrgId = null;
  }

  async function handleRetry() {
    await refreshOrgList();
  }

  function isActive(org: OrgInfo): boolean {
    return current?.orgId === org.id;
  }
</script>

<div class="org-list">
  {#if error}
    <div class="error-banner">
      <p>{error}</p>
      <button class="btn btn-small" onclick={handleRetry}>Retry</button>
    </div>
  {/if}

  {#if loading && orgs.length === 0}
    <div class="loading-state">
      <span class="loading-spinner"></span>
      <p>Loading orgs...</p>
    </div>
  {:else if orgs.length === 0}
    <div class="empty-state">
      <p>No connected orgs</p>
      <p class="empty-hint">Click "Add Org" to connect a Salesforce org.</p>
    </div>
  {:else}
    <ul class="org-items">
      {#each orgs as org (org.id)}
        {@const active = isActive(org)}
        {@const isConfirming = confirmRemoveOrgId === org.id}
        {@const isSwitching = switchingOrgId === org.id}
        {@const isRemoving = removingOrgId === org.id}
        {@const isDisabled = switchingOrgId !== null || removingOrgId !== null}

        <li class="org-item" class:active>
          <div class="org-info">
            <div class="org-name">
              {org.name}
              {#if active}
                <span class="active-badge">Active</span>
              {/if}
            </div>
            <div class="org-details">
              <span class="org-type">{org.orgType}</span>
              <span class="org-url">{org.instanceUrl}</span>
            </div>
            <div class="org-username">{org.username}</div>
          </div>

          <div class="org-actions">
            {#if isConfirming}
              <div class="confirm-remove">
                <span class="confirm-text">Remove?</span>
                <button
                  class="btn btn-small btn-danger"
                  onclick={() => handleRemove(org.id)}
                  disabled={isDisabled}
                >
                  Yes
                </button>
                <button
                  class="btn btn-small"
                  onclick={handleCancelRemove}
                  disabled={isDisabled}
                >
                  No
                </button>
              </div>
            {:else}
              {#if !active}
                <button
                  class="btn btn-small"
                  onclick={() => handleSwitch(org.id)}
                  disabled={isDisabled}
                >
                  {isSwitching ? 'Switching...' : 'Switch'}
                </button>
              {/if}
              <button
                class="btn btn-small btn-danger-outline"
                onclick={() => handleConfirmRemove(org.id)}
                disabled={isDisabled}
              >
                {isRemoving ? 'Removing...' : 'Remove'}
              </button>
            {/if}
          </div>
        </li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  .org-list {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .error-banner {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.75rem;
    background: rgba(244, 67, 54, 0.1);
    border: 1px solid var(--error-color, #f44);
    border-radius: 4px;
  }

  .error-banner p {
    margin: 0;
    color: var(--error-color, #f44);
    font-size: 0.875rem;
  }

  .loading-state,
  .empty-state {
    text-align: center;
    padding: 2rem;
    color: var(--text-secondary, #888);
  }

  .loading-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.75rem;
  }

  .loading-spinner {
    display: inline-block;
    width: 24px;
    height: 24px;
    border: 2px solid var(--border-color, #333);
    border-top-color: var(--accent-color, #4a9eff);
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  .empty-state p {
    margin: 0;
  }

  .empty-hint {
    margin-top: 0.5rem !important;
    font-size: 0.875rem;
  }

  .org-items {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .org-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 1rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
  }

  .org-item.active {
    border-color: var(--accent-color, #4a9eff);
  }

  .org-info {
    flex: 1;
    min-width: 0;
  }

  .org-name {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
  }

  .active-badge {
    padding: 0.125rem 0.5rem;
    background: var(--accent-color, #4a9eff);
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 500;
    color: #fff;
    text-transform: uppercase;
  }

  .org-details {
    display: flex;
    gap: 0.75rem;
    margin-top: 0.25rem;
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
  }

  .org-type {
    padding: 0.125rem 0.375rem;
    background: var(--bg-secondary, #1e1e1e);
    border-radius: 3px;
  }

  .org-url {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .org-username {
    margin-top: 0.25rem;
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .org-actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-shrink: 0;
  }

  .confirm-remove {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .confirm-text {
    font-size: 0.8rem;
    color: var(--text-secondary, #888);
  }

  .btn {
    padding: 0.5rem 0.75rem;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    color: var(--text-primary, #fff);
  }

  .btn:hover:not(:disabled) {
    background: var(--bg-hover, #2a2a2a);
  }

  .btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .btn-small {
    padding: 0.375rem 0.625rem;
    font-size: 0.75rem;
  }

  .btn-danger {
    background: var(--error-color, #f44);
    border-color: var(--error-color, #f44);
    color: #fff;
  }

  .btn-danger:hover:not(:disabled) {
    background: #d32f2f;
    border-color: #d32f2f;
  }

  .btn-danger-outline {
    background: transparent;
    border-color: var(--error-color, #f44);
    color: var(--error-color, #f44);
  }

  .btn-danger-outline:hover:not(:disabled) {
    background: rgba(244, 67, 54, 0.1);
  }
</style>
