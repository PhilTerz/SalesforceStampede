import { writable, get } from 'svelte/store';
import type { OrgInfo } from '$lib/tauri/invoke';

export type OrgsState = {
  loading: boolean;
  orgs: OrgInfo[];
  error?: string;
};

export const orgsState = writable<OrgsState>({
  loading: false,
  orgs: [],
  error: undefined
});

// Refresh lock to prevent concurrent refreshes
let refreshInProgress = false;

/**
 * Refreshes the org list from the backend.
 * Serializes calls to prevent concurrent refreshes.
 */
export async function refreshOrgs(
  listOrgsFn: () => Promise<OrgInfo[]>
): Promise<OrgInfo[]> {
  // If already refreshing, wait and return current state
  if (refreshInProgress) {
    return get(orgsState).orgs;
  }

  refreshInProgress = true;
  orgsState.update((s) => ({ ...s, loading: true, error: undefined }));

  try {
    const orgs = await listOrgsFn();
    orgsState.set({ loading: false, orgs, error: undefined });
    return orgs;
  } catch (e) {
    const error = e instanceof Error ? e.message : 'Failed to load orgs';
    orgsState.update((s) => ({ ...s, loading: false, error }));
    return [];
  } finally {
    refreshInProgress = false;
  }
}

/**
 * Clears any error in the orgs state.
 */
export function clearOrgsError(): void {
  orgsState.update((s) => ({ ...s, error: undefined }));
}
