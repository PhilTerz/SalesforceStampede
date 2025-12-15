/**
 * Org management Tauri command wrappers.
 * Provides a clean API for org operations with proper state synchronization.
 */

import {
  login as invokeLogin,
  logout as invokeLogout,
  listOrgs as invokeListOrgs,
  switchOrg as invokeSwitchOrg,
  getActiveOrg as invokeGetActiveOrg,
  type OrgInfo
} from './invoke';
import { currentOrg, type OrgSummary } from '$lib/stores/currentOrg';
import { orgsState, refreshOrgs } from '$lib/stores/orgs';

export type LoginEnvironment = 'production' | 'sandbox' | 'custom';

export type LoginRequest = {
  environment: LoginEnvironment;
  customDomain?: string;
};

/**
 * Converts OrgInfo from backend to OrgSummary for the currentOrg store.
 */
function toOrgSummary(org: OrgInfo): OrgSummary {
  return {
    orgId: org.id,
    name: org.name,
    instanceUrl: org.instanceUrl
  };
}

/**
 * Lists all connected orgs.
 */
export async function listOrgs(): Promise<OrgInfo[]> {
  return invokeListOrgs();
}

/**
 * Refreshes the org list and updates the orgsState store.
 */
export async function refreshOrgList(): Promise<OrgInfo[]> {
  return refreshOrgs(invokeListOrgs);
}

/**
 * Gets the currently active org from the backend.
 */
export async function getActiveOrg(): Promise<OrgInfo | null> {
  return invokeGetActiveOrg();
}

/**
 * Synchronizes the currentOrg store with backend truth.
 * Call this after any org-changing operation.
 */
export async function syncCurrentOrg(): Promise<void> {
  try {
    const activeOrg = await invokeGetActiveOrg();
    if (activeOrg) {
      currentOrg.set(toOrgSummary(activeOrg));
    } else {
      currentOrg.set(null);
    }
  } catch (e) {
    console.error('Failed to sync current org:', e);
    currentOrg.set(null);
  }
}

/**
 * Logs into a Salesforce org.
 * Opens browser for OAuth, waits for completion.
 *
 * @param request - Login configuration
 * @returns The logged-in org info
 */
export async function login(request: LoginRequest): Promise<OrgInfo> {
  // Convert request to backend format
  let loginType: string;
  if (request.environment === 'custom' && request.customDomain) {
    loginType = request.customDomain;
  } else if (request.environment === 'sandbox') {
    loginType = 'sandbox';
  } else {
    loginType = 'production';
  }

  const org = await invokeLogin(loginType);

  // Refresh org list and sync current org
  await refreshOrgList();
  currentOrg.set(toOrgSummary(org));

  return org;
}

/**
 * Switches to a different org.
 *
 * @param orgId - The org ID to switch to
 * @returns The switched org info
 */
export async function switchOrg(orgId: string): Promise<OrgInfo> {
  const org = await invokeSwitchOrg(orgId);

  // Update currentOrg from the returned org
  currentOrg.set(toOrgSummary(org));

  return org;
}

/**
 * Logs out from an org (removes it and its tokens).
 *
 * @param orgId - The org ID to remove
 */
export async function logout(orgId: string): Promise<void> {
  await invokeLogout(orgId);

  // Refresh org list
  await refreshOrgList();

  // Sync current org from backend (may have changed)
  await syncCurrentOrg();
}

/**
 * Alias for logout - removes an org.
 * The backend's logout command removes the org and tokens.
 */
export const removeOrg = logout;

/**
 * Initializes org state on app startup.
 * Loads org list and syncs current org.
 */
export async function initializeOrgs(): Promise<void> {
  await refreshOrgList();
  await syncCurrentOrg();
}
