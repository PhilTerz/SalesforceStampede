import { invoke as tauriInvoke } from '@tauri-apps/api/core';

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export type OrgInfo = {
  id: string;
  name: string;
  orgType: string;
  instanceUrl: string;
  username: string;
};

export type ExecuteQueryRequest = {
  soql: string;
  strategy?: 'auto' | 'rest' | 'bulk';
  maxRows?: number;
  exportPath?: string;
};

export type ExecuteQueryResult = {
  records: unknown[];
  totalSize: number;
  done: boolean;
  truncated: boolean;
  strategyUsed: string;
  durationMs: number;
  exportPath?: string;
};

export type SavedQueryInfo = {
  id: string;
  name: string;
  soql: string;
  createdAt: number;
};

export type QueryHistoryInfo = {
  id: string;
  soql: string;
  executedAt: number;
  rowCount?: number;
  strategy?: string;
  durationMs?: number;
  truncated: boolean;
  exportPath?: string;
};

export type BatchSize = 'extra_small' | 'small' | 'medium' | 'large';

export type BulkUploadRequest = {
  object: string;
  operation: 'insert' | 'update' | 'upsert' | 'delete';
  csv_path: string;
  batch_size: BatchSize;
  external_id_field_name?: string;
};

export type BulkUploadStarted = {
  group_id: string;
  total_parts: number;
};

// ─────────────────────────────────────────────────────────────────────────────
// Auth Commands
// ─────────────────────────────────────────────────────────────────────────────

export async function login(loginType: string): Promise<OrgInfo> {
  return tauriInvoke<OrgInfo>('login', { loginType });
}

export async function listOrgs(): Promise<OrgInfo[]> {
  return tauriInvoke<OrgInfo[]>('list_orgs');
}

export async function switchOrg(orgId: string): Promise<OrgInfo> {
  return tauriInvoke<OrgInfo>('switch_org', { orgId });
}

export async function getActiveOrg(): Promise<OrgInfo | null> {
  return tauriInvoke<OrgInfo | null>('get_active_org');
}

export async function logout(orgId: string): Promise<void> {
  return tauriInvoke<void>('logout', { orgId });
}

// ─────────────────────────────────────────────────────────────────────────────
// Query Commands
// ─────────────────────────────────────────────────────────────────────────────

export async function executeQuery(request: ExecuteQueryRequest): Promise<ExecuteQueryResult> {
  return tauriInvoke<ExecuteQueryResult>('execute_query', { request });
}

export async function saveQuery(name: string, soql: string): Promise<SavedQueryInfo> {
  return tauriInvoke<SavedQueryInfo>('save_query', { name, soql });
}

export async function getSavedQueries(): Promise<SavedQueryInfo[]> {
  return tauriInvoke<SavedQueryInfo[]>('get_saved_queries');
}

export async function deleteSavedQuery(queryId: string): Promise<void> {
  return tauriInvoke<void>('delete_saved_query', { queryId });
}

export async function getQueryHistory(limit?: number, offset?: number): Promise<QueryHistoryInfo[]> {
  return tauriInvoke<QueryHistoryInfo[]>('get_query_history', { limit, offset });
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk Commands
// ─────────────────────────────────────────────────────────────────────────────

export async function startBulkUpload(request: BulkUploadRequest): Promise<BulkUploadStarted> {
  return tauriInvoke<BulkUploadStarted>('start_bulk_upload', { request });
}

export async function cancelBulkUpload(groupId: string): Promise<void> {
  return tauriInvoke<void>('cancel_bulk_upload', { group_id: groupId });
}
