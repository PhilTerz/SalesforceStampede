import { writable } from 'svelte/store';

export type OrgSummary = {
  orgId: string;
  name: string;
  instanceUrl: string;
};

export const currentOrg = writable<OrgSummary | null>(null);
