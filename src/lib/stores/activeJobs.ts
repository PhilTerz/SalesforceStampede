import { writable } from 'svelte/store';

export type ActiveJobsState = {
  bulkActive: number;
  bulkMax: number;
};

export const activeJobs = writable<ActiveJobsState>({
  bulkActive: 0,
  bulkMax: 3
});
