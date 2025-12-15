<script lang="ts">
  import Sidebar from '$lib/components/Sidebar.svelte';
  import { activeJobs } from '$lib/stores/activeJobs';
  import { initializeOrgs } from '$lib/tauri/orgs';
  import { onBulkProgress } from '$lib/tauri/events';
  import { onMount } from 'svelte';

  let { children } = $props();

  onMount(async () => {
    // Initialize org state (loads org list and syncs current org)
    try {
      await initializeOrgs();
    } catch (e) {
      console.error('Failed to initialize orgs:', e);
    }

    // Subscribe to bulk progress events
    const unlisten = await onBulkProgress((event) => {
      activeJobs.update((state) => ({
        ...state,
        bulkActive: event.active_jobs,
        bulkMax: event.max_jobs
      }));
    });

    return () => {
      unlisten();
    };
  });
</script>

<div class="app-layout">
  <Sidebar />
  <main class="main-content">
    {@render children()}
  </main>
</div>

<style>
  :global(*) {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }

  :global(html, body) {
    height: 100%;
    overflow: hidden;
  }

  :global(body) {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    font-size: 14px;
    line-height: 1.5;
    color: var(--text-primary);
    background: var(--bg-primary);
  }

  :global(:root) {
    --bg-primary: #1a1a1a;
    --bg-secondary: #1e1e1e;
    --bg-hover: #2a2a2a;
    --bg-active: #333;
    --sidebar-bg: #171717;
    --border-color: #333;
    --text-primary: #f0f0f0;
    --text-secondary: #888;
    --accent-color: #4a9eff;
    --error-color: #f44;
    --success-color: #4caf50;
    --warning-color: #ff9800;
  }

  .app-layout {
    display: flex;
    height: 100vh;
    overflow: hidden;
  }

  .main-content {
    flex: 1;
    overflow: auto;
    padding: 1.5rem;
  }
</style>
