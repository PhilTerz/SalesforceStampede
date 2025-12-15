<script lang="ts">
  import OrgSwitcher from './OrgSwitcher.svelte';
  import NavigationMenu from './NavigationMenu.svelte';
  import ActiveJobsIndicator from './ActiveJobsIndicator.svelte';
</script>

<aside class="sidebar">
  <div class="sidebar-header">
    <h1 class="app-title">Stampede</h1>
  </div>

  <OrgSwitcher />

  <div class="sidebar-divider"></div>

  <NavigationMenu />

  <div class="sidebar-spacer"></div>

  <ActiveJobsIndicator />
</aside>

<style>
  .sidebar {
    width: 240px;
    height: 100vh;
    background: var(--sidebar-bg, #171717);
    border-right: 1px solid var(--border-color, #333);
    display: flex;
    flex-direction: column;
    flex-shrink: 0;
  }

  .sidebar-header {
    padding: 1rem;
    border-bottom: 1px solid var(--border-color, #333);
  }

  .app-title {
    margin: 0;
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--text-primary, #fff);
    letter-spacing: -0.02em;
  }

  .sidebar-divider {
    height: 1px;
    background: var(--border-color, #333);
    margin: 0.5rem 0;
  }

  .sidebar-spacer {
    flex: 1;
  }
</style>
