<script lang="ts">
  import { page } from '$app/stores';

  type NavItem = {
    label: string;
    href: string;
    icon: string;
  };

  const navItems: NavItem[] = [
    { label: 'Query', href: '/query', icon: '🔍' },
    { label: 'Bulk', href: '/bulk', icon: '📦' },
    { label: 'Apex', href: '/apex', icon: '⚡' },
    { label: 'Settings', href: '/settings', icon: '⚙️' }
  ];

  let currentPath = $state('/');
  page.subscribe((p) => {
    currentPath = p.url.pathname;
  });
</script>

<nav class="nav-menu">
  {#each navItems as item}
    <a href={item.href} class="nav-item" class:active={currentPath === item.href}>
      <span class="nav-icon">{item.icon}</span>
      <span class="nav-label">{item.label}</span>
    </a>
  {/each}
</nav>

<style>
  .nav-menu {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    padding: 0.5rem;
  }

  .nav-item {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.75rem;
    border-radius: 4px;
    color: var(--text-secondary, #888);
    text-decoration: none;
    transition: background 0.15s, color 0.15s;
  }

  .nav-item:hover {
    background: var(--bg-hover, #2a2a2a);
    color: var(--text-primary, #fff);
  }

  .nav-item.active {
    background: var(--bg-active, #333);
    color: var(--text-primary, #fff);
  }

  .nav-icon {
    font-size: 1.1rem;
  }

  .nav-label {
    font-size: 0.9rem;
    font-weight: 500;
  }
</style>
