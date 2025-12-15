<script lang="ts">
  import type { QueryStrategy } from '$lib/stores/queryState';

  type Props = {
    value: QueryStrategy;
    onChange: (value: QueryStrategy) => void;
    disabled?: boolean;
  };

  let { value, onChange, disabled = false }: Props = $props();

  function handleChange(e: Event) {
    const select = e.target as HTMLSelectElement;
    onChange(select.value as QueryStrategy);
  }
</script>

<select
  class="strategy-select"
  {value}
  onchange={handleChange}
  {disabled}
  aria-label="Query strategy"
>
  <option value="auto">Auto</option>
  <option value="rest">REST</option>
  <option value="bulk">Bulk</option>
</select>

<style>
  .strategy-select {
    padding: 0.5rem 0.75rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
    cursor: pointer;
    min-width: 100px;
  }

  .strategy-select:hover:not(:disabled) {
    border-color: var(--accent-color, #4a9eff);
  }

  .strategy-select:focus {
    outline: none;
    border-color: var(--accent-color, #4a9eff);
  }

  .strategy-select:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .strategy-select option {
    background: var(--bg-secondary, #1e1e1e);
    color: var(--text-primary, #fff);
  }
</style>
