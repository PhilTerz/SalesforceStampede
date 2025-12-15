<script lang="ts">
  import type { BatchSize } from '$lib/stores/bulkState';

  type Props = {
    value: BatchSize;
    onChange: (size: BatchSize) => void;
    disabled?: boolean;
  };

  let { value, onChange, disabled = false }: Props = $props();

  // Determine current selection type
  let selectedPreset = $derived.by(() => {
    if (typeof value === 'string') return value;
    return 'custom';
  });

  // Local state for the custom input field
  let localCustomValue = $state<number>(5000);

  // Update local state when value prop changes
  $effect(() => {
    if (typeof value === 'object' && 'custom' in value) {
      localCustomValue = value.custom;
    }
  });

  function handlePresetChange(event: Event) {
    const target = event.target as HTMLSelectElement;
    const preset = target.value;

    if (preset === 'custom') {
      onChange({ custom: localCustomValue });
    } else {
      onChange(preset as BatchSize);
    }
  }

  function handleCustomChange(event: Event) {
    const target = event.target as HTMLInputElement;
    const num = parseInt(target.value, 10);

    if (!isNaN(num) && num > 0) {
      localCustomValue = num;
      onChange({ custom: num });
    }
  }
</script>

<div class="batch-size-select">
  <select
    class="batch-select"
    value={selectedPreset}
    onchange={handlePresetChange}
    {disabled}
  >
    <option value="extra_small">Extra Small (10)</option>
    <option value="small">Small (200)</option>
    <option value="medium">Medium (2,000)</option>
    <option value="large">Large (10,000)</option>
    <option value="custom">Custom...</option>
  </select>

  {#if selectedPreset === 'custom'}
    <input
      type="number"
      class="custom-input"
      value={localCustomValue}
      oninput={handleCustomChange}
      min="1"
      max="100000"
      {disabled}
      placeholder="Records per batch"
    />
  {/if}
</div>

<style>
  .batch-size-select {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .batch-select {
    flex: 1;
    padding: 0.5rem 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
    min-width: 150px;
  }

  .batch-select:focus {
    outline: none;
    border-color: var(--accent-color, #4a9eff);
  }

  .batch-select:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .custom-input {
    width: 120px;
    padding: 0.5rem 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
  }

  .custom-input:focus {
    outline: none;
    border-color: var(--accent-color, #4a9eff);
  }

  .custom-input:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  /* Hide number input arrows */
  .custom-input::-webkit-outer-spin-button,
  .custom-input::-webkit-inner-spin-button {
    -webkit-appearance: none;
    margin: 0;
  }
</style>
