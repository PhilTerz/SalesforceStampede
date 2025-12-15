<script lang="ts">
  import { login, type LoginEnvironment } from '$lib/tauri/orgs';

  type Props = {
    isOpen: boolean;
    onClose: () => void;
    onSuccess?: () => void;
  };

  let { isOpen, onClose, onSuccess }: Props = $props();

  let environment = $state<LoginEnvironment>('production');
  let customDomain = $state('');
  let isLoading = $state(false);
  let error = $state<string | null>(null);

  let productionRadioRef: HTMLInputElement | null = $state(null);

  // Focus the first radio button when modal opens
  $effect(() => {
    if (isOpen && productionRadioRef) {
      // Small delay to ensure DOM is ready
      requestAnimationFrame(() => {
        productionRadioRef?.focus();
      });
    }
  });

  function resetForm() {
    environment = 'production';
    customDomain = '';
    error = null;
  }

  async function handleSubmit(e: Event) {
    e.preventDefault();

    // Prevent double-submit
    if (isLoading) return;

    isLoading = true;
    error = null;

    try {
      await login({
        environment,
        customDomain: environment === 'custom' ? customDomain.trim() : undefined
      });

      // Success - close modal and notify parent
      resetForm();
      onSuccess?.();
      onClose();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Login failed';
    } finally {
      isLoading = false;
    }
  }

  function handleClose() {
    // Prevent close during login (OAuth in progress)
    if (isLoading) return;
    resetForm();
    onClose();
  }

  function handleBackdropClick(e: MouseEvent) {
    if (e.target === e.currentTarget && !isLoading) {
      handleClose();
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape' && !isLoading) {
      handleClose();
    }
  }
</script>

<svelte:window onkeydown={handleKeydown} />

{#if isOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions a11y_no_noninteractive_element_interactions -->
  <div class="modal-backdrop" onclick={handleBackdropClick} role="presentation">
    <div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
      <header class="modal-header">
        <h2 id="modal-title">Connect Salesforce Org</h2>
        {#if !isLoading}
          <button class="close-btn" onclick={handleClose} aria-label="Close">
            &times;
          </button>
        {/if}
      </header>

      <form class="modal-body" onsubmit={handleSubmit}>
        <fieldset class="form-group" disabled={isLoading}>
          <legend class="form-legend">Environment</legend>

          <label class="radio-label">
            <input
              type="radio"
              name="environment"
              value="production"
              bind:group={environment}
              bind:this={productionRadioRef}
            />
            <span>Production</span>
          </label>

          <label class="radio-label">
            <input
              type="radio"
              name="environment"
              value="sandbox"
              bind:group={environment}
            />
            <span>Sandbox</span>
          </label>

          <label class="radio-label">
            <input
              type="radio"
              name="environment"
              value="custom"
              bind:group={environment}
            />
            <span>Custom Domain</span>
          </label>
        </fieldset>

        {#if environment === 'custom'}
          <div class="form-group">
            <label class="input-label" for="custom-domain">
              Custom Domain
            </label>
            <input
              id="custom-domain"
              type="text"
              class="input"
              placeholder="mydomain.my.salesforce.com"
              bind:value={customDomain}
              disabled={isLoading}
              required={environment === 'custom'}
            />
            <p class="input-hint">Enter your Salesforce My Domain URL</p>
          </div>
        {/if}

        {#if isLoading}
          <div class="loading-message">
            <span class="loading-spinner"></span>
            <p>Check your browser to complete login...</p>
            <p class="loading-hint">A browser window should open for Salesforce authentication.</p>
          </div>
        {/if}

        {#if error}
          <div class="error-message">
            <p>{error}</p>
          </div>
        {/if}

        <div class="modal-actions">
          <button
            type="button"
            class="btn btn-secondary"
            onclick={handleClose}
            disabled={isLoading}
          >
            Cancel
          </button>
          <button
            type="submit"
            class="btn btn-primary"
            disabled={isLoading || (environment === 'custom' && !customDomain.trim())}
          >
            {isLoading ? 'Connecting...' : 'Connect'}
          </button>
        </div>
      </form>
    </div>
  </div>
{/if}

<style>
  .modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.7);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
  }

  .modal {
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 8px;
    width: 100%;
    max-width: 420px;
    max-height: 90vh;
    overflow-y: auto;
  }

  .modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1rem 1.25rem;
    border-bottom: 1px solid var(--border-color, #333);
  }

  .modal-header h2 {
    margin: 0;
    font-size: 1.1rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
  }

  .close-btn {
    background: none;
    border: none;
    font-size: 1.5rem;
    color: var(--text-secondary, #888);
    cursor: pointer;
    padding: 0;
    line-height: 1;
  }

  .close-btn:hover {
    color: var(--text-primary, #fff);
  }

  .modal-body {
    padding: 1.25rem;
  }

  .form-group {
    margin-bottom: 1.25rem;
    border: none;
    padding: 0;
  }

  .form-legend {
    font-size: 0.875rem;
    font-weight: 500;
    color: var(--text-primary, #fff);
    margin-bottom: 0.75rem;
    padding: 0;
  }

  .radio-label {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0;
    cursor: pointer;
    color: var(--text-primary, #fff);
  }

  .radio-label input[type="radio"] {
    accent-color: var(--accent-color, #4a9eff);
  }

  .input-label {
    display: block;
    font-size: 0.875rem;
    font-weight: 500;
    color: var(--text-primary, #fff);
    margin-bottom: 0.5rem;
  }

  .input {
    width: 100%;
    padding: 0.625rem 0.75rem;
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-primary, #fff);
    font-size: 0.875rem;
  }

  .input:focus {
    outline: none;
    border-color: var(--accent-color, #4a9eff);
  }

  .input:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .input-hint {
    margin: 0.375rem 0 0;
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
  }

  .loading-message {
    text-align: center;
    padding: 1rem;
    background: var(--bg-primary, #1a1a1a);
    border-radius: 4px;
    margin-bottom: 1rem;
  }

  .loading-message p {
    margin: 0.5rem 0 0;
    color: var(--text-primary, #fff);
  }

  .loading-hint {
    font-size: 0.75rem;
    color: var(--text-secondary, #888) !important;
  }

  .loading-spinner {
    display: inline-block;
    width: 24px;
    height: 24px;
    border: 2px solid var(--border-color, #333);
    border-top-color: var(--accent-color, #4a9eff);
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  .error-message {
    padding: 0.75rem;
    background: rgba(244, 67, 54, 0.1);
    border: 1px solid var(--error-color, #f44);
    border-radius: 4px;
    margin-bottom: 1rem;
  }

  .error-message p {
    margin: 0;
    color: var(--error-color, #f44);
    font-size: 0.875rem;
  }

  .modal-actions {
    display: flex;
    gap: 0.75rem;
    justify-content: flex-end;
    padding-top: 0.5rem;
  }

  .btn {
    padding: 0.625rem 1rem;
    border-radius: 4px;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
  }

  .btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .btn-secondary {
    background: var(--bg-primary, #1a1a1a);
    border: 1px solid var(--border-color, #333);
    color: var(--text-primary, #fff);
  }

  .btn-secondary:hover:not(:disabled) {
    background: var(--bg-hover, #2a2a2a);
  }

  .btn-primary {
    background: var(--accent-color, #4a9eff);
    border: 1px solid var(--accent-color, #4a9eff);
    color: #fff;
  }

  .btn-primary:hover:not(:disabled) {
    background: #3d8be6;
    border-color: #3d8be6;
  }
</style>
