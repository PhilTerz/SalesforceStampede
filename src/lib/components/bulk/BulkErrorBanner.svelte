<script lang="ts">
  type Props = {
    error: string;
    onDismiss: () => void;
  };

  let { error, onDismiss }: Props = $props();
</script>

<div class="error-banner" role="alert">
  <div class="error-content">
    <span class="error-icon">!</span>
    <p class="error-message">{error}</p>
  </div>
  <button class="dismiss-btn" onclick={onDismiss} aria-label="Dismiss error">
    &times;
  </button>
</div>

<style>
  .error-banner {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
    padding: 1rem;
    background: rgba(244, 67, 54, 0.1);
    border: 1px solid var(--error-color, #f44);
    border-radius: 4px;
  }

  .error-content {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    flex: 1;
  }

  .error-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    background: var(--error-color, #f44);
    color: #fff;
    border-radius: 50%;
    font-size: 0.75rem;
    font-weight: 700;
    flex-shrink: 0;
  }

  .error-message {
    margin: 0;
    color: var(--error-color, #f44);
    font-size: 0.875rem;
    line-height: 1.4;
    word-break: break-word;
  }

  .dismiss-btn {
    background: none;
    border: none;
    color: var(--error-color, #f44);
    font-size: 1.25rem;
    cursor: pointer;
    padding: 0;
    line-height: 1;
    opacity: 0.7;
  }

  .dismiss-btn:hover {
    opacity: 1;
  }
</style>
