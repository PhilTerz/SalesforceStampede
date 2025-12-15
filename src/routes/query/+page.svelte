<script lang="ts">
  import { onMount } from 'svelte';
  import { currentOrg, type OrgSummary } from '$lib/stores/currentOrg';
  import {
    queryState,
    setSoql,
    setStrategy,
    clearError,
    type QueryUiState
  } from '$lib/stores/queryState';
  import { runQuery, prepareRecordsForGrid } from '$lib/tauri/query';
  import QueryEditor from '$lib/components/query/QueryEditor.svelte';
  import QueryToolbar from '$lib/components/query/QueryToolbar.svelte';
  import QueryProgress from '$lib/components/query/QueryProgress.svelte';
  import QueryErrorBanner from '$lib/components/query/QueryErrorBanner.svelte';
  import ResultsGrid from '$lib/components/query/ResultsGrid.svelte';
  import BulkExportResult from '$lib/components/query/BulkExportResult.svelte';

  let org = $state<OrgSummary | null>(null);
  let state = $state<QueryUiState>({
    soql: 'SELECT Id, Name FROM Account LIMIT 10',
    strategy: 'auto',
    running: false,
    progress: null,
    lastResult: null,
    error: null
  });

  currentOrg.subscribe((value) => {
    org = value;
  });

  queryState.subscribe((value) => {
    state = value;
  });

  let pageRef: HTMLDivElement | null = $state(null);

  // Listen for custom run-query event from editor
  onMount(() => {
    const handleRunQuery = (e: Event) => {
      e.preventDefault();
      handleRun();
    };

    pageRef?.addEventListener('run-query', handleRunQuery);
    return () => {
      pageRef?.removeEventListener('run-query', handleRunQuery);
    };
  });

  function handleSoqlChange(soql: string) {
    setSoql(soql);
  }

  function handleStrategyChange(strategy: 'auto' | 'rest' | 'bulk') {
    setStrategy(strategy);
  }

  async function handleRun() {
    if (!org || state.running) return;

    try {
      await runQuery(state.soql, state.strategy);
    } catch (e) {
      // Error is already handled in runQuery -> failQuery
      console.error('Query failed:', e);
    }
  }

  function handleDismissError() {
    clearError();
  }

  // Prepare grid data when we have REST results
  let gridData = $derived.by(() => {
    if (state.lastResult?.kind === 'rest' && state.lastResult.records.length > 0) {
      return prepareRecordsForGrid(state.lastResult.records);
    }
    return null;
  });

  let hasNoOrg = $derived(!org);
  let isDisabled = $derived(hasNoOrg || state.running);
</script>

<div class="query-page" bind:this={pageRef}>
  <header class="page-header">
    <h1>Query</h1>
    {#if org}
      <span class="org-badge">{org.name}</span>
    {/if}
  </header>

  {#if hasNoOrg}
    <div class="no-org-message">
      <p>Select an org to run queries</p>
      <p class="hint">Use the org switcher in the sidebar to connect to a Salesforce org.</p>
    </div>
  {:else}
    <div class="query-content">
      <!-- Toolbar -->
      <QueryToolbar
        strategy={state.strategy}
        onStrategyChange={handleStrategyChange}
        onRun={handleRun}
        running={state.running}
        disabled={isDisabled}
      />

      <!-- Editor -->
      <div class="editor-section">
        <QueryEditor
          value={state.soql}
          onChange={handleSoqlChange}
          disabled={state.running}
        />
      </div>

      <!-- Error Banner -->
      {#if state.error}
        <QueryErrorBanner error={state.error} onDismiss={handleDismissError} />
      {/if}

      <!-- Progress -->
      {#if state.running && state.progress}
        <QueryProgress progress={state.progress} />
      {/if}

      <!-- Results -->
      {#if !state.running && state.lastResult}
        <div class="results-section">
          {#if state.lastResult.kind === 'rest'}
            {#if gridData && gridData.flatRecords.length > 0 && org}
              <ResultsGrid
                records={gridData.flatRecords}
                columns={gridData.columns}
                instanceUrl={org.instanceUrl}
                truncated={state.lastResult.truncated}
                totalSize={state.lastResult.totalSize}
              />
            {:else}
              <div class="empty-results">
                <p>No records returned</p>
              </div>
            {/if}
          {:else if state.lastResult.kind === 'bulk'}
            <BulkExportResult exportPath={state.lastResult.exportPath} />
          {/if}
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .query-page {
    height: 100%;
    display: flex;
    flex-direction: column;
  }

  .page-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1rem;
    flex-shrink: 0;
  }

  .page-header h1 {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--text-primary, #fff);
  }

  .org-badge {
    padding: 0.25rem 0.75rem;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    font-size: 0.75rem;
    color: var(--text-secondary, #888);
  }

  .no-org-message {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
    color: var(--text-secondary, #888);
  }

  .no-org-message p {
    margin: 0;
  }

  .no-org-message .hint {
    margin-top: 0.5rem;
    font-size: 0.875rem;
  }

  .query-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 1rem;
    min-height: 0;
  }

  .editor-section {
    height: 200px;
    flex-shrink: 0;
  }

  .results-section {
    flex: 1;
    min-height: 300px;
    display: flex;
    flex-direction: column;
  }

  .empty-results {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--bg-secondary, #1e1e1e);
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    color: var(--text-secondary, #888);
  }

  .empty-results p {
    margin: 0;
  }
</style>
