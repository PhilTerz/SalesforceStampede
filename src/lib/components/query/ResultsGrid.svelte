<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { createGrid, type GridApi, type ColDef, type GridOptions } from 'ag-grid-community';
  import 'ag-grid-community/styles/ag-grid.css';
  import 'ag-grid-community/styles/ag-theme-alpine.css';
  import { isIdColumn, isSalesforceId } from '$lib/query/flatten';
  import { openRecordInBrowser } from '$lib/tauri/query';

  type Props = {
    records: Record<string, unknown>[];
    columns: string[];
    instanceUrl: string;
    truncated?: boolean;
    totalSize?: number;
  };

  let { records, columns, instanceUrl, truncated = false, totalSize }: Props = $props();

  let containerRef: HTMLDivElement | null = $state(null);
  let gridApi: GridApi | null = null;

  // Build column definitions from column names
  function buildColDefs(cols: string[]): ColDef[] {
    return cols.map((col) => {
      const colDef: ColDef = {
        field: col,
        headerName: col,
        sortable: true,
        resizable: true,
        filter: true,
        minWidth: 100
      };

      // Make ID columns clickable
      if (isIdColumn(col)) {
        colDef.cellRenderer = (params: { value: unknown }) => {
          const value = params.value;
          if (value && isSalesforceId(value)) {
            const link = document.createElement('button');
            link.className = 'id-link';
            link.textContent = String(value);
            link.onclick = (e) => {
              e.preventDefault();
              openRecordInBrowser(instanceUrl, String(value));
            };
            return link;
          }
          return String(value ?? '');
        };
      }

      return colDef;
    });
  }

  onMount(() => {
    if (!containerRef) return;

    const gridOptions: GridOptions = {
      columnDefs: buildColDefs(columns),
      rowData: records,
      defaultColDef: {
        flex: 1,
        minWidth: 100,
        sortable: true,
        resizable: true
      },
      animateRows: false,
      rowSelection: 'multiple',
      suppressCellFocus: true
    };

    gridApi = createGrid(containerRef, gridOptions);
  });

  onDestroy(() => {
    gridApi?.destroy();
  });

  // Update data when records change
  $effect(() => {
    if (gridApi) {
      gridApi.setGridOption('columnDefs', buildColDefs(columns));
      gridApi.setGridOption('rowData', records);
    }
  });
</script>

<div class="results-container">
  {#if truncated}
    <div class="truncation-warning">
      Truncated at 50,000 rows. Use Bulk export for full results.
      {#if totalSize}
        (Total: {totalSize.toLocaleString()} rows)
      {/if}
    </div>
  {/if}

  <div class="results-header">
    <span class="row-count">{records.length.toLocaleString()} rows</span>
    {#if totalSize && totalSize !== records.length}
      <span class="total-count">of {totalSize.toLocaleString()} total</span>
    {/if}
  </div>

  <div class="grid-wrapper ag-theme-alpine-dark" bind:this={containerRef}></div>
</div>

<style>
  .results-container {
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 300px;
  }

  .truncation-warning {
    padding: 0.75rem;
    background: rgba(255, 152, 0, 0.1);
    border: 1px solid var(--warning-color, #ff9800);
    border-radius: 4px;
    color: var(--warning-color, #ff9800);
    font-size: 0.875rem;
    margin-bottom: 0.75rem;
  }

  .results-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0;
    font-size: 0.875rem;
  }

  .row-count {
    color: var(--text-primary, #fff);
    font-weight: 500;
  }

  .total-count {
    color: var(--text-secondary, #888);
  }

  .grid-wrapper {
    flex: 1;
    width: 100%;
    min-height: 200px;
    border-radius: 4px;
    overflow: hidden;
  }

  /* AG Grid dark theme overrides */
  :global(.ag-theme-alpine-dark) {
    --ag-background-color: var(--bg-primary, #1a1a1a);
    --ag-header-background-color: var(--bg-secondary, #1e1e1e);
    --ag-odd-row-background-color: var(--bg-secondary, #1e1e1e);
    --ag-border-color: var(--border-color, #333);
    --ag-foreground-color: var(--text-primary, #f0f0f0);
    --ag-secondary-foreground-color: var(--text-secondary, #888);
  }

  /* ID link styling */
  :global(.id-link) {
    background: none;
    border: none;
    color: var(--accent-color, #4a9eff);
    cursor: pointer;
    padding: 0;
    font-size: inherit;
    text-decoration: none;
  }

  :global(.id-link:hover) {
    text-decoration: underline;
  }
</style>
