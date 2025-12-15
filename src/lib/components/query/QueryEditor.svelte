<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import type * as Monaco from 'monaco-editor';

  type Props = {
    value: string;
    onChange: (value: string) => void;
    disabled?: boolean;
  };

  let { value, onChange, disabled = false }: Props = $props();

  let containerRef: HTMLDivElement | null = $state(null);
  let editor: Monaco.editor.IStandaloneCodeEditor | null = null;
  let monaco: typeof Monaco | null = null;

  // Track if we're updating from outside to prevent infinite loops
  let isExternalUpdate = false;

  onMount(async () => {
    // Dynamically import Monaco to ensure it loads only on client
    const monacoModule = await import('monaco-editor');
    monaco = monacoModule;

    if (!containerRef) return;

    // Create editor instance
    editor = monaco.editor.create(containerRef, {
      value,
      language: 'sql', // SQL highlighting works reasonably for SOQL
      theme: 'vs-dark',
      minimap: { enabled: false },
      fontSize: 14,
      lineNumbers: 'on',
      scrollBeyondLastLine: false,
      automaticLayout: true,
      wordWrap: 'on',
      tabSize: 2,
      readOnly: disabled,
      padding: { top: 8, bottom: 8 }
    });

    // Listen for content changes
    editor.onDidChangeModelContent(() => {
      if (!isExternalUpdate && editor) {
        const newValue = editor.getValue();
        onChange(newValue);
      }
    });

    // Handle Ctrl+Enter / Cmd+Enter to run query
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      // Dispatch a custom event that parent can listen to
      containerRef?.dispatchEvent(new CustomEvent('run-query', { bubbles: true }));
    });
  });

  onDestroy(() => {
    editor?.dispose();
  });

  // Update editor when value changes externally
  $effect(() => {
    if (editor && value !== editor.getValue()) {
      isExternalUpdate = true;
      editor.setValue(value);
      isExternalUpdate = false;
    }
  });

  // Update readonly state
  $effect(() => {
    if (editor) {
      editor.updateOptions({ readOnly: disabled });
    }
  });
</script>

<div class="editor-wrapper">
  <div class="editor-container" bind:this={containerRef}></div>
</div>

<style>
  .editor-wrapper {
    width: 100%;
    height: 100%;
    min-height: 150px;
    border: 1px solid var(--border-color, #333);
    border-radius: 4px;
    overflow: hidden;
  }

  .editor-container {
    width: 100%;
    height: 100%;
  }
</style>
