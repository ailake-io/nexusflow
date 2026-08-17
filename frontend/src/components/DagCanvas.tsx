import { useCallback, useEffect, useRef, useState, type DragEvent } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
  useReactFlow,
  type Edge,
  type OnConnect,
  type OnNodesChange,
  type OnEdgesChange,
  type OnSelectionChangeFunc,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Code2, Layers, Sparkles, Terminal } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import { ConnectorPalette } from '@/components/ConnectorPalette'
import { dagNodeTypes } from '@/components/dag-node-types'
import { ExecutionPanel } from '@/components/ExecutionPanel'
import { NodeInspector } from '@/components/NodeInspector'
import { PipelineIoPanel } from '@/components/PipelineIoPanel'
import { ApiError, createPipeline, updatePipeline } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'
import { useConnectors } from '@/hooks/useConnectors'
import { useRunProgress } from '@/hooks/useRunProgress'
import {
  fromPipelineSpec,
  toPipelineSpec,
  type ConnectorNodeData,
  type DagNode,
  type DagNodeData,
  type DbtNodeData,
  type EmbeddingNodeData,
  type PipelineMeta,
  type PipelineSpec,
  type PythonNodeData,
  type TransformNodeData,
} from '@/lib/dag'

function newNodeId(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return `node-${crypto.randomUUID()}`
  }
  return `node-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

interface CanvasInnerProps {
  pipelineToLoad?: PipelineSpec | null
  onPipelineLoaded?: () => void
}

function CanvasInner({ pipelineToLoad, onPipelineLoaded }: CanvasInnerProps) {
  const { t } = useI18n()
  const { connectors, loading, error } = useConnectors()
  const { screenToFlowPosition } = useReactFlow()
  const wrapperRef = useRef<HTMLDivElement>(null)
  const { token } = useAuth()
  const execution = useRunProgress()

  const [nodes, setNodes] = useState<DagNode[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [meta, setMeta] = useState<PipelineMeta>({ pipelineId: '' })
  const [runTriggerError, setRunTriggerError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [autoSaveEnabled, setAutoSaveEnabled] = useState(() => {
    if (typeof window === 'undefined') return false
    return window.localStorage.getItem('nexusflow-autosave') === 'true'
  })
  const [autoSaveStatus, setAutoSaveStatus] = useState<string | null>(null)

  const onNodesChange: OnNodesChange<DagNode> = useCallback(
    (changes) => setNodes((current) => applyNodeChanges(changes, current)),
    [],
  )
  const onEdgesChange: OnEdgesChange = useCallback(
    (changes) => setEdges((current) => applyEdgeChanges(changes, current)),
    [],
  )
  const onConnect: OnConnect = useCallback(
    (connection) => setEdges((current) => addEdge(connection, current)),
    [],
  )
  const onSelectionChange: OnSelectionChangeFunc = useCallback(({ nodes: selected }) => {
    setSelectedId(selected[0]?.id ?? null)
  }, [])

  const onDragOver = useCallback((event: DragEvent<HTMLDivElement>) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault()
      const connector = event.dataTransfer.getData('application/nexusflow-connector')
      if (!connector) return

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      const id = newNodeId()
      const newNode: DagNode = {
        id,
        type: 'connector',
        position,
        data: { kind: 'connector', connector, role: 'source', name: '', config: '{}' },
      }
      setNodes((current) => [...current, newNode])
    },
    [screenToFlowPosition],
  )

  const addTransformNode = useCallback(() => {
    const id = newNodeId()
    setNodes((current) => [
      ...current,
      {
        id,
        type: 'transform',
        position: { x: 200, y: 200 },
        data: { kind: 'transform', sql: '' },
      },
    ])
  }, [])

  const addDbtNode = useCallback(() => {
    const id = newNodeId()
    setNodes((current) => [
      ...current,
      {
        id,
        type: 'dbt',
        position: { x: 400, y: 200 },
        data: { kind: 'dbt', projectDir: '', command: 'run', select: '' },
      },
    ])
  }, [])

  const addPythonNode = useCallback(() => {
    const id = newNodeId()
    setNodes((current) => [
      ...current,
      {
        id,
        type: 'python',
        position: { x: 480, y: 200 },
        data: { kind: 'python', script: '', timeoutSeconds: 0 },
      },
    ])
  }, [])

  const addEmbeddingNode = useCallback(() => {
    const id = newNodeId()
    setNodes((current) => [
      ...current,
      {
        id,
        type: 'embedding',
        position: { x: 100, y: -100 },
        data: {
          kind: 'embedding',
          sourceColumn: '',
          outputColumn: 'embedding',
          dimension: 384,
          backend: 'onnx',
          repo: '',
          filename: '',
          tokenizerFilename: '',
          maxLength: 128,
          baseUrl: '',
          model: '',
          apiKeyEnv: '',
          strategy: 'fixed_window',
          chunkSize: 256,
          overlap: 0,
          separators: '',
        },
      },
    ])
  }, [])

  const updateNodeData = useCallback(
    (
      id: string,
      patch:
        | Partial<ConnectorNodeData>
        | Partial<TransformNodeData>
        | Partial<DbtNodeData>
        | Partial<EmbeddingNodeData>
        | Partial<PythonNodeData>,
    ) => {
      setNodes((current) =>
        current.map((n) =>
          n.id === id ? { ...n, data: { ...n.data, ...patch } as DagNodeData } : n,
        ),
      )
    },
    [],
  )

  const handleExport = useCallback(() => {
    const spec = toPipelineSpec(nodes, meta)
    return JSON.stringify(spec, null, 2)
  }, [nodes, meta])

  const handleRun = useCallback(() => {
    if (!token) return
    try {
      const spec = toPipelineSpec(nodes, meta)
      setRunTriggerError(null)
      execution.run(token, spec)
    } catch (err) {
      setRunTriggerError(err instanceof Error ? err.message : String(err))
    }
  }, [nodes, meta, token, execution])

  const handleSave = useCallback(async () => {
    if (!token) throw new Error('not logged in')
    const spec = toPipelineSpec(nodes, meta, true)
    setSaving(true)
    try {
      try {
        await createPipeline(token, spec)
      } catch (err) {
        if (err instanceof ApiError && err.status === 409) {
          await updatePipeline(token, spec)
        } else {
          throw err
        }
      }
    } finally {
      setSaving(false)
    }
  }, [nodes, meta, token])

  const saveRef = useRef(handleSave)
  const savingRef = useRef(saving)
  useEffect(() => {
    saveRef.current = handleSave
  }, [handleSave])
  useEffect(() => {
    savingRef.current = saving
  }, [saving])

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.localStorage.setItem('nexusflow-autosave', String(autoSaveEnabled))
  }, [autoSaveEnabled])

  useEffect(() => {
    if (!autoSaveEnabled || !meta.pipelineId.trim()) {
      setAutoSaveStatus(null)
      return
    }
    const id = setInterval(() => {
      if (savingRef.current) return
      saveRef
        .current()
        .then(() => setAutoSaveStatus(t('ioPanel.autoSaveSaved')))
        .catch((err) =>
          setAutoSaveStatus(
            t('ioPanel.autoSaveError', { msg: err instanceof Error ? err.message : String(err) }),
          ),
        )
    }, 60000)
    return () => clearInterval(id)
  }, [autoSaveEnabled, meta.pipelineId, t])

  const handleNewPipeline = useCallback(() => {
    if (nodes.length > 0 && !window.confirm(t('ioPanel.newPipelineConfirm'))) return
    setNodes([])
    setEdges([])
    setSelectedId(null)
    setMeta({ pipelineId: '' })
    setRunTriggerError(null)
  }, [nodes.length, t])

  const loadSpec = useCallback((spec: PipelineSpec) => {
    const { nodes: importedNodes, edges: importedEdges } = fromPipelineSpec(spec)
    setNodes(importedNodes)
    setEdges(importedEdges)
    setSelectedId(null)
    setMeta({
      pipelineId: spec.pipeline_id,
      channelCapacity: spec.channel_capacity,
      partitions: spec.partitions,
      schedule: spec.schedule,
    })
  }, [])

  const handleImport = useCallback(
    (json: string) => {
      const parsed = JSON.parse(json)
      if (!parsed || typeof parsed !== 'object' || typeof parsed.pipeline_id !== 'string') {
        throw new Error('invalid PipelineSpec: pipeline_id is required')
      }
      loadSpec(parsed as PipelineSpec)
    },
    [loadSpec],
  )

  // Loads a saved pipeline (fetched via GET /pipelines/{id}/spec from
  // PipelinesList's Edit button) onto the canvas — same code path as
  // pasting/loading JSON manually.
  useEffect(() => {
    if (!pipelineToLoad) return
    loadSpec(pipelineToLoad)
    onPipelineLoaded?.()
  }, [pipelineToLoad, loadSpec, onPipelineLoaded])

  // Delete/Backspace removes the selected node (and its edges) unless the
  // user is typing in an input/textarea/select.
  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (event.key !== 'Delete' && event.key !== 'Backspace') return
      const target = event.target as HTMLElement | null
      if (target && ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName)) return
      if (!selectedId) return
      event.preventDefault()
      setNodes((current) => current.filter((n) => n.id !== selectedId))
      setEdges((current) => current.filter((e) => e.source !== selectedId && e.target !== selectedId))
      setSelectedId(null)
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [selectedId])

  const selectedNode = nodes.find((n) => n.id === selectedId) ?? null

  return (
    <div className="flex h-full w-full flex-col">
      <PipelineIoPanel
        meta={meta}
        onMetaChange={setMeta}
        onExport={handleExport}
        onImport={handleImport}
        onRun={handleRun}
        onSave={handleSave}
        onNewPipeline={handleNewPipeline}
        running={execution.status === 'starting' || execution.status === 'running'}
        saving={saving}
        autoSaveEnabled={autoSaveEnabled}
        onToggleAutoSave={() => setAutoSaveEnabled((v) => !v)}
        autoSaveStatus={autoSaveStatus}
      />
      {runTriggerError && (
        <div className="flex items-center gap-2 border-b border-red-500/20 bg-red-500/10 px-4 py-2 text-xs text-red-400">
          <span className="font-semibold">{t('canvas.runError')}</span> {runTriggerError}
        </div>
      )}
      <div className="flex flex-1 overflow-hidden">
        <ConnectorPalette connectors={connectors} loading={loading} error={error} />
        <div ref={wrapperRef} className="relative flex-1 bg-background" onDragOver={onDragOver} onDrop={onDrop}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={dagNodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onSelectionChange={onSelectionChange}
            fitView
            fitViewOptions={{ maxZoom: 1 }}
          >
            <Background gap={20} size={1} color="oklch(1 0 0 / 8%)" />
          </ReactFlow>

          <div className="absolute bottom-4 left-4 flex gap-2">
            <button
              type="button"
              onClick={addTransformNode}
              className="flex items-center gap-2 rounded-lg border border-white/10 bg-card/90 px-3 py-2 text-sm font-medium text-foreground shadow-lg backdrop-blur transition-all hover:border-accent/40 hover:bg-card"
            >
              <Code2 className="h-3.5 w-3.5 text-accent" />
              {t('canvas.addTransform')}
            </button>
            <button
              type="button"
              onClick={addDbtNode}
              className="flex items-center gap-2 rounded-lg border border-white/10 bg-card/90 px-3 py-2 text-sm font-medium text-foreground shadow-lg backdrop-blur transition-all hover:border-emerald-400/40 hover:bg-card"
            >
              <Layers className="h-3.5 w-3.5 text-emerald-400" />
              {t('canvas.addDbt')}
            </button>
            <button
              type="button"
              onClick={addPythonNode}
              className="flex items-center gap-2 rounded-lg border border-white/10 bg-card/90 px-3 py-2 text-sm font-medium text-foreground shadow-lg backdrop-blur transition-all hover:border-sky-400/40 hover:bg-card"
            >
              <Terminal className="h-3.5 w-3.5 text-sky-400" />
              {t('canvas.addPython')}
            </button>
            <button
              type="button"
              onClick={addEmbeddingNode}
              className="flex items-center gap-2 rounded-lg border border-white/10 bg-card/90 px-3 py-2 text-sm font-medium text-foreground shadow-lg backdrop-blur transition-all hover:border-fuchsia-400/40 hover:bg-card"
            >
              <Sparkles className="h-3.5 w-3.5 text-fuchsia-400" />
              {t('canvas.addEmbedding')}
            </button>
          </div>
        </div>
        {selectedNode && (
          <NodeInspector node={selectedNode} connectors={connectors} onChange={updateNodeData} />
        )}
      </div>
      <ExecutionPanel
        status={execution.status}
        runId={execution.runId}
        partitions={execution.partitions}
        hardwareStats={execution.hardwareStats}
        error={execution.error}
        dbtSummary={execution.dbtSummary}
        logs={execution.logs}
      />
    </div>
  )
}

/** `useReactFlow` (for drag-and-drop coordinate conversion) needs a provider
 * above it — kept here so callers of `DagCanvas` don't need to know that. */
export function DagCanvas(props: CanvasInnerProps) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  )
}

export default DagCanvas
