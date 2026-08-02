import { useCallback, useEffect, useRef, useState, type DragEvent } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
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
  type PipelineMeta,
  type PipelineSpec,
  type TransformNodeData,
} from '@/lib/dag'

let nextNodeId = 1

interface CanvasInnerProps {
  pipelineToLoad?: PipelineSpec | null
  onPipelineLoaded?: () => void
}

function CanvasInner({ pipelineToLoad, onPipelineLoaded }: CanvasInnerProps) {
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
      const id = `node-${nextNodeId++}`
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
    const id = `node-${nextNodeId++}`
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
    const id = `node-${nextNodeId++}`
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

  const updateNodeData = useCallback(
    (
      id: string,
      patch: Partial<ConnectorNodeData> | Partial<TransformNodeData> | Partial<DbtNodeData>,
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
    const spec = toPipelineSpec(nodes, meta)
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
    (json: string) => loadSpec(JSON.parse(json) as PipelineSpec),
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
        running={execution.status === 'starting' || execution.status === 'running'}
        saving={saving}
      />
      {runTriggerError && (
        <p className="border-b bg-card px-3 py-1 text-sm text-destructive">{runTriggerError}</p>
      )}
      <div className="flex flex-1 overflow-hidden">
        <ConnectorPalette connectors={connectors} loading={loading} error={error} />
        <div ref={wrapperRef} className="flex-1" onDragOver={onDragOver} onDrop={onDrop}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={dagNodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onSelectionChange={onSelectionChange}
            fitView
          >
            <Background />
            <Controls />
          </ReactFlow>
        </div>
        {selectedNode && (
          <NodeInspector node={selectedNode} connectors={connectors} onChange={updateNodeData} />
        )}
      </div>
      <ExecutionPanel
        status={execution.status}
        runId={execution.runId}
        partitions={execution.partitions}
        error={execution.error}
        dbtSummary={execution.dbtSummary}
      />
      <div className="absolute bottom-4 left-64 flex gap-2">
        <button
          type="button"
          onClick={addTransformNode}
          className="rounded-md border bg-card px-3 py-1.5 text-sm shadow-sm hover:bg-muted"
        >
          + Transform
        </button>
        <button
          type="button"
          onClick={addDbtNode}
          className="rounded-md border bg-card px-3 py-1.5 text-sm shadow-sm hover:bg-muted"
        >
          + dbt
        </button>
      </div>
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
