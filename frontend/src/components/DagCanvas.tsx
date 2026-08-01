import { useCallback, useRef, useState, type DragEvent } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
  useReactFlow,
  type Node,
  type Edge,
  type OnConnect,
  type OnNodesChange,
  type OnEdgesChange,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { ConnectorPalette } from '@/components/ConnectorPalette'
import { useConnectors } from '@/hooks/useConnectors'

let nextNodeId = 1

function CanvasInner() {
  const { connectors, loading, error } = useConnectors()
  const { screenToFlowPosition } = useReactFlow()
  const wrapperRef = useRef<HTMLDivElement>(null)

  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])

  const onNodesChange: OnNodesChange = useCallback(
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
      const newNode: Node = {
        id,
        position,
        data: { label: connector, connector },
      }
      setNodes((current) => [...current, newNode])
    },
    [screenToFlowPosition],
  )

  return (
    <div className="flex h-screen w-screen">
      <ConnectorPalette connectors={connectors} loading={loading} error={error} />
      <div ref={wrapperRef} className="flex-1" onDragOver={onDragOver} onDrop={onDrop}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </div>
  )
}

/** `useReactFlow` (for drag-and-drop coordinate conversion) needs a provider
 * above it — kept here so callers of `DagCanvas` don't need to know that. */
export function DagCanvas() {
  return (
    <ReactFlowProvider>
      <CanvasInner />
    </ReactFlowProvider>
  )
}
