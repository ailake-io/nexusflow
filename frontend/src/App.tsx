import { ReactFlow, Background, Controls } from '@xyflow/react'
import '@xyflow/react/dist/style.css'

// Placeholder canvas — proves the Vite + React Flow + Tailwind wiring works.
// The real DAG canvas (nodes from GET /connectors, drag-and-drop) is Marco 8's
// next task; this file will grow into that.
function App() {
  return (
    <div className="h-screen w-screen">
      <ReactFlow nodes={[]} edges={[]} fitView>
        <Background />
        <Controls />
      </ReactFlow>
    </div>
  )
}

export default App
