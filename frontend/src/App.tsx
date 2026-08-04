import { useState } from 'react'
import { useAuth } from '@/lib/auth-context'
import { LoginForm } from '@/components/LoginForm'
import { DagCanvas } from '@/components/DagCanvas'
import { PipelinesList } from '@/components/PipelinesList'
import { PipelineStatusBoard } from '@/components/PipelineStatusBoard'
import type { PipelineSpec } from '@/lib/dag'

type View = 'canvas' | 'pipelines' | 'status'

function App() {
  const { token, logout } = useAuth()
  const [view, setView] = useState<View>('canvas')
  const [pipelineToLoad, setPipelineToLoad] = useState<PipelineSpec | null>(null)

  if (!token) return <LoginForm />

  const handleEdit = (spec: PipelineSpec) => {
    setPipelineToLoad(spec)
    setView('canvas')
  }

  return (
    <div className="flex h-screen w-screen flex-col">
      <nav className="flex items-center justify-between gap-1 border-b bg-card px-3 py-1.5">
        <div className="flex gap-1">
          {(['canvas', 'pipelines', 'status'] as const).map((v) => (
            <button
              key={v}
              type="button"
              onClick={() => setView(v)}
              className={`rounded-md px-2.5 py-1 text-sm capitalize ${
                view === v ? 'bg-muted font-medium' : 'text-muted-foreground hover:bg-muted/50'
              }`}
            >
              {v}
            </button>
          ))}
        </div>
        <button
          type="button"
          onClick={logout}
          className="rounded-md px-2.5 py-1 text-sm text-muted-foreground hover:bg-muted/50"
        >
          Logout
        </button>
      </nav>
      <div className="flex-1 overflow-hidden">
        {view === 'canvas' && (
          <DagCanvas
            pipelineToLoad={pipelineToLoad}
            onPipelineLoaded={() => setPipelineToLoad(null)}
          />
        )}
        {view === 'pipelines' && <PipelinesList onEdit={handleEdit} />}
        {view === 'status' && <PipelineStatusBoard />}
      </div>
    </div>
  )
}

export default App
