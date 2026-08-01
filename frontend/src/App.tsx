import { useState } from 'react'
import { useAuth } from '@/lib/auth'
import { LoginForm } from '@/components/LoginForm'
import { DagCanvas } from '@/components/DagCanvas'
import { PipelinesList } from '@/components/PipelinesList'

type View = 'canvas' | 'pipelines'

function App() {
  const { token } = useAuth()
  const [view, setView] = useState<View>('canvas')

  if (!token) return <LoginForm />

  return (
    <div className="flex h-screen w-screen flex-col">
      <nav className="flex gap-1 border-b bg-card px-3 py-1.5">
        {(['canvas', 'pipelines'] as const).map((v) => (
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
      </nav>
      <div className="flex-1 overflow-hidden">
        {view === 'canvas' ? <DagCanvas /> : <PipelinesList />}
      </div>
    </div>
  )
}

export default App
