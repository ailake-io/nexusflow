import { useState } from 'react'
import {
  Workflow,
  List,
  Activity,
  LogOut,
  Sparkles,
} from 'lucide-react'
import { useAuth } from '@/lib/auth-context'
import { LoginForm } from '@/components/LoginForm'
import { DagCanvas } from '@/components/DagCanvas'
import { PipelinesList } from '@/components/PipelinesList'
import { PipelineStatusBoard } from '@/components/PipelineStatusBoard'
import { Logo } from '@/components/Logo'
import { Button } from '@/components/ui/button'
import type { PipelineSpec } from '@/lib/dag'

type View = 'canvas' | 'pipelines' | 'status'

const NAV_ITEMS: { id: View; label: string; icon: typeof Workflow }[] = [
  { id: 'canvas', label: 'Canvas', icon: Workflow },
  { id: 'pipelines', label: 'Pipelines', icon: List },
  { id: 'status', label: 'Status', icon: Activity },
]

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
    <div className="flex h-screen w-screen overflow-hidden bg-background">
      <aside className="flex w-60 shrink-0 flex-col border-r bg-sidebar">
        <div className="flex h-14 items-center border-b border-sidebar-border px-4">
          <Logo />
        </div>

        <nav className="flex-1 overflow-auto p-3">
          <div className="mb-2 px-2 text-[10px] font-semibold uppercase tracking-wider text-sidebar-foreground/50">
            Workbench
          </div>
          <div className="flex flex-col gap-1">
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon
              const active = view === item.id
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setView(item.id)}
                  className={`flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                    active
                      ? 'bg-sidebar-primary/10 text-sidebar-primary'
                      : 'text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-foreground'
                  }`}
                >
                  <Icon className="h-4 w-4" />
                  {item.label}
                </button>
              )
            })}
          </div>

          <div className="mt-6 rounded-lg border border-white/5 bg-white/[0.02] p-3">
            <div className="flex items-center gap-2 text-xs font-medium text-foreground">
              <Sparkles className="h-3.5 w-3.5 text-primary" />
              AI Lakehouse Builder
            </div>
            <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">
              Drag connectors, add transforms, and run data pipelines with built-in CDC support.
            </p>
          </div>
        </nav>

        <div className="border-t border-sidebar-border p-3">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={logout}
            className="w-full justify-start gap-2 text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-foreground"
          >
            <LogOut className="h-4 w-4" />
            Sign out
          </Button>
        </div>
      </aside>

      <main className="flex-1 overflow-hidden">
        <div className="flex h-14 items-center justify-between border-b bg-card px-6">
          <h1 className="text-sm font-medium capitalize text-foreground">
            {NAV_ITEMS.find((i) => i.id === view)?.label}
          </h1>
          <div className="text-xs text-muted-foreground">
            NexusFlow v0.0.0
          </div>
        </div>
        <div className="h-[calc(100vh-3.5rem)] animate-fade-in">
          {view === 'canvas' && (
            <DagCanvas
              pipelineToLoad={pipelineToLoad}
              onPipelineLoaded={() => setPipelineToLoad(null)}
            />
          )}
          {view === 'pipelines' && <PipelinesList onEdit={handleEdit} />}
          {view === 'status' && <PipelineStatusBoard />}
        </div>
      </main>
    </div>
  )
}

export default App
