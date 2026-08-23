import { useCallback, useEffect, useState, lazy, Suspense } from 'react'
import {
  Workflow,
  List,
  Activity,
  LogOut,
  Sparkles,
  ShieldCheck,
  Store as StoreIcon,
  Waypoints,
  BarChart3,
} from 'lucide-react'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { LoginForm } from '@/components/LoginForm'
import { Logo } from '@/components/Logo'
import { LanguageToggle } from '@/components/LanguageToggle'
import { Button } from '@/components/ui/button'
import type { PipelineSpec } from '@/lib/dag'

const DagCanvas = lazy(() => import('@/components/DagCanvas'))
const PipelinesList = lazy(() => import('@/components/PipelinesList'))
const PipelineStatusBoard = lazy(() => import('@/components/PipelineStatusBoard'))
const UsersPanel = lazy(() => import('@/components/UsersPanel'))
const Store = lazy(() => import('@/components/Store'))
const LineagePanel = lazy(() => import('@/components/LineagePanel'))
const DataPreviewPanel = lazy(() => import('@/components/DataPreviewPanel'))

type View = 'canvas' | 'pipelines' | 'status' | 'store' | 'lineage' | 'preview' | 'admin'

function ViewFallback() {
  const { t } = useI18n()
  return (
    <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
      <span className="animate-pulse">{t('common.loading')}</span>
    </div>
  )
}

function App() {
  const { token, role, logout } = useAuth()
  const { t } = useI18n()
  const [view, setView] = useState<View>('canvas')
  const [pipelineToLoad, setPipelineToLoad] = useState<PipelineSpec | null>(null)

  const handleEdit = useCallback((spec: PipelineSpec) => {
    setPipelineToLoad(spec)
    setView('canvas')
  }, [])

  const handlePipelineLoaded = useCallback(() => setPipelineToLoad(null), [])

  // `view` is plain component state, not a route — it doesn't reset on its
  // own when a different (non-Admin) user logs in on the same tab, which
  // would otherwise leave them staring at a 403'd Admin panel instead of
  // Canvas. The /users routes are still Admin-enforced server-side either
  // way; this only fixes the UX of the stale client-side state.
  useEffect(() => {
    if (role !== 'admin' && view === 'admin') {
      setView('canvas')
    }
  }, [role, view])

  if (!token) return <LoginForm />

  const navItems: { id: View; label: string; icon: typeof Workflow }[] = [
    { id: 'canvas', label: t('nav.canvas'), icon: Workflow },
    { id: 'pipelines', label: t('nav.pipelines'), icon: List },
    { id: 'status', label: t('nav.status'), icon: Activity },
    { id: 'store', label: t('nav.store'), icon: StoreIcon },
    { id: 'lineage', label: t('nav.lineage'), icon: Waypoints },
    { id: 'preview', label: t('nav.preview'), icon: BarChart3 },
    // Client-side gating only decides visibility of the nav item — the
    // /users routes are Admin-enforced server-side regardless (auth.rs).
    ...(role === 'admin'
      ? [{ id: 'admin' as const, label: t('nav.admin'), icon: ShieldCheck }]
      : []),
  ]

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background">
      <aside className="flex w-60 shrink-0 flex-col border-r bg-sidebar">
        <div className="flex h-14 items-center border-b border-sidebar-border px-4">
          <Logo />
        </div>

        <nav className="flex-1 overflow-auto p-3">
          <div className="mb-2 px-2 text-[10px] font-semibold uppercase tracking-wider text-sidebar-foreground/50">
            {t('app.workbench')}
          </div>
          <div className="flex flex-col gap-1">
            {navItems.map((item) => {
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
              {t('app.tagline')}
            </div>
            <p className="mt-1 text-[10px] leading-relaxed text-muted-foreground">
              {t('app.taglineDescription')}
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
            {t('nav.signOut')}
          </Button>
        </div>
      </aside>

      <main className="flex-1 overflow-hidden">
        <div className="flex h-14 items-center justify-between border-b bg-card px-6">
          <h1 className="text-sm font-medium capitalize text-foreground">
            {navItems.find((i) => i.id === view)?.label}
          </h1>
          <div className="flex items-center gap-4">
            <LanguageToggle />
            <div className="text-xs text-muted-foreground">{t('app.version')}</div>
          </div>
        </div>
        <div className="h-[calc(100vh-3.5rem)] animate-fade-in">
          <Suspense fallback={<ViewFallback />}>
            {view === 'canvas' && (
              <DagCanvas
                pipelineToLoad={pipelineToLoad}
                onPipelineLoaded={handlePipelineLoaded}
              />
            )}
            {view === 'pipelines' && <PipelinesList onEdit={handleEdit} />}
            {view === 'status' && <PipelineStatusBoard />}
            {view === 'store' && <Store />}
            {view === 'lineage' && <LineagePanel />}
            {view === 'preview' && <DataPreviewPanel />}
            {view === 'admin' && <UsersPanel />}
          </Suspense>
        </div>
      </main>
    </div>
  )
}

export default App
