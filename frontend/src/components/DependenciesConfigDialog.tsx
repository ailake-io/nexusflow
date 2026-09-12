import { useI18n } from '@/lib/i18n'
import { usePipelines } from '@/hooks/usePipelines'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'

interface DependenciesConfigDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Excluded from the candidate list — a pipeline can't depend on itself
   *  (dag.rs's own `validate()` rejects this server-side too). Empty string
   *  for a not-yet-named new pipeline just means nothing gets excluded. */
  currentPipelineId: string
  dependsOn?: string[]
  dependencyMode?: 'any' | 'all'
  onChange: (dependsOn: string[] | undefined, dependencyMode: 'any' | 'all' | undefined) => void
}

/**
 * Cross-pipeline dependency configuration (Fase 26) — pick 0+ upstream
 * pipelines from every other saved pipeline, plus a trigger mode (Any/All)
 * once 2+ are selected. Same controlled, writes-straight-through pattern as
 * `AlertsConfigDialog`/`QualityChecksDialog` — no local draft state to lose
 * on close/reopen.
 */
export function DependenciesConfigDialog({
  open,
  onOpenChange,
  currentPipelineId,
  dependsOn,
  dependencyMode,
  onChange,
}: DependenciesConfigDialogProps) {
  const { t } = useI18n()
  const { pipelines, loading } = usePipelines()
  const selected = new Set(dependsOn ?? [])
  const candidates = pipelines.filter((p) => p.pipeline_id !== currentPipelineId)

  const toggle = (id: string) => {
    const next = new Set(selected)
    if (next.has(id)) {
      next.delete(id)
    } else {
      next.add(id)
    }
    const arr = Array.from(next)
    onChange(arr.length > 0 ? arr : undefined, arr.length > 0 ? (dependencyMode ?? 'any') : undefined)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] max-w-md overflow-y-auto sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{t('dependencies.title')}</DialogTitle>
          <p className="text-sm text-muted-foreground">{t('dependencies.subtitle')}</p>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          {loading && <p className="text-xs text-muted-foreground">{t('dependencies.loading')}</p>}
          {!loading && candidates.length === 0 && (
            <p className="text-xs text-muted-foreground">{t('dependencies.noOtherPipelines')}</p>
          )}
          {!loading && candidates.length > 0 && (
            <div className="flex max-h-64 flex-col gap-1 overflow-y-auto rounded-md border border-white/10 p-2">
              {candidates.map((p) => (
                <label
                  key={p.pipeline_id}
                  className="flex items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-white/5"
                >
                  <input
                    type="checkbox"
                    checked={selected.has(p.pipeline_id)}
                    onChange={() => toggle(p.pipeline_id)}
                  />
                  <span className="font-mono text-xs">{p.pipeline_id}</span>
                </label>
              ))}
            </div>
          )}

          {selected.size > 1 && (
            <div>
              <div className="mb-1.5 text-xs font-medium text-muted-foreground">
                {t('dependencies.mode')}
              </div>
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => onChange(Array.from(selected), 'any')}
                  className={`rounded-md border px-3 py-1.5 text-xs ${
                    dependencyMode !== 'all'
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-white/10 text-muted-foreground'
                  }`}
                >
                  {t('dependencies.modeAny')}
                </button>
                <button
                  type="button"
                  onClick={() => onChange(Array.from(selected), 'all')}
                  className={`rounded-md border px-3 py-1.5 text-xs ${
                    dependencyMode === 'all'
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-white/10 text-muted-foreground'
                  }`}
                >
                  {t('dependencies.modeAll')}
                </button>
              </div>
              <p className="mt-1.5 text-[11px] text-muted-foreground">
                {dependencyMode === 'all'
                  ? t('dependencies.modeAllHint')
                  : t('dependencies.modeAnyHint')}
              </p>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
