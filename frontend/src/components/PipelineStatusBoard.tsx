import { useEffect } from 'react'
import {
  Activity,
  CheckCircle2,
  XCircle,
  Clock,
  CalendarClock,
  Loader2,
  AlertCircle,
} from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import { usePipelines } from '@/hooks/usePipelines'
import { StatusBadge } from '@/components/ui/status-badge'
import { EmptyState } from '@/components/EmptyState'
import type { PipelineSummary } from '@/lib/api'

const POLL_INTERVAL_MS = 5000

type FlagColor = 'green' | 'yellow' | 'red' | 'gray'

const FLAG_CONFIG: Record<
  FlagColor,
  { variant: 'success' | 'running' | 'failed' | 'idle'; icon: typeof CheckCircle2 }
> = {
  green: { variant: 'success', icon: CheckCircle2 },
  yellow: { variant: 'running', icon: Clock },
  red: { variant: 'failed', icon: XCircle },
  gray: { variant: 'idle', icon: CalendarClock },
}

/** One pipeline's flag reflects its last known outcome, not just whether it
 * has a schedule — a pipeline that already ran fine stays green even while
 * idle between scheduled ticks. Gray is only for "no result yet": either it
 * has never run, or (for a scheduled one) it's waiting on its first tick. */
function flagFor(p: PipelineSummary): FlagColor {
  if (p.last_run_status === 'running') return 'yellow'
  if (p.last_run_status === 'success') return 'green'
  if (p.last_run_status === 'failed') return 'red'
  return 'gray'
}

/**
 * Dashboard tab: every saved pipeline at a glance — one colored flag per
 * row (green/yellow/red/gray) instead of the detailed connector badges the
 * "Pipelines" management tab shows. Polls GET /pipelines every 5s so a
 * "running" flag flips to green/red without a manual refresh.
 */
export function PipelineStatusBoard() {
  const { t } = useI18n()
  const { pipelines, loading, error, refresh } = usePipelines()

  useEffect(() => {
    const id = setInterval(refresh, POLL_INTERVAL_MS)
    return () => clearInterval(id)
  }, [refresh])

  const counts = pipelines.reduce(
    (acc, p) => {
      acc[flagFor(p)] += 1
      return acc
    },
    { green: 0, yellow: 0, red: 0, gray: 0 } as Record<FlagColor, number>,
  )

  const flagOrder: FlagColor[] = ['green', 'yellow', 'red', 'gray']

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">{t('status.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('status.subtitle')}</p>
        </div>
        <div className="flex flex-wrap gap-2">
          {flagOrder.map((color) => {
            const config = FLAG_CONFIG[color]
            const Icon = config.icon
            const labelKey: 'success' | 'running' | 'failed' | 'scheduled' =
              color === 'green' ? 'success' : color === 'yellow' ? 'running' : color === 'red' ? 'failed' : 'scheduled'
            return (
              <div
                key={color}
                className="flex items-center gap-2 rounded-lg border border-white/10 bg-card px-3 py-1.5 text-xs"
              >
                <Icon className="h-3.5 w-3.5 text-muted-foreground" />
                <span className="font-medium text-foreground">{counts[color]}</span>
                <span className="text-muted-foreground">{t(`status.${labelKey}`).toLowerCase()}</span>
              </div>
            )
          })}
        </div>
      </div>

      {loading && pipelines.length === 0 && (
        <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('status.loading')}
        </div>
      )}

      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {!loading && pipelines.length === 0 && (
        <EmptyState
          icon={<Activity className="h-6 w-6" />}
          title={t('status.emptyTitle')}
          description={t('status.emptyDescription')}
        />
      )}

      {pipelines.length > 0 && (
        <div className="overflow-hidden rounded-xl border border-white/10 bg-card">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-left text-xs uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-4 py-3 font-medium">{t('status.status')}</th>
                <th className="px-4 py-3 font-medium">{t('status.pipeline')}</th>
                <th className="px-4 py-3 font-medium">{t('status.schedule')}</th>
                <th className="px-4 py-3 font-medium">{t('status.lastRun')}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {pipelines.map((p) => {
                const color = flagFor(p)
                const config = FLAG_CONFIG[color]
                const labelKey: 'success' | 'running' | 'failed' | 'scheduled' =
                  color === 'green'
                    ? 'success'
                    : color === 'yellow'
                      ? 'running'
                      : color === 'red'
                        ? 'failed'
                        : 'scheduled'
                return (
                  <tr key={p.pipeline_id} className="transition-colors hover:bg-white/[0.02]">
                    <td className="px-4 py-3">
                      <StatusBadge variant={config.variant} pulse={color === 'yellow'}>
                        {t(`status.${labelKey}`)}
                      </StatusBadge>
                    </td>
                    <td className="px-4 py-3 font-medium text-foreground">{p.pipeline_id}</td>
                    <td className="px-4 py-3 text-muted-foreground">
                      {p.schedule ?? <span className="italic opacity-60">{t('status.manual')}</span>}
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">
                      {p.last_run_at ?? <span className="italic opacity-60">{t('status.neverRun')}</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default PipelineStatusBoard
