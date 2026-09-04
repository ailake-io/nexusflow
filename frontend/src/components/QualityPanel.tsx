import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, BadgeCheck, Loader2 } from 'lucide-react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { ValueType } from 'recharts/types/component/DefaultTooltipContent'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { usePipelines } from '@/hooks/usePipelines'
import {
  getDbtTestResults,
  getQualityCheckResults,
  listRuns,
  type DbtTestOutcome,
  type QualityCheckOutcome,
  type RunRecord,
} from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import { StatusBadge } from '@/components/ui/status-badge'

function totalRowsWritten(run: RunRecord): number {
  return (run.stats ?? []).reduce((sum, s) => sum + s.rows_written, 0)
}

function badgeVariant(status: string): 'success' | 'failed' | 'warning' {
  if (status === 'pass') return 'success'
  if (status === 'warn') return 'warning'
  return 'failed'
}

/** One test's history, oldest-first (matches the API's own ordering —
 * `dbt_test_result_store.rs`'s `list_for_pipeline`). */
interface TestGroup {
  uniqueId: string
  history: DbtTestOutcome[]
}

function groupByTest(results: DbtTestOutcome[]): TestGroup[] {
  const order: string[] = []
  const byId = new Map<string, DbtTestOutcome[]>()
  for (const r of results) {
    if (!byId.has(r.unique_id)) {
      byId.set(r.unique_id, [])
      order.push(r.unique_id)
    }
    byId.get(r.unique_id)!.push(r)
  }
  return order.map((uniqueId) => ({ uniqueId, history: byId.get(uniqueId)! }))
}

/** Same shape as `TestGroup`, one row per `column`+`check` pair — native
 * checks have no single "unique_id", so the pair is the natural key. */
interface QualityCheckGroup {
  key: string
  column: string
  check: string
  history: QualityCheckOutcome[]
}

function groupByCheck(results: QualityCheckOutcome[]): QualityCheckGroup[] {
  const order: string[] = []
  const byKey = new Map<string, QualityCheckOutcome[]>()
  for (const r of results) {
    const key = `${r.column}:${r.check}`
    if (!byKey.has(key)) {
      byKey.set(key, [])
      order.push(key)
    }
    byKey.get(key)!.push(r)
  }
  return order.map((key) => {
    const history = byKey.get(key)!
    return { key, column: history[0].column, check: history[0].check, history }
  })
}

/**
 * "Quality" tab: row-count trend + dbt test history for one saved pipeline
 * at a time. Both sources already exist — `listRuns` (rows written, from
 * `PartitionStats` already persisted per run) and `getDbtTestResults`
 * (individual test outcomes `dbt_test_result_store.rs` now exposes,
 * persisted since Fase 2 but unread until this tab). No new sampling, no
 * polling: both are historical, not live.
 */
export function QualityPanel() {
  const { token } = useAuth()
  const { t } = useI18n()
  const { pipelines, loading: pipelinesLoading } = usePipelines()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [runs, setRuns] = useState<RunRecord[]>([])
  const [testResults, setTestResults] = useState<DbtTestOutcome[]>([])
  const [qualityResults, setQualityResults] = useState<QualityCheckOutcome[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!selectedId && pipelines.length > 0) {
      setSelectedId(pipelines[0].pipeline_id)
    }
  }, [pipelines, selectedId])

  useEffect(() => {
    if (!token || !selectedId) return
    let cancelled = false
    setLoading(true)
    Promise.all([
      listRuns(token, selectedId),
      getDbtTestResults(token, selectedId),
      getQualityCheckResults(token, selectedId),
    ])
      .then(([runsResult, testsResult, qualityResult]) => {
        if (cancelled) return
        setRuns([...runsResult].reverse()) // API returns newest-first; chart wants oldest-first
        setTestResults(testsResult)
        setQualityResults(qualityResult)
        setError(null)
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : t('quality.error'))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, selectedId, t])

  const rowsChartData = useMemo(
    () =>
      runs
        .filter((r) => r.status === 'success')
        .map((r) => ({
          run: `#${r.id}`,
          rows: totalRowsWritten(r),
        })),
    [runs],
  )

  const testGroups = useMemo(() => groupByTest(testResults), [testResults])
  const qualityGroups = useMemo(() => groupByCheck(qualityResults), [qualityResults])

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">{t('quality.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('quality.subtitle')}</p>
        </div>
        {pipelines.length > 0 && (
          <select
            value={selectedId ?? ''}
            onChange={(e) => setSelectedId(e.target.value)}
            className="rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
          >
            {pipelines.map((p) => (
              <option key={p.pipeline_id} value={p.pipeline_id}>
                {p.pipeline_id}
              </option>
            ))}
          </select>
        )}
      </div>

      {!pipelinesLoading && pipelines.length === 0 && (
        <EmptyState
          icon={<BadgeCheck className="h-6 w-6" />}
          title={t('quality.noPipelines')}
          description={t('lineage.emptyDescription')}
        />
      )}

      {loading && (
        <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('quality.loading')}
        </div>
      )}

      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {!loading && !error && selectedId && (
        <div className="flex flex-col gap-6">
          <div className="rounded-lg border border-white/10 bg-card p-4">
            <div className="mb-2 text-xs font-medium text-muted-foreground">
              {t('quality.rowsWritten')}
            </div>
            {rowsChartData.length > 0 ? (
              <div className="h-48 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={rowsChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis dataKey="run" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} />
                    <Tooltip
                      contentStyle={{ fontSize: 12, background: '#111', border: '1px solid #333' }}
                      formatter={(value: ValueType | undefined) => [
                        `${Number(Array.isArray(value) ? value[0] : value).toLocaleString()}`,
                        t('quality.rowsWritten'),
                      ]}
                    />
                    <Area
                      type="monotone"
                      dataKey="rows"
                      stroke="#38bdf8"
                      fill="#38bdf8"
                      fillOpacity={0.2}
                      isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div className="flex h-24 items-center justify-center text-xs text-muted-foreground">
                {t('quality.runsTracked', { count: 0 })}
              </div>
            )}
          </div>

          <div className="rounded-lg border border-white/10 bg-card p-4">
            <div className="mb-3 text-xs font-medium text-muted-foreground">
              {t('quality.dbtTests')}
            </div>
            {testGroups.length === 0 ? (
              <div className="flex h-16 items-center text-xs text-muted-foreground">
                {t('quality.noDbtTests')}
              </div>
            ) : (
              <div className="flex flex-col gap-3">
                {testGroups.map((group) => {
                  const latest = group.history[group.history.length - 1]
                  return (
                    <div
                      key={group.uniqueId}
                      className="rounded-md border border-white/5 bg-white/[0.02] p-3"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <span className="font-mono text-xs text-foreground">{group.uniqueId}</span>
                        <div className="flex items-center gap-1.5">
                          {group.history.map((r, i) => (
                            <StatusBadge key={i} variant={badgeVariant(r.status)}>
                              {t(`quality.${r.status}`)}
                            </StatusBadge>
                          ))}
                        </div>
                      </div>
                      {latest.status !== 'pass' && latest.message && (
                        <p className="mt-2 text-xs text-red-400">
                          {t('quality.latestMessage')}: {latest.message}
                        </p>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>

          <div className="rounded-lg border border-white/10 bg-card p-4">
            <div className="mb-3 text-xs font-medium text-muted-foreground">
              {t('quality.nativeChecks')}
            </div>
            {qualityGroups.length === 0 ? (
              <div className="flex h-16 items-center text-xs text-muted-foreground">
                {t('quality.noNativeChecks')}
              </div>
            ) : (
              <div className="flex flex-col gap-3">
                {qualityGroups.map((group) => {
                  const latest = group.history[group.history.length - 1]
                  return (
                    <div
                      key={group.key}
                      className="rounded-md border border-white/5 bg-white/[0.02] p-3"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <span className="font-mono text-xs text-foreground">
                          {group.check} ({group.column})
                        </span>
                        <div className="flex items-center gap-1.5">
                          {group.history.map((r, i) => (
                            <StatusBadge key={i} variant={badgeVariant(r.status)}>
                              {t(`quality.${r.status}`)}
                            </StatusBadge>
                          ))}
                        </div>
                      </div>
                      {latest.status !== 'pass' && latest.message && (
                        <p className="mt-2 text-xs text-red-400">
                          {t('quality.latestMessage')}: {latest.message}
                        </p>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

export default QualityPanel
