import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, BarChart3, Loader2 } from 'lucide-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { usePipelines } from '@/hooks/usePipelines'
import { ApiError, previewNode, resolvedNodeName } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'

const LIMIT_OPTIONS = [50, 100, 200, 500]

/** `typeof value === 'number'` decides which columns can be a Y axis — the
 *  preview endpoint has no separate schema block (see `previewNode`'s doc
 *  comment), so column "type" is only ever known from the values
 *  themselves, same as any other client reading raw JSON rows. */
function inferColumns(rows: Record<string, unknown>[]): {
  allColumns: string[]
  numericColumns: string[]
} {
  const first = rows[0]
  if (!first) return { allColumns: [], numericColumns: [] }
  const allColumns = Object.keys(first)
  const numericColumns = allColumns.filter((c) => typeof first[c] === 'number')
  return { allColumns, numericColumns }
}

/**
 * "Prévia" tab: a quick sanity-check bar chart of what's already sitting in
 * a saved pipeline's sink — not BI (no aggregation across the whole table,
 * just the same sample `GET /pipelines/{id}/preview` already reads, capped
 * at `limit` rows). That endpoint existed already (reused by nothing in
 * the frontend until this tab) — this only adds the first UI for it.
 */
export function DataPreviewPanel() {
  const { token } = useAuth()
  const { t } = useI18n()
  const { pipelines, loading: pipelinesLoading } = usePipelines()
  const [selectedPipelineId, setSelectedPipelineId] = useState<string | null>(null)
  const [selectedSink, setSelectedSink] = useState<string | null>(null)
  const [limit, setLimit] = useState(LIMIT_OPTIONS[0])
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [xColumn, setXColumn] = useState<string | null>(null)
  const [yColumn, setYColumn] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [unsupported, setUnsupported] = useState(false)

  useEffect(() => {
    if (!selectedPipelineId && pipelines.length > 0) {
      setSelectedPipelineId(pipelines[0].pipeline_id)
    }
  }, [pipelines, selectedPipelineId])

  const selectedPipeline = pipelines.find((p) => p.pipeline_id === selectedPipelineId) ?? null
  const sinkNames = useMemo(
    () => (selectedPipeline?.sinks ?? []).map((s, i) => resolvedNodeName(s, i, 'sink')),
    [selectedPipeline],
  )

  useEffect(() => {
    setSelectedSink(sinkNames[0] ?? null)
  }, [sinkNames])

  useEffect(() => {
    if (!token || !selectedPipelineId || !selectedSink) {
      setRows([])
      return
    }
    let cancelled = false
    setLoading(true)
    setError(null)
    setUnsupported(false)
    previewNode(token, selectedPipelineId, selectedSink, limit)
      .then((data) => {
        if (cancelled) return
        setRows(data.rows)
        const { allColumns, numericColumns } = inferColumns(data.rows)
        setXColumn(allColumns.find((c) => !numericColumns.includes(c)) ?? allColumns[0] ?? null)
        setYColumn(numericColumns[0] ?? null)
      })
      .catch((err: unknown) => {
        if (cancelled) return
        if (err instanceof ApiError && err.status === 400) {
          setUnsupported(true)
        } else {
          setError(t('preview.error'))
        }
        setRows([])
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, selectedPipelineId, selectedSink, limit, t])

  const { allColumns, numericColumns } = useMemo(() => inferColumns(rows), [rows])

  const chartData = useMemo(() => {
    if (!xColumn || !yColumn) return []
    return rows.map((r, i) => ({
      x: String(r[xColumn] ?? i),
      y: typeof r[yColumn] === 'number' ? (r[yColumn] as number) : 0,
    }))
  }, [rows, xColumn, yColumn])

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">{t('preview.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('preview.subtitle')}</p>
        </div>
        {pipelines.length > 0 && (
          <div className="flex flex-wrap items-center gap-2">
            <select
              value={selectedPipelineId ?? ''}
              onChange={(e) => setSelectedPipelineId(e.target.value)}
              className="rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
            >
              {pipelines.map((p) => (
                <option key={p.pipeline_id} value={p.pipeline_id}>
                  {p.pipeline_id}
                </option>
              ))}
            </select>
            <select
              value={selectedSink ?? ''}
              onChange={(e) => setSelectedSink(e.target.value)}
              disabled={sinkNames.length === 0}
              className="rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none disabled:opacity-50"
            >
              {sinkNames.length === 0 && <option value="">{t('preview.noSinks')}</option>}
              {sinkNames.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
            >
              {LIMIT_OPTIONS.map((n) => (
                <option key={n} value={n}>
                  {t('preview.limitOption', { n })}
                </option>
              ))}
            </select>
          </div>
        )}
      </div>

      {!pipelinesLoading && pipelines.length === 0 && (
        <EmptyState
          icon={<BarChart3 className="h-6 w-6" />}
          title={t('preview.noPipelines')}
          description={t('preview.noPipelinesDescription')}
        />
      )}

      {loading && (
        <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('preview.loading')}
        </div>
      )}

      {!loading && error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {!loading && !error && unsupported && (
        <EmptyState
          icon={<AlertCircle className="h-6 w-6" />}
          title={t('preview.unsupportedTitle')}
          description={t('preview.unsupportedDescription')}
        />
      )}

      {!loading && !error && !unsupported && selectedSink && rows.length === 0 && (
        <EmptyState
          icon={<BarChart3 className="h-6 w-6" />}
          title={t('preview.emptyTitle')}
          description={t('preview.emptyDescription')}
        />
      )}

      {!loading && !error && !unsupported && rows.length > 0 && (
        <div className="flex flex-col gap-4">
          {numericColumns.length > 0 && xColumn && yColumn ? (
            <div className="rounded-lg border border-white/10 bg-card p-4">
              <div className="mb-3 flex flex-wrap items-center gap-2">
                <label className="text-xs text-muted-foreground">
                  {t('preview.xAxis')}
                  <select
                    value={xColumn}
                    onChange={(e) => setXColumn(e.target.value)}
                    className="ml-2 rounded-md border border-white/10 bg-background px-2 py-1 text-xs text-foreground focus:border-primary/40 focus:outline-none"
                  >
                    {allColumns.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="text-xs text-muted-foreground">
                  {t('preview.yAxis')}
                  <select
                    value={yColumn}
                    onChange={(e) => setYColumn(e.target.value)}
                    className="ml-2 rounded-md border border-white/10 bg-background px-2 py-1 text-xs text-foreground focus:border-primary/40 focus:outline-none"
                  >
                    {numericColumns.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis dataKey="x" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} />
                    <Tooltip
                      contentStyle={{ fontSize: 12, background: '#111', border: '1px solid #333' }}
                    />
                    <Bar dataKey="y" fill="#38bdf8" isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          ) : (
            <div className="rounded-lg border border-white/10 bg-card p-4 text-xs text-muted-foreground">
              {t('preview.noNumericColumn')}
            </div>
          )}

          <p className="text-[11px] text-muted-foreground">
            {t('preview.sampleNote', { count: rows.length })}
          </p>
        </div>
      )}
    </div>
  )
}

export default DataPreviewPanel
