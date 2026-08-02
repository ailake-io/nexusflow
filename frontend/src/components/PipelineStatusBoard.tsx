import { useEffect } from 'react'
import { usePipelines } from '@/hooks/usePipelines'
import type { PipelineSummary } from '@/lib/api'

const POLL_INTERVAL_MS = 5000

type FlagColor = 'green' | 'yellow' | 'red' | 'gray'

const FLAG_STYLE: Record<FlagColor, string> = {
  green: 'bg-green-500',
  yellow: 'bg-yellow-500 animate-pulse',
  red: 'bg-red-500',
  gray: 'bg-muted-foreground/40',
}

const FLAG_LABEL: Record<FlagColor, string> = {
  green: 'Sucesso',
  yellow: 'Em execução',
  red: 'Falha',
  gray: 'Agendado',
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

function Flag({ color }: { color: FlagColor }) {
  return (
    <span className="inline-flex items-center gap-1.5" title={FLAG_LABEL[color]}>
      <span className={`h-2.5 w-2.5 rounded-full ${FLAG_STYLE[color]}`} />
      <span className="text-xs text-muted-foreground">{FLAG_LABEL[color]}</span>
    </span>
  )
}

/**
 * Dashboard tab: every saved pipeline at a glance — one colored flag per
 * row (green/yellow/red/gray) instead of the detailed connector badges the
 * "Pipelines" management tab shows. Polls GET /pipelines every 5s so a
 * "running" flag flips to green/red without a manual refresh.
 */
export function PipelineStatusBoard() {
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

  return (
    <div className="h-full overflow-auto p-4">
      <div className="mb-3 flex items-center justify-between">
        <h1 className="text-lg font-medium">Status dos pipelines</h1>
        <div className="flex gap-3">
          {(['green', 'yellow', 'red', 'gray'] as const).map((color) => (
            <span key={color} className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <span className={`h-2.5 w-2.5 rounded-full ${FLAG_STYLE[color]}`} />
              {counts[color]} {FLAG_LABEL[color].toLowerCase()}
            </span>
          ))}
        </div>
      </div>
      {loading && pipelines.length === 0 && (
        <p className="text-sm text-muted-foreground">Loading…</p>
      )}
      {error && <p className="text-sm text-destructive">{error}</p>}
      {!loading && pipelines.length === 0 && (
        <p className="text-sm text-muted-foreground">Nenhum pipeline salvo ainda.</p>
      )}
      <div className="overflow-x-auto rounded-lg border">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 text-left text-xs text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Status</th>
              <th className="px-3 py-2 font-medium">pipeline_id</th>
              <th className="px-3 py-2 font-medium">schedule</th>
              <th className="px-3 py-2 font-medium">última execução</th>
            </tr>
          </thead>
          <tbody>
            {pipelines.map((p) => (
              <tr key={p.pipeline_id} className="border-t">
                <td className="px-3 py-2">
                  <Flag color={flagFor(p)} />
                </td>
                <td className="px-3 py-2 font-medium">{p.pipeline_id}</td>
                <td className="px-3 py-2 text-muted-foreground">
                  {p.schedule ?? <span className="italic">manual</span>}
                </td>
                <td className="px-3 py-2 text-muted-foreground">
                  {p.last_run_at ?? <span className="italic">nunca rodou</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
