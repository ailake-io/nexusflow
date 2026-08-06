import type { DbtRunSummary } from '@/lib/api'
import type { ExecutionStatus, PartitionProgress } from '@/hooks/useRunProgress'
import { StatusBadge } from '@/components/ui/status-badge'
import { AlertCircle, Terminal, Database, Clock } from 'lucide-react'

interface ExecutionPanelProps {
  status: ExecutionStatus
  runId: number | null
  partitions: Record<string, PartitionProgress>
  error: string | null
  dbtSummary: DbtRunSummary | null
}

const STATUS_CONFIG: Record<
  ExecutionStatus,
  { label: string; variant: 'idle' | 'running' | 'success' | 'failed' }
> = {
  idle: { label: 'Idle', variant: 'idle' },
  starting: { label: 'Starting…', variant: 'running' },
  running: { label: 'Running', variant: 'running' },
  success: { label: 'Success', variant: 'success' },
  failed: { label: 'Failed', variant: 'failed' },
}

/** Live execution panel (Marco 8 task #16) — consumes the progress
 * WebSocket from Marco 7 (task #9): rows/s and MB/s per partition/sink,
 * computed client-side from the cumulative counters each event carries. */
export function ExecutionPanel({
  status,
  runId,
  partitions,
  error,
  dbtSummary,
}: ExecutionPanelProps) {
  if (status === 'idle') return null

  const rows = Object.values(partitions)
  const statusConfig = STATUS_CONFIG[status]

  return (
    <div className="border-t border-white/10 bg-card p-4 animate-slide-in-up">
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <StatusBadge variant={statusConfig.variant} pulse={status === 'running' || status === 'starting'}>
          {statusConfig.label}
        </StatusBadge>
        {runId !== null && (
          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
            <Terminal className="h-3.5 w-3.5" />
            run #{runId}
          </span>
        )}
      </div>

      {rows.length > 0 && (
        <div className="overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-xs">
            <thead className="bg-muted/50 text-left uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-3 py-2 font-medium">Partition</th>
                <th className="px-3 py-2 font-medium">Rows</th>
                <th className="px-3 py-2 font-medium">Rows/s</th>
                <th className="px-3 py-2 font-medium">MB</th>
                <th className="px-3 py-2 font-medium">MB/s</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/5">
              {rows.map((p) => (
                <tr key={p.partition_id} className="hover:bg-white/[0.02]">
                  <td className="px-3 py-2 font-mono text-foreground">{p.partition_id}</td>
                  <td className="px-3 py-2">{p.rows_written.toLocaleString()}</td>
                  <td className="px-3 py-2">{p.rowsPerSecond.toFixed(0)}</td>
                  <td className="px-3 py-2">{(p.bytes_written / 1_000_000).toFixed(2)}</td>
                  <td className="px-3 py-2">{p.mbPerSecond.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {dbtSummary && (
        <div className="mt-3 rounded-lg border border-white/10 bg-white/[0.02] p-3">
          <div className="mb-2 flex items-center gap-2 text-xs font-medium text-foreground">
            <Database className="h-3.5 w-3.5 text-emerald-400" />
            dbt {dbtSummary.command}
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span>
              models: {dbtSummary.models_succeeded}/{dbtSummary.models_total}
              {dbtSummary.models_failed > 0 && (
                <span className="text-red-400"> ({dbtSummary.models_failed} failed)</span>
              )}
            </span>
            <span>
              tests: {dbtSummary.tests_passed}/{dbtSummary.tests_total}
              {dbtSummary.tests_failed > 0 && (
                <span className="text-red-400"> ({dbtSummary.tests_failed} failed)</span>
              )}
            </span>
            {dbtSummary.nodes_in_lineage !== null && (
              <span>lineage: {dbtSummary.nodes_in_lineage} nodes</span>
            )}
            <span className="inline-flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {dbtSummary.elapsed_time.toFixed(2)}s
            </span>
          </div>
        </div>
      )}

      {error && (
        <div className="mt-3 flex items-start gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          {error}
        </div>
      )}
    </div>
  )
}
