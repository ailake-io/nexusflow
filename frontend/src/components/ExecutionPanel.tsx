import { useState } from 'react'
import { useI18n } from '@/lib/i18n'
import type { DbtRunSummary, HardwareStats, RunLogEvent } from '@/lib/api'
import type { ExecutionStatus, PartitionProgress } from '@/hooks/useRunProgress'
import { StatusBadge } from '@/components/ui/status-badge'
import { LogTerminal } from '@/components/LogTerminal'
import { AlertCircle, Terminal, Database, Clock, Cpu, ChevronDown, ChevronUp } from 'lucide-react'

interface ExecutionPanelProps {
  status: ExecutionStatus
  runId: number | null
  partitions: Record<string, PartitionProgress>
  hardwareStats: HardwareStats | null
  error: string | null
  dbtSummary: DbtRunSummary | null
  logs: RunLogEvent[]
}

export function ExecutionPanel({
  status,
  runId,
  partitions,
  hardwareStats,
  error,
  dbtSummary,
  logs,
}: ExecutionPanelProps) {
  const { t } = useI18n()
  const [showLogs, setShowLogs] = useState(false)
  if (status === 'idle') return null

  const rows = Object.values(partitions)

  const statusConfig: Record<
    Exclude<ExecutionStatus, 'idle'>,
    { label: string; variant: 'running' | 'success' | 'failed' }
  > = {
    starting: { label: t('status.running') + '…', variant: 'running' },
    running: { label: t('status.running'), variant: 'running' },
    success: { label: t('status.success'), variant: 'success' },
    failed: { label: t('status.failed'), variant: 'failed' },
  }

  const config = statusConfig[status]

  return (
    <div className="border-t border-white/10 bg-card p-4 animate-slide-in-up">
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <StatusBadge variant={config.variant} pulse={status === 'running' || status === 'starting'}>
          {config.label}
        </StatusBadge>
        {runId !== null && (
          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
            <Terminal className="h-3.5 w-3.5" />
            {t('execution.run', { id: runId })}
          </span>
        )}
        {hardwareStats && (
          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
            <Cpu className="h-3.5 w-3.5" />
            {t('execution.cpu')} {hardwareStats.cpu_percent.toFixed(0)}% ·{' '}
            {t('execution.memory')}{' '}
            {(hardwareStats.memory_used_bytes / 1_000_000_000).toFixed(1)}/
            {(hardwareStats.memory_total_bytes / 1_000_000_000).toFixed(1)} GB
          </span>
        )}
        <button
          type="button"
          onClick={() => setShowLogs((v) => !v)}
          className="ml-auto inline-flex items-center gap-1 rounded-md border border-white/10 px-2 py-1 text-xs text-muted-foreground hover:bg-white/5"
        >
          <Terminal className="h-3.5 w-3.5" />
          {t('execution.logs.toggle')}
          {showLogs ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        </button>
      </div>

      {showLogs && (
        <div className="mb-3">
          <LogTerminal logs={logs} />
        </div>
      )}

      {rows.length > 0 && (
        <div className="overflow-hidden rounded-lg border border-white/10">
          <table className="w-full text-xs">
            <thead className="bg-muted/50 text-left uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-3 py-2 font-medium">{t('execution.partition')}</th>
                <th className="px-3 py-2 font-medium">{t('execution.rows')}</th>
                <th className="px-3 py-2 font-medium">{t('execution.rowsPerSecond')}</th>
                <th className="px-3 py-2 font-medium">{t('execution.mb')}</th>
                <th className="px-3 py-2 font-medium">{t('execution.mbPerSecond')}</th>
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
            {t('execution.dbtCommand', { command: dbtSummary.command })}
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span>
              {t('execution.models', {
                succeeded: dbtSummary.models_succeeded,
                total: dbtSummary.models_total,
              })}
              {dbtSummary.models_failed > 0 && (
                <span className="text-red-400">
                  {t('execution.modelsFailed', { failed: dbtSummary.models_failed })}
                </span>
              )}
            </span>
            <span>
              {t('execution.tests', {
                passed: dbtSummary.tests_passed,
                total: dbtSummary.tests_total,
              })}
              {dbtSummary.tests_failed > 0 && (
                <span className="text-red-400">
                  {t('execution.testsFailed', { failed: dbtSummary.tests_failed })}
                </span>
              )}
            </span>
            {dbtSummary.nodes_in_lineage !== null && (
              <span>{t('execution.lineage', { count: dbtSummary.nodes_in_lineage })}</span>
            )}
            <span className="inline-flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {t('execution.elapsed', { seconds: dbtSummary.elapsed_time.toFixed(2) })}
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
