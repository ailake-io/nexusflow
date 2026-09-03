import { useState } from 'react'
import {
  AlertCircle,
  ChevronDown,
  ChevronUp,
  Loader2,
  RefreshCw,
  Terminal,
  Trash2,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { StatusBadge } from '@/components/ui/status-badge'
import { useI18n } from '@/lib/i18n'
import { formatDuration } from '@/lib/utils'
import { useRunHistory } from '@/hooks/useRunHistory'
import { useRunLogs } from '@/hooks/useRunLogs'
import { LogTerminal } from '@/components/LogTerminal'
import { deleteRun, type PartitionStats, type RunRecord } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'

function totalRowsWritten(stats: PartitionStats[] | null): number | null {
  if (!stats) return null
  return stats.reduce((sum, s) => sum + s.rows_written, 0)
}

function statusVariant(status: RunRecord['status']) {
  if (status === 'running') return 'running'
  if (status === 'success') return 'success'
  return 'failed'
}

function RunRow({
  run,
  pipelineId,
  onDeleted,
}: {
  run: RunRecord
  pipelineId: string
  onDeleted: () => void
}) {
  const { t } = useI18n()
  const { token } = useAuth()
  const duration = formatDuration(run.started_at, run.finished_at)
  const rows = totalRowsWritten(run.stats)
  const [showLogs, setShowLogs] = useState(false)
  const { logs, loading: logsLoading, fetchLogs } = useRunLogs()
  const [confirmingDelete, setConfirmingDelete] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)

  const toggleLogs = () => {
    const next = !showLogs
    setShowLogs(next)
    if (next) fetchLogs(pipelineId, run.id)
  }

  // Same disallow-while-running the backend enforces (409) — kept here too
  // so the button doesn't invite a click that's just going to error.
  const canDelete = run.status !== 'running'

  const handleDelete = async () => {
    if (!token) return
    setDeleting(true)
    setDeleteError(null)
    try {
      await deleteRun(token, pipelineId, run.id)
      onDeleted()
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : String(err))
      setDeleting(false)
    }
  }

  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.02] p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium text-foreground">
            {t('pipelines.history.run', { id: run.id })}
          </span>
          <StatusBadge variant={statusVariant(run.status)} pulse={run.status === 'running'}>
            {t(`status.${run.status}` as 'status.running' | 'status.success' | 'status.failed')}
          </StatusBadge>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-foreground">
            {duration ?? t('pipelines.history.running')}
          </span>
          {canDelete && (
            <button
              type="button"
              onClick={() => setConfirmingDelete(true)}
              className="text-muted-foreground hover:text-red-400"
              title={t('pipelines.history.delete')}
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted-foreground">
        <span>{t('pipelines.history.startedAt', { started: new Date(run.started_at).toLocaleString() })}</span>
        {rows !== null && <span>{t('pipelines.history.rowsWritten', { rows })}</span>}
        {run.dbt_summary && (
          <span>
            {t('pipelines.history.dbtSummary', {
              succeeded: run.dbt_summary.models_succeeded,
              total: run.dbt_summary.models_total,
            })}
          </span>
        )}
        <button
          type="button"
          onClick={toggleLogs}
          className="ml-auto inline-flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
        >
          <Terminal className="h-3 w-3" />
          {t('execution.logs.toggle')}
          {showLogs ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        </button>
      </div>

      {confirmingDelete && (
        <div className="mt-2 rounded-lg border border-red-500/20 bg-red-500/10 p-2">
          <p className="mb-2 text-[10px] text-red-200">
            {t('pipelines.history.deleteConfirm', { id: run.id })}
          </p>
          <div className="flex gap-2">
            <Button
              type="button"
              variant="destructive"
              size="sm"
              disabled={deleting}
              onClick={handleDelete}
              className="gap-1.5"
            >
              {deleting ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : (
                <Trash2 className="h-3 w-3" />
              )}
              {t('pipelines.history.confirmDelete')}
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => setConfirmingDelete(false)}
              disabled={deleting}
            >
              {t('pipelines.history.cancelDelete')}
            </Button>
          </div>
          {deleteError && <p className="mt-1.5 text-[10px] text-red-300">{deleteError}</p>}
        </div>
      )}

      {showLogs && (
        <div className="mt-2">
          {logsLoading && logs.length === 0 ? (
            <div className="flex h-10 items-center justify-center text-[10px] text-muted-foreground">
              <Loader2 className="mr-2 h-3 w-3 animate-spin" />
              {t('pipelines.history.loading')}
            </div>
          ) : (
            <LogTerminal logs={logs} autoScroll={false} />
          )}
        </div>
      )}

      {run.error && (
        <p className="mt-2 rounded border border-red-500/20 bg-red-500/10 p-2 text-[10px] text-red-300">
          {run.error}
        </p>
      )}
    </div>
  )
}

interface RunHistoryPanelProps {
  pipelineId: string
}

/** Expanded inline panel (toggled by a "Histórico" button in PipelinesList,
 * same accordion idiom as that screen's delete-confirmation) — lists every
 * run of one pipeline with computed duration, since neither existing
 * pipeline screen (list or status board) shows more than the latest run. */
export function RunHistoryPanel({ pipelineId }: RunHistoryPanelProps) {
  const { t } = useI18n()
  const { runs, loading, error, refresh } = useRunHistory(pipelineId)

  const sorted = [...runs].sort((a, b) => b.id - a.id)

  return (
    <div className="mt-3 rounded-lg border border-white/10 bg-black/10 p-3">
      <div className="mb-2 flex items-center justify-between">
        <span className="text-xs font-semibold text-foreground">{t('pipelines.history.title')}</span>
        <Button type="button" variant="outline" size="sm" onClick={refresh} className="gap-1.5">
          <RefreshCw className="h-3 w-3" />
          {t('pipelines.history.refresh')}
        </Button>
      </div>

      {loading && sorted.length === 0 && (
        <div className="flex h-16 items-center justify-center text-xs text-muted-foreground">
          <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />
          {t('pipelines.history.loading')}
        </div>
      )}

      {error && (
        <div className="mb-2 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-2 text-xs text-red-400">
          <AlertCircle className="h-3.5 w-3.5" />
          {error}
        </div>
      )}

      {!loading && sorted.length === 0 && !error && (
        <p className="py-4 text-center text-xs text-muted-foreground">{t('pipelines.history.empty')}</p>
      )}

      <div className="flex flex-col gap-2">
        {sorted.map((run) => (
          <RunRow key={run.id} run={run} pipelineId={pipelineId} onDeleted={refresh} />
        ))}
      </div>
    </div>
  )
}

export default RunHistoryPanel
