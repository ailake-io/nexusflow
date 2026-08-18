import { useState } from 'react'
import {
  Edit2,
  Trash2,
  Clock,
  History,
  ArrowRight,
  Database,
  Workflow,
  AlertCircle,
  Loader2,
  Play,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { StatusBadge } from '@/components/ui/status-badge'
import { EmptyState } from '@/components/EmptyState'
import { RunHistoryPanel } from '@/components/RunHistoryPanel'
import { useI18n } from '@/lib/i18n'
import { deletePipeline, getPipelineSpec, runPipeline, type NodeSummary } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'
import { usePipelines } from '@/hooks/usePipelines'
import type { PipelineSpec } from '@/lib/dag'

function statusVariant(status: 'running' | 'success' | 'failed' | null) {
  if (status === 'running') return 'running'
  if (status === 'success') return 'success'
  if (status === 'failed') return 'failed'
  return 'idle'
}

function NodeBadge({ node }: { node: NodeSummary }) {
  const { t } = useI18n()
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.03] px-2 py-0.5 text-xs">
      <Database className="h-3 w-3 text-muted-foreground" />
      <span className="font-medium text-foreground">{node.connector}</span>
      {node.name && <span className="text-muted-foreground">({node.name})</span>}
      {/* The config a node carries (where secrets like uri/password live)
       * is never returned by the API once persisted — nothing to render
       * here except this mask, there's no plaintext to accidentally leak. */}
      <span
        className="text-muted-foreground"
        title={t('pipelines.configMaskHint')}
      >
        ••••
      </span>
    </span>
  )
}

/**
 * Masked credentials screen (Marco 8 task #17): lists persisted pipelines
 * without ever rendering a connector's config in plain text. The masking
 * isn't a frontend rendering trick — GET /pipelines and GET /pipelines/{id}
 * never include the config field server-side (pipeline_store.rs), so
 * there's nothing here to leak by accident. Edit is the one deliberate
 * exception: it calls GET /pipelines/{id}/spec (Write-role only) to reload
 * the real config onto the canvas, same trust level as creating a pipeline.
 */
interface PipelinesListProps {
  onEdit: (spec: PipelineSpec) => void
}

export function PipelinesList({ onEdit }: PipelinesListProps) {
  const { t } = useI18n()
  const { token } = useAuth()
  const { pipelines, loading, error, refresh } = usePipelines()
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editError, setEditError] = useState<string | null>(null)
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null)
  const [historyId, setHistoryId] = useState<string | null>(null)
  const [runningId, setRunningId] = useState<string | null>(null)
  const [runError, setRunError] = useState<string | null>(null)

  // Triggers a run straight from the list, without loading the pipeline
  // onto the Canvas first. `POST /pipelines/{id}/run` uses the persisted
  // spec whenever one exists for `pipeline_id` (see run_pipeline_handler)
  // — the body only needs to carry the id, sources/sinks are ignored.
  const handleRun = async (pipelineId: string) => {
    if (!token) return
    setRunningId(pipelineId)
    setRunError(null)
    try {
      // PipelineSpec.validate() (called on the raw request body, before the
      // handler swaps in the persisted spec — see run_pipeline_handler)
      // rejects empty sources/sinks unconditionally, so a bare
      // `{pipeline_id}` placeholder body doesn't work here the way it does
      // for an already-loaded Canvas run. Fetch the real spec first, same
      // call `handleEdit` uses.
      const spec = await getPipelineSpec(token, pipelineId)
      await runPipeline(token, spec)
      refresh()
    } catch (err) {
      setRunError(err instanceof Error ? err.message : String(err))
    } finally {
      setRunningId(null)
    }
  }

  const handleDelete = async (pipelineId: string) => {
    if (!token) return
    setDeletingId(pipelineId)
    setDeleteError(null)
    try {
      await deletePipeline(token, pipelineId)
      refresh()
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : String(err))
    } finally {
      setDeletingId(null)
      setConfirmDeleteId(null)
    }
  }

  const handleEdit = async (pipelineId: string) => {
    if (!token) return
    setEditingId(pipelineId)
    setEditError(null)
    try {
      const spec = await getPipelineSpec(token, pipelineId)
      onEdit(spec)
    } catch (err) {
      setEditError(err instanceof Error ? err.message : String(err))
    } finally {
      setEditingId(null)
    }
  }

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">{t('pipelines.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('pipelines.subtitle')}</p>
        </div>
      </div>

      {loading && pipelines.length === 0 && (
        <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('pipelines.loading')}
        </div>
      )}

      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}
      {deleteError && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {deleteError}
        </div>
      )}
      {editError && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {editError}
        </div>
      )}
      {runError && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {runError}
        </div>
      )}

      {!loading && pipelines.length === 0 && (
        <EmptyState
          icon={<Workflow className="h-6 w-6" />}
          title={t('pipelines.emptyTitle')}
          description={t('pipelines.emptyDescription')}
        />
      )}

      <div className="flex flex-col gap-3">
        {pipelines.map((p) => (
          <div
            key={p.pipeline_id}
            className="group rounded-xl border border-white/10 bg-card p-4 transition-all hover:border-white/15 hover:shadow-sm"
          >
            <div className="flex items-start justify-between gap-4">
              <div className="flex flex-wrap items-center gap-2">
                <span className="font-semibold text-foreground">{p.pipeline_id}</span>
                <StatusBadge
                  variant={statusVariant(p.last_run_status)}
                  pulse={p.last_run_status === 'running'}
                >
                  {p.last_run_status
                    ? t(`status.${p.last_run_status}` as 'status.running' | 'status.success' | 'status.failed')
                    : t('pipelines.neverRun')}
                </StatusBadge>
                {p.schedule && (
                  <span
                    className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/[0.03] px-2 py-0.5 text-xs text-muted-foreground"
                    title={t('pipelines.runsOn', { schedule: p.schedule })}
                  >
                    <Clock className="h-3 w-3" />
                    {p.schedule}
                  </span>
                )}
              </div>
              <div className="flex gap-2">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={runningId === p.pipeline_id}
                  onClick={() => handleRun(p.pipeline_id)}
                  className="gap-1.5"
                >
                  {runningId === p.pipeline_id ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Play className="h-3.5 w-3.5" />
                  )}
                  {runningId === p.pipeline_id ? t('pipelines.running') : t('pipelines.run')}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    setHistoryId(historyId === p.pipeline_id ? null : p.pipeline_id)
                  }
                  className="gap-1.5"
                >
                  <History className="h-3.5 w-3.5" />
                  {t('pipelines.history.toggle')}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={editingId === p.pipeline_id}
                  onClick={() => handleEdit(p.pipeline_id)}
                  className="gap-1.5"
                >
                  {editingId === p.pipeline_id ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Edit2 className="h-3.5 w-3.5" />
                  )}
                  {editingId === p.pipeline_id ? t('pipelines.editing') : t('pipelines.edit')}
                </Button>
                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  disabled={deletingId === p.pipeline_id}
                  onClick={() => setConfirmDeleteId(p.pipeline_id)}
                  className="gap-1.5"
                >
                  {deletingId === p.pipeline_id ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Trash2 className="h-3.5 w-3.5" />
                  )}
                  {deletingId === p.pipeline_id ? t('pipelines.deleting') : t('pipelines.delete')}
                </Button>
              </div>
            </div>

            <div className="mt-3 flex flex-wrap items-center gap-1.5">
              {p.sources.map((n, i) => (
                <NodeBadge key={`source-${i}`} node={n} />
              ))}
              <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />
              {p.has_transform && (
                <span className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/[0.03] px-2 py-0.5 text-xs text-foreground">
                  <Workflow className="h-3 w-3 text-muted-foreground" />
                  {t('pipelines.transform')}
                </span>
              )}
              {p.has_transform && <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />}
              {p.sinks.map((n, i) => (
                <NodeBadge key={`sink-${i}`} node={n} />
              ))}
            </div>

            {confirmDeleteId === p.pipeline_id && (
              <div className="mt-3 rounded-lg border border-red-500/20 bg-red-500/10 p-3">
                <p className="mb-2 text-xs text-red-200">
                  {t('pipelines.deleteConfirm', { id: p.pipeline_id })}
                </p>
                <div className="flex gap-2">
                  <Button
                    type="button"
                    variant="destructive"
                    size="sm"
                    disabled={deletingId === p.pipeline_id}
                    onClick={() => handleDelete(p.pipeline_id)}
                    className="gap-1.5"
                  >
                    {deletingId === p.pipeline_id ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Trash2 className="h-3.5 w-3.5" />
                    )}
                    {t('pipelines.confirmDelete')}
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => setConfirmDeleteId(null)}
                    disabled={deletingId === p.pipeline_id}
                  >
                    {t('pipelines.cancelDelete')}
                  </Button>
                </div>
              </div>
            )}

            <p className="mt-3 text-[10px] text-muted-foreground">
              {t('pipelines.updatedAt', { updated: p.updated_at })} ·{' '}
              {t('pipelines.createdAt', { created: p.created_at })}
            </p>

            {historyId === p.pipeline_id && <RunHistoryPanel pipelineId={p.pipeline_id} />}
          </div>
        ))}
      </div>
    </div>
  )
}

export default PipelinesList
