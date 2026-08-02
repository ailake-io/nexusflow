import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { deletePipeline, getPipelineSpec, type NodeSummary } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'
import { usePipelines } from '@/hooks/usePipelines'
import type { PipelineSpec } from '@/lib/dag'

const STATUS_STYLE: Record<string, string> = {
  success: 'border-green-600/40 bg-green-600/10 text-green-700 dark:text-green-400',
  failed: 'border-destructive/40 bg-destructive/10 text-destructive',
  running: 'border-blue-600/40 bg-blue-600/10 text-blue-700 dark:text-blue-400',
}

function StatusBadge({ status }: { status: 'running' | 'success' | 'failed' | null }) {
  if (!status) {
    return (
      <span className="rounded-full border bg-background px-2 py-0.5 text-xs text-muted-foreground">
        nunca rodou
      </span>
    )
  }
  return (
    <span className={`rounded-full border px-2 py-0.5 text-xs ${STATUS_STYLE[status]}`}>
      {status}
    </span>
  )
}

function NodeBadge({ node }: { node: NodeSummary }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full border bg-background px-2 py-0.5 text-xs">
      <span className="font-medium">{node.connector}</span>
      {node.name && <span className="text-muted-foreground">({node.name})</span>}
      {/* The config a node carries (where secrets like uri/password live)
       * is never returned by the API once persisted — nothing to render
       * here except this mask, there's no plaintext to accidentally leak. */}
      <span className="text-muted-foreground" title="connector config is never exposed once saved">
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
  const { token } = useAuth()
  const { pipelines, loading, error, refresh } = usePipelines()
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editError, setEditError] = useState<string | null>(null)

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
    <div className="h-full overflow-auto p-4">
      <h1 className="mb-3 text-lg font-medium">Pipelines</h1>
      {loading && <p className="text-sm text-muted-foreground">Loading…</p>}
      {error && <p className="text-sm text-destructive">{error}</p>}
      {deleteError && <p className="text-sm text-destructive">{deleteError}</p>}
      {editError && <p className="text-sm text-destructive">{editError}</p>}
      {!loading && pipelines.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No pipelines saved yet — create one from the canvas and Export JSON, then POST it to
          /pipelines.
        </p>
      )}
      <div className="flex flex-col gap-2">
        {pipelines.map((p) => (
          <div key={p.pipeline_id} className="rounded-lg border bg-card p-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="font-medium">{p.pipeline_id}</span>
                <StatusBadge status={p.last_run_status} />
                {p.schedule && (
                  <span
                    className="rounded-full border bg-background px-2 py-0.5 text-xs text-muted-foreground"
                    title="Roda automaticamente conforme este cron"
                  >
                    ⏰ {p.schedule}
                  </span>
                )}
              </div>
              <div className="flex gap-2">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={editingId === p.pipeline_id}
                  onClick={() => handleEdit(p.pipeline_id)}
                >
                  {editingId === p.pipeline_id ? 'Loading…' : 'Edit'}
                </Button>
                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  disabled={deletingId === p.pipeline_id}
                  onClick={() => handleDelete(p.pipeline_id)}
                >
                  {deletingId === p.pipeline_id ? 'Deleting…' : 'Delete'}
                </Button>
              </div>
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-1.5">
              {p.sources.map((n, i) => (
                <NodeBadge key={`source-${i}`} node={n} />
              ))}
              <span className="text-muted-foreground">→</span>
              {p.has_transform && (
                <span className="rounded-full border bg-background px-2 py-0.5 text-xs">
                  transform
                </span>
              )}
              {p.has_transform && <span className="text-muted-foreground">→</span>}
              {p.sinks.map((n, i) => (
                <NodeBadge key={`sink-${i}`} node={n} />
              ))}
            </div>
            <p className="mt-2 text-xs text-muted-foreground">
              •••• edited at {p.updated_at} (created {p.created_at})
            </p>
          </div>
        ))}
      </div>
    </div>
  )
}
