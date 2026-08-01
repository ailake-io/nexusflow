import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { deletePipeline, type NodeSummary } from '@/lib/api'
import { useAuth } from '@/lib/auth'
import { usePipelines } from '@/hooks/usePipelines'

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
 * there's nothing here to leak by accident.
 */
export function PipelinesList() {
  const { token } = useAuth()
  const { pipelines, loading, error, refresh } = usePipelines()
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)

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

  return (
    <div className="h-full overflow-auto p-4">
      <h1 className="mb-3 text-lg font-medium">Pipelines</h1>
      {loading && <p className="text-sm text-muted-foreground">Loading…</p>}
      {error && <p className="text-sm text-destructive">{error}</p>}
      {deleteError && <p className="text-sm text-destructive">{deleteError}</p>}
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
              <span className="font-medium">{p.pipeline_id}</span>
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
