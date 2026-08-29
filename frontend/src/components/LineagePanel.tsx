import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, Loader2, Waypoints, X } from 'lucide-react'
import {
  Background,
  ReactFlow,
  ReactFlowProvider,
  type Edge,
  type Node,
  type NodeMouseHandler,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import {
  ApiError,
  getLineage,
  getPipelineSchema,
  type LineageEdge,
  type LineageGraph,
  type LineageNode,
  type PipelineSchema,
} from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import {
  LineagePipelineNodeView,
  LineageResourceNodeView,
  LineageDbtNodeView,
  type LineagePipelineNodeData,
  type LineageResourceNodeData,
  type LineageDbtNodeData,
} from '@/components/lineage-nodes'

const lineageNodeTypes = {
  pipeline: LineagePipelineNodeView,
  resource: LineageResourceNodeView,
  dbt_node: LineageDbtNodeView,
}

const RANK_GAP = 260
const ROW_GAP = 90

/**
 * Layered left-to-right layout by longest-path rank (Kahn's algorithm,
 * peeling zero-in-degree nodes level by level). Saved specs carry no x/y
 * for lineage purposes (that only exists for the editable Canvas), so this
 * is needed to render anything sensible. No new dependency (dagre/elkjs) —
 * pipeline counts are typically small; revisit if this looks bad in
 * practice with real data.
 */
function computeLayout(
  nodes: LineageNode[],
  edges: LineageEdge[],
): Record<string, { x: number; y: number }> {
  const outgoing = new Map<string, string[]>()
  const inDegree = new Map<string, number>()
  for (const n of nodes) {
    inDegree.set(n.id, 0)
    outgoing.set(n.id, [])
  }
  for (const e of edges) {
    outgoing.get(e.from)?.push(e.to)
    inDegree.set(e.to, (inDegree.get(e.to) ?? 0) + 1)
  }

  const remaining = new Set(nodes.map((n) => n.id))
  const localInDegree = new Map(inDegree)
  const rank = new Map<string, number>()
  let frontier = nodes.filter((n) => (inDegree.get(n.id) ?? 0) === 0).map((n) => n.id)
  let level = 0

  while (remaining.size > 0) {
    if (frontier.length === 0) {
      // A cycle (or leftover disconnected nodes) with no zero-in-degree
      // candidate left — dump everything still remaining into this level
      // so layout terminates instead of looping forever.
      frontier = Array.from(remaining)
    }
    for (const id of frontier) {
      if (!remaining.has(id)) continue
      rank.set(id, level)
      remaining.delete(id)
    }
    const next = new Set<string>()
    for (const id of frontier) {
      for (const target of outgoing.get(id) ?? []) {
        if (!remaining.has(target)) continue
        const d = (localInDegree.get(target) ?? 1) - 1
        localInDegree.set(target, d)
        if (d <= 0) next.add(target)
      }
    }
    frontier = Array.from(next)
    level += 1
  }

  const byRank = new Map<number, string[]>()
  for (const [id, r] of rank.entries()) {
    if (!byRank.has(r)) byRank.set(r, [])
    byRank.get(r)!.push(id)
  }

  const incoming = new Map<string, string[]>()
  for (const e of edges) {
    if (!incoming.has(e.to)) incoming.set(e.to, [])
    incoming.get(e.to)!.push(e.from)
  }

  // Two passes per rank: a node with exactly one parent in the previous
  // rank inherits that parent's row (keeps a straight chain like
  // pipeline -> model A -> model B visually level instead of every rank
  // restarting its row count at 0, which put an unrelated node from an
  // earlier rank in the same row as a later, taller chain — a real
  // collision found reviewing this in a real browser, not theoretical).
  // Anything left over (multiple parents, no parent, or its parent's row
  // is already taken) gets the next free row in this rank.
  const positions: Record<string, { x: number; y: number }> = {}
  const ranksAscending = Array.from(byRank.keys()).sort((a, b) => a - b)
  for (const r of ranksAscending) {
    const ids = byRank.get(r)!
    const usedRows = new Set<number>()
    const pending: string[] = []

    for (const id of ids) {
      const parents = (incoming.get(id) ?? []).filter((p) => rank.get(p) === r - 1)
      const parentPosition = parents.length === 1 ? positions[parents[0]] : undefined
      const parentRow = parentPosition ? parentPosition.y / ROW_GAP : null
      if (parentRow !== null && !usedRows.has(parentRow)) {
        positions[id] = { x: r * RANK_GAP, y: parentRow * ROW_GAP }
        usedRows.add(parentRow)
      } else {
        pending.push(id)
      }
    }

    let cursor = 0
    for (const id of pending) {
      while (usedRows.has(cursor)) cursor += 1
      positions[id] = { x: r * RANK_GAP, y: cursor * ROW_GAP }
      usedRows.add(cursor)
      cursor += 1
    }
  }
  return positions
}

function toFlowElements(graph: LineageGraph): { nodes: Node[]; edges: Edge[] } {
  const positions = computeLayout(graph.nodes, graph.edges)
  const nodes: Node[] = graph.nodes.map((n) => {
    const position = positions[n.id] ?? { x: 0, y: 0 }
    if (n.kind === 'pipeline') {
      const data: LineagePipelineNodeData = {
        kind: 'pipeline',
        label: n.label,
        hasSchedule: n.has_schedule,
      }
      return { id: n.id, type: 'pipeline', position, data }
    }
    if (n.kind === 'resource') {
      const data: LineageResourceNodeData = {
        kind: 'resource',
        label: n.label,
        connector: n.connector,
        resourceKind: n.resource_kind,
      }
      return { id: n.id, type: 'resource', position, data }
    }
    const data: LineageDbtNodeData = {
      kind: 'dbt_node',
      label: n.label,
      resourceType: n.resource_type,
    }
    return { id: n.id, type: 'dbt_node', position, data }
  })
  const edges: Edge[] = graph.edges.map((e) => ({
    id: `${e.from}->${e.to}`,
    source: e.from,
    target: e.to,
    animated: false,
    style: { stroke: 'oklch(1 0 0 / 20%)' },
  }))
  return { nodes, edges }
}

function ColumnList({ columns }: { columns: PipelineSchema['source_columns'] }) {
  if (columns.length === 0) {
    return <p className="text-xs text-muted-foreground">—</p>
  }
  return (
    <ul className="flex flex-col gap-1">
      {columns.map((c) => (
        <li
          key={c.name}
          className="flex items-baseline justify-between gap-3 font-mono text-xs text-foreground"
        >
          <span className="truncate">{c.name}</span>
          <span className="shrink-0 text-muted-foreground">{c.data_type}</span>
        </li>
      ))}
    </ul>
  )
}

/** Fetched and shown only on demand — clicking a pipeline node in the
 *  Lineage tab, per the "not up front" scope: nothing is fetched or
 *  rendered here until `pipelineId` is set. */
function PipelineSchemaPanel({ pipelineId, onClose }: { pipelineId: string; onClose: () => void }) {
  const { token } = useAuth()
  const { t } = useI18n()
  const [schema, setSchema] = useState<PipelineSchema | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [notRun, setNotRun] = useState(false)

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    setSchema(null)
    setError(null)
    setNotRun(false)
    getPipelineSchema(token, pipelineId)
      .then((data) => {
        if (!cancelled) setSchema(data)
      })
      .catch((err) => {
        if (cancelled) return
        if (err instanceof ApiError && err.status === 404) {
          setNotRun(true)
        } else {
          setError(t('lineage.schema.error'))
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, pipelineId, t])

  return (
    <div className="absolute right-0 top-0 z-10 flex h-full w-80 flex-col overflow-hidden border-l border-white/10 bg-card shadow-xl">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold text-foreground">{t('lineage.schema.title')}</h2>
          <p className="truncate font-mono text-[11px] text-muted-foreground">{pipelineId}</p>
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label={t('lineage.schema.close')}
          className="rounded-md p-1.5 text-muted-foreground hover:bg-white/5 hover:text-foreground"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="flex-1 overflow-auto p-4">
        {loading && (
          <div className="flex items-center justify-center py-8 text-xs text-muted-foreground">
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            {t('lineage.schema.loading')}
          </div>
        )}

        {!loading && notRun && (
          <p className="text-xs text-muted-foreground">{t('lineage.schema.neverRun')}</p>
        )}

        {!loading && error && (
          <div className="flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
            <AlertCircle className="h-4 w-4" />
            {error}
          </div>
        )}

        {!loading && schema && (
          <div className="flex flex-col gap-5">
            <div>
              <div className="mb-2 text-xs font-medium text-muted-foreground">
                {t('lineage.schema.sourceColumns')}
              </div>
              <ColumnList columns={schema.source_columns} />
            </div>

            <div>
              <div className="mb-2 text-xs font-medium text-muted-foreground">
                {t('lineage.schema.outputColumns')}
              </div>
              <ColumnList columns={schema.output_columns} />
            </div>

            {schema.column_lineage && (
              <div>
                <div className="mb-2 text-xs font-medium text-muted-foreground">
                  {t('lineage.schema.columnLineage')}
                </div>
                <ul className="flex flex-col gap-1.5">
                  {schema.column_lineage.map((l) => (
                    <li key={l.output_column} className="font-mono text-xs text-foreground">
                      <span className="text-primary">{l.output_column}</span>{' '}
                      <span className="text-muted-foreground">{t('lineage.schema.from')}</span>{' '}
                      {l.source_columns ? (
                        l.source_columns.join(', ')
                      ) : (
                        <span className="italic text-muted-foreground">
                          {t('lineage.schema.notDetermined')}
                        </span>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function LineageGraphView() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [graph, setGraph] = useState<LineageGraph | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedPipelineId, setSelectedPipelineId] = useState<string | null>(null)

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    getLineage(token)
      .then((data) => {
        if (!cancelled) {
          setGraph(data)
          setError(null)
        }
      })
      .catch(() => {
        if (!cancelled) setError(t('lineage.error'))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, t])

  const { nodes, edges } = useMemo(() => toFlowElements(graph ?? { nodes: [], edges: [] }), [graph])

  // Schema is fetched/shown only on demand — clicking a pipeline node opens
  // the panel; other node kinds (resource, dbt) don't react to this click.
  // A pipeline node's graph id is `pipeline::{pipeline_id}` (see
  // lineage.rs::build_graph, mirrors the `dbt::{pipeline_id}::...` prefix
  // dbt nodes use) — strip it back to the raw id the schema endpoint keys
  // on.
  const handleNodeClick: NodeMouseHandler = (_event, node) => {
    if (node.type === 'pipeline') {
      setSelectedPipelineId(node.id.replace(/^pipeline::/, ''))
    }
  }

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-white/10 px-6 py-4">
        <h1 className="text-lg font-semibold tracking-tight">{t('lineage.title')}</h1>
        <p className="text-xs text-muted-foreground">{t('lineage.subtitle')}</p>
      </div>

      {loading && (
        <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('lineage.loading')}
        </div>
      )}

      {!loading && error && (
        <div className="m-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {!loading && !error && graph && graph.nodes.length === 0 && (
        <div className="flex flex-1 items-center justify-center">
          <EmptyState
            icon={<Waypoints className="h-6 w-6" />}
            title={t('lineage.emptyTitle')}
            description={t('lineage.emptyDescription')}
          />
        </div>
      )}

      {!loading && !error && graph && graph.nodes.length > 0 && (
        <div className="relative flex-1 bg-background">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={lineageNodeTypes}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={true}
            onNodeClick={handleNodeClick}
            colorMode="dark"
            fitView
            fitViewOptions={{ maxZoom: 1 }}
          >
            <Background gap={20} size={1} color="oklch(1 0 0 / 8%)" />
          </ReactFlow>
          {selectedPipelineId && (
            <PipelineSchemaPanel
              pipelineId={selectedPipelineId}
              onClose={() => setSelectedPipelineId(null)}
            />
          )}
        </div>
      )}
    </div>
  )
}

/** `useReactFlow`-free, but React Flow still needs a provider ancestor for
 * its internal store — same requirement `DagCanvas.tsx` has. */
export default function LineagePanel() {
  return (
    <ReactFlowProvider>
      <LineageGraphView />
    </ReactFlowProvider>
  )
}
