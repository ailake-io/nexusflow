import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, Loader2, Waypoints } from 'lucide-react'
import { Background, ReactFlow, ReactFlowProvider, type Edge, type Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { getLineage, type LineageEdge, type LineageGraph, type LineageNode } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import {
  LineagePipelineNodeView,
  LineageResourceNodeView,
  type LineagePipelineNodeData,
  type LineageResourceNodeData,
} from '@/components/lineage-nodes'

const lineageNodeTypes = {
  pipeline: LineagePipelineNodeView,
  resource: LineageResourceNodeView,
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

  const positions: Record<string, { x: number; y: number }> = {}
  for (const [r, ids] of byRank.entries()) {
    ids.forEach((id, i) => {
      positions[id] = { x: r * RANK_GAP, y: i * ROW_GAP }
    })
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
    const data: LineageResourceNodeData = {
      kind: 'resource',
      label: n.label,
      connector: n.connector,
      resourceKind: n.resource_kind,
    }
    return { id: n.id, type: 'resource', position, data }
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

function LineageGraphView() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [graph, setGraph] = useState<LineageGraph | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

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
            colorMode="dark"
            fitView
            fitViewOptions={{ maxZoom: 1 }}
          >
            <Background gap={20} size={1} color="oklch(1 0 0 / 8%)" />
          </ReactFlow>
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
