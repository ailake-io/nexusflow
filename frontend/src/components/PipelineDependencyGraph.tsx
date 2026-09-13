import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, GitBranch, Loader2 } from 'lucide-react'
import { Background, ReactFlow, ReactFlowProvider, type Edge, type Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { getOrchestrationGraph, type OrchestrationGraph } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import { LineagePipelineNodeView, type LineagePipelineNodeData } from '@/components/lineage-nodes'

const nodeTypes = { pipeline: LineagePipelineNodeView }

const RANK_GAP = 240
const ROW_GAP = 80

/** Same layered left-to-right layout as `LineagePanel.tsx`'s
 *  `computeLayout` (Kahn's algorithm, longest-path rank) — kept as its own
 *  small copy rather than a shared import: this graph's nodes are plain
 *  pipeline ids (no resource/dbt node kinds), so the two would otherwise
 *  need a shared generic node/edge shape for no real benefit at this size. */
function computeLayout(
  ids: string[],
  edges: { from: string; to: string }[],
): Record<string, { x: number; y: number }> {
  const outgoing = new Map<string, string[]>()
  const inDegree = new Map<string, number>()
  for (const id of ids) {
    inDegree.set(id, 0)
    outgoing.set(id, [])
  }
  for (const e of edges) {
    outgoing.get(e.from)?.push(e.to)
    inDegree.set(e.to, (inDegree.get(e.to) ?? 0) + 1)
  }

  const remaining = new Set(ids)
  const localInDegree = new Map(inDegree)
  const rank = new Map<string, number>()
  let frontier = ids.filter((id) => (inDegree.get(id) ?? 0) === 0)
  let level = 0

  while (remaining.size > 0) {
    if (frontier.length === 0) {
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
  const ranksAscending = Array.from(byRank.keys()).sort((a, b) => a - b)
  for (const r of ranksAscending) {
    byRank.get(r)!.forEach((id, i) => {
      positions[id] = { x: r * RANK_GAP, y: i * ROW_GAP }
    })
  }
  return positions
}

function toFlowElements(graph: OrchestrationGraph): { nodes: Node[]; edges: Edge[] } {
  const ids = graph.nodes.map((n) => n.pipeline_id)
  const positions = computeLayout(ids, graph.edges)
  const nodes: Node[] = graph.nodes.map((n) => {
    const data: LineagePipelineNodeData = {
      kind: 'pipeline',
      label: n.pipeline_id,
      hasSchedule: false,
    }
    return {
      id: n.pipeline_id,
      type: 'pipeline',
      position: positions[n.pipeline_id] ?? { x: 0, y: 0 },
      data,
    }
  })
  const edges: Edge[] = graph.edges.map((e) => ({
    id: `${e.from}->${e.to}`,
    source: e.from,
    target: e.to,
    animated: false,
    // `all` mode edges are dashed — visually distinct so "waits for every
    // upstream" reads differently from "any one upstream fires it".
    style: {
      stroke: 'oklch(1 0 0 / 20%)',
      strokeDasharray: e.dependency_mode === 'all' ? '4 3' : undefined,
    },
  }))
  return { nodes, edges }
}

function OrchestrationGraphView() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [graph, setGraph] = useState<OrchestrationGraph | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    getOrchestrationGraph(token)
      .then((data) => {
        if (!cancelled) {
          setGraph(data)
          setError(null)
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : t('orchestration.error'))
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, t])

  const { nodes, edges } = useMemo(
    () => toFlowElements(graph ?? { nodes: [], edges: [] }),
    [graph],
  )
  const hasDependencies = (graph?.edges.length ?? 0) > 0

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-white/10 px-6 py-4">
        <h1 className="text-lg font-semibold tracking-tight">{t('orchestration.title')}</h1>
        <p className="text-xs text-muted-foreground">{t('orchestration.subtitle')}</p>
      </div>

      {loading && (
        <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('orchestration.loading')}
        </div>
      )}

      {!loading && error && (
        <div className="m-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {!loading && !error && !hasDependencies && (
        <div className="flex flex-1 items-center justify-center">
          <EmptyState
            icon={<GitBranch className="h-6 w-6" />}
            title={t('orchestration.emptyTitle')}
            description={t('orchestration.emptyDescription')}
          />
        </div>
      )}

      {!loading && !error && hasDependencies && (
        <div className="relative flex-1 bg-background">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
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
 *  its internal store — same requirement `LineagePanel.tsx`/`DagCanvas.tsx`
 *  have. */
export default function PipelineDependencyGraph() {
  return (
    <ReactFlowProvider>
      <OrchestrationGraphView />
    </ReactFlowProvider>
  )
}
