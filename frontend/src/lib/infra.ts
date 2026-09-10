import type { Edge, Node } from '@xyflow/react'
import type { InfraGraph, InfraNode as InfraNodeSpec } from '@/lib/api'

/**
 * Unlike `dag.ts`'s `toPipelineSpec`/`fromPipelineSpec` (which *infer* a
 * rigid pipeline shape — sources/transform/sinks — from each node's `kind`),
 * the Infra canvas is a genuine general graph: any module can depend on any
 * other module's output, arbitrary fan-in/fan-out. So this conversion is a
 * near-literal passthrough between React Flow's {nodes, edges} and the
 * backend's `InfraGraph` — no shape inference needed.
 */
export interface InfraModuleNodeData extends Record<string, unknown> {
  kind: 'module'
  module: string
  /** Raw JSON text, edited via SchemaForm in the inspector — parsed on
   * generate, same convention as `dag.ts`'s `ConnectorNodeData.config`. */
  config: string
}

export type InfraNode = Node<InfraModuleNodeData, 'module'>

/** React Flow edge data — which named output of the source node feeds which
 * named input of the target node. Set once, at connect time, from the
 * inspector (a module can have more than one output/input, so this can't
 * be inferred from the connection alone) — see `InfraCanvas.tsx`. */
export interface InfraEdgeData extends Record<string, unknown> {
  output: string
  input: string
}

export function toInfraGraph(
  nodes: InfraNode[],
  edges: Edge[],
  provider: Record<string, unknown> = {},
): InfraGraph {
  return {
    nodes: nodes.map((n) => ({
      id: n.id,
      module: n.data.module,
      config: JSON.parse(n.data.config || '{}'),
    })),
    edges: edges.map((e) => {
      const data = (e.data as InfraEdgeData | undefined) ?? { output: '', input: '' }
      return { from: e.source, to: e.target, output: data.output, input: data.input }
    }),
    provider,
  }
}

/** Inverse of `toInfraGraph` — used when loading a previously exported/saved
 * graph back onto the canvas. Positions aren't part of `InfraGraph` (the
 * backend doesn't care where a node sits visually), so reloaded nodes are
 * laid out in a simple grid; the user can drag them apart afterward. */
export function fromInfraGraph(graph: InfraGraph): { nodes: InfraNode[]; edges: Edge[] } {
  const nodes: InfraNode[] = graph.nodes.map((n: InfraNodeSpec, i) => ({
    id: n.id,
    type: 'module',
    position: { x: (i % 4) * 260, y: Math.floor(i / 4) * 160 },
    data: { kind: 'module', module: n.module, config: JSON.stringify(n.config ?? {}, null, 2) },
  }))
  const edges: Edge[] = graph.edges.map((e, i) => ({
    id: `edge-${i}-${e.from}-${e.to}`,
    source: e.from,
    target: e.to,
    data: { output: e.output, input: e.input } satisfies InfraEdgeData,
  }))
  return { nodes, edges }
}
