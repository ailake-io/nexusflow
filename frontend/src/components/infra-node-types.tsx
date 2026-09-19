import type { NodeProps } from '@xyflow/react'
import { Box } from 'lucide-react'
import type { InfraNode } from '@/lib/infra'
import { NodeCard } from '@/components/node-card'

/**
 * Single node view for every infra module — unlike `dag-nodes.tsx`'s
 * per-`kind` views (connector/transform/dbt/...), every Infra canvas node is
 * the same `kind: 'module'` shape, just a different `module` id. Both
 * handles are always shown (target *and* source) because, unlike a
 * connector's fixed source-or-sink role, any module can be upstream of one
 * module and downstream of another at the same time (e.g. a VPC feeds
 * subnet IDs to an ECS service while itself having no upstream).
 */
export function ModuleNodeView({ data, selected }: NodeProps<InfraNode>) {
  return <NodeCard accent="primary" icon={Box} title={data.module} selected={selected} />
}

export const infraNodeTypes = {
  module: ModuleNodeView,
}
