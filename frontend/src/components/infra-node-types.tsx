import { Handle, Position, type NodeProps } from '@xyflow/react'
import { Box } from 'lucide-react'
import type { InfraNode } from '@/lib/infra'

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
  return (
    <div
      className={`group min-w-[9rem] rounded-lg border bg-card px-2.5 py-1.5 shadow-sm transition-all ${
        selected
          ? 'border-primary shadow-[0_0_0_2px_hsl(var(--color-primary)/0.3)]'
          : 'border-white/10 hover:border-primary/40'
      }`}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-primary"
      />
      <div className="flex items-center gap-2">
        <Box className="h-3.5 w-3.5 text-primary" />
        <div className="text-sm font-semibold text-foreground">{data.module}</div>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-primary"
      />
    </div>
  )
}

export const infraNodeTypes = {
  module: ModuleNodeView,
}
