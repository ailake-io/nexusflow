import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { DagNode } from '@/lib/dag'

/** Custom renderers for canvas nodes — read connector/role/sql straight off
 * node.data instead of a separate display-only `label` field, so there's
 * nothing to keep in sync when the inspector edits role/name/sql. */

export function ConnectorNodeView({ data, selected }: NodeProps<DagNode>) {
  if (data.kind !== 'connector') return null
  return (
    <div
      className={`rounded-md border bg-card px-3 py-2 text-sm shadow-sm ${selected ? 'border-primary' : ''}`}
    >
      <Handle type="target" position={Position.Left} />
      <div className="font-medium">{data.connector}</div>
      <div className="text-xs text-muted-foreground">{data.name || data.role}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export function TransformNodeView({ selected }: NodeProps<DagNode>) {
  return (
    <div
      className={`rounded-md border bg-card px-3 py-2 text-sm shadow-sm ${selected ? 'border-primary' : ''}`}
    >
      <Handle type="target" position={Position.Left} />
      <div className="font-medium">transform</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

export const dagNodeTypes = {
  connector: ConnectorNodeView,
  transform: TransformNodeView,
}
