import { Handle, Position, type NodeProps } from '@xyflow/react'
import { Workflow, Table2, Boxes, Radio, FileText, CalendarClock } from 'lucide-react'
import type { LineageResourceKind } from '@/lib/api'

/** Read-only node renderers for the Lineage tab — visually related to
 * `dag-nodes.tsx`'s `ConnectorNodeView` (same card shape/border language)
 * but simpler: no edit affordances, since this graph is computed, never
 * dragged/connected by the user. */

export type LineagePipelineNodeData = {
  kind: 'pipeline'
  label: string
  hasSchedule: boolean
}

export type LineageResourceNodeData = {
  kind: 'resource'
  label: string
  connector: string
  resourceKind: LineageResourceKind
}

const resourceIcon: Record<LineageResourceKind, typeof Table2> = {
  table: Table2,
  collection: Boxes,
  topic: Radio,
  file: FileText,
}

export function LineagePipelineNodeView({
  data,
}: NodeProps & { data: LineagePipelineNodeData }) {
  return (
    <div className="min-w-[9rem] rounded-lg border border-primary/40 bg-card px-3 py-2 shadow-sm">
      <Handle type="target" position={Position.Left} className="!h-2.5 !w-2.5 !border-2 !bg-background !border-primary" />
      <div className="flex items-center gap-2">
        <Workflow className="h-3.5 w-3.5 text-primary" />
        <div className="truncate text-sm font-semibold text-foreground">{data.label}</div>
      </div>
      {data.hasSchedule && (
        <div className="mt-0.5 flex items-center gap-1 text-[10px] text-muted-foreground">
          <CalendarClock className="h-3 w-3" />
          cron
        </div>
      )}
      <Handle type="source" position={Position.Right} className="!h-2.5 !w-2.5 !border-2 !bg-background !border-primary" />
    </div>
  )
}

export function LineageResourceNodeView({
  data,
}: NodeProps & { data: LineageResourceNodeData }) {
  const Icon = resourceIcon[data.resourceKind]
  return (
    <div className="min-w-[9rem] rounded-lg border border-white/10 bg-card px-3 py-2 shadow-sm">
      <Handle type="target" position={Position.Left} className="!h-2.5 !w-2.5 !border-2 !bg-background !border-white/30" />
      <div className="flex items-center gap-2">
        <Icon className="h-3.5 w-3.5 text-amber-400" />
        <div className="truncate text-sm font-semibold text-foreground">{data.label}</div>
      </div>
      <div className="mt-0.5 truncate text-[10px] uppercase tracking-wide text-muted-foreground">
        {data.connector}
      </div>
      <Handle type="source" position={Position.Right} className="!h-2.5 !w-2.5 !border-2 !bg-background !border-white/30" />
    </div>
  )
}
