import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { DagNode } from '@/lib/dag'
import { Database, Code2, Layers } from 'lucide-react'

/** Custom renderers for canvas nodes — read connector/role/sql straight off
 * node.data instead of a separate display-only `label` field, so there's
 * nothing to keep in sync when the inspector edits role/name/sql. */

export function ConnectorNodeView({ data, selected }: NodeProps<DagNode>) {
  if (data.kind !== 'connector') return null
  const isSource = data.role === 'source'
  return (
    <div
      className={`group min-w-[9rem] rounded-lg border bg-card px-3 py-2 shadow-sm transition-all ${
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
        <Database className="h-3.5 w-3.5 text-primary" />
        <div className="text-sm font-semibold text-foreground">{data.connector}</div>
      </div>
      <div className="mt-0.5 flex items-center gap-1.5 text-xs text-muted-foreground">
        <span
          className={`rounded px-1 py-0 text-[10px] font-medium uppercase ${
            isSource ? 'bg-emerald-500/10 text-emerald-400' : 'bg-amber-500/10 text-amber-400'
          }`}
        >
          {data.role}
        </span>
        {data.name && <span className="truncate">{data.name}</span>}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-primary"
      />
    </div>
  )
}

export function TransformNodeView({ selected }: NodeProps<DagNode>) {
  return (
    <div
      className={`min-w-[8rem] rounded-lg border bg-card px-3 py-2 shadow-sm transition-all ${
        selected
          ? 'border-accent shadow-[0_0_0_2px_hsl(var(--color-accent)/0.3)]'
          : 'border-white/10 hover:border-accent/40'
      }`}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-accent"
      />
      <div className="flex items-center gap-2">
        <Code2 className="h-3.5 w-3.5 text-accent" />
        <div className="text-sm font-semibold text-foreground">transform</div>
      </div>
      <div className="mt-0.5 text-xs text-muted-foreground">SQL</div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-accent"
      />
    </div>
  )
}

export function DbtNodeView({ data, selected }: NodeProps<DagNode>) {
  if (data.kind !== 'dbt') return null
  return (
    <div
      className={`min-w-[8rem] rounded-lg border bg-card px-3 py-2 shadow-sm transition-all ${
        selected
          ? 'border-emerald-400 shadow-[0_0_0_2px_rgba(52,211,153,0.25)]'
          : 'border-white/10 hover:border-emerald-400/40'
      }`}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-emerald-400"
      />
      <div className="flex items-center gap-2">
        <Layers className="h-3.5 w-3.5 text-emerald-400" />
        <div className="text-sm font-semibold text-foreground">dbt {data.command}</div>
      </div>
      <div className="mt-0.5 truncate text-xs text-muted-foreground">
        {data.select || data.projectDir || 'no project set'}
      </div>
    </div>
  )
}
