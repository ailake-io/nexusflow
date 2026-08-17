import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { DagNode } from '@/lib/dag'
import { useI18n } from '@/lib/i18n'
import { Database, Code2, Layers, Sparkles } from 'lucide-react'

/** Custom renderers for canvas nodes — read connector/role/sql straight off
 * node.data instead of a separate display-only `label` field, so there's
 * nothing to keep in sync when the inspector edits role/name/sql. */

export function ConnectorNodeView({ data, selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  if (data.kind !== 'connector') return null
  const isSource = data.role === 'source'
  return (
    <div
      className={`group min-w-[8rem] rounded-lg border bg-card px-2.5 py-1.5 shadow-sm transition-all ${
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
          {isSource ? t('pipelines.source') : t('pipelines.sink')}
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
  const { t } = useI18n()
  return (
    <div
      className={`min-w-[7rem] rounded-lg border bg-card px-2.5 py-1.5 shadow-sm transition-all ${
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
        <div className="text-sm font-semibold text-foreground">{t('pipelines.transform')}</div>
      </div>
      <div className="mt-0.5 text-xs text-muted-foreground">{t('canvas.sql')}</div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-accent"
      />
    </div>
  )
}

function dbtCommandLabel(command: string, t: (key: string) => string): string {
  if (command === 'build') return t('canvas.dbtBuild')
  if (command === 'test') return t('canvas.dbtTest')
  return t('canvas.dbtRun')
}

export function DbtNodeView({ data, selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  if (data.kind !== 'dbt') return null
  return (
    <div
      className={`min-w-[7rem] rounded-lg border bg-card px-2.5 py-1.5 shadow-sm transition-all ${
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
        <div className="text-sm font-semibold text-foreground">
          {t('canvas.dbt')} {dbtCommandLabel(data.command, t)}
        </div>
      </div>
      <div className="mt-0.5 truncate text-xs text-muted-foreground">
        {data.select || data.projectDir || t('canvas.noProjectSet')}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-emerald-400"
      />
    </div>
  )
}

export function EmbeddingNodeView({ data, selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  if (data.kind !== 'embedding') return null
  return (
    <div
      className={`min-w-[7rem] rounded-lg border bg-card px-2.5 py-1.5 shadow-sm transition-all ${
        selected
          ? 'border-fuchsia-400 shadow-[0_0_0_2px_rgba(232,121,249,0.25)]'
          : 'border-white/10 hover:border-fuchsia-400/40'
      }`}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-fuchsia-400"
      />
      <div className="flex items-center gap-2">
        <Sparkles className="h-3.5 w-3.5 text-fuchsia-400" />
        <div className="text-sm font-semibold text-foreground">{t('canvas.embedding')}</div>
      </div>
      <div className="mt-0.5 truncate text-xs text-muted-foreground">
        {data.outputColumn || t('canvas.embeddingNoColumn')} · {data.backend}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !bg-background !border-fuchsia-400"
      />
    </div>
  )
}
