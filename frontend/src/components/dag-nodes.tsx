import type { NodeProps } from '@xyflow/react'
import type { DagNode } from '@/lib/dag'
import { useI18n } from '@/lib/i18n'
import { Database, Code2, Layers, Sparkles, Terminal } from 'lucide-react'
import { NodeBadge, NodeCard } from '@/components/node-card'

/** Custom renderers for canvas nodes — read connector/role/sql straight off
 * node.data instead of a separate display-only `label` field, so there's
 * nothing to keep in sync when the inspector edits role/name/sql. The look
 * itself lives in `NodeCard`; each view only picks an accent and its text. */

export function ConnectorNodeView({ data, selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  if (data.kind !== 'connector') return null
  const isSource = data.role === 'source'
  return (
    <NodeCard
      accent="primary"
      icon={Database}
      title={data.connector}
      badge={
        <NodeBadge tone={isSource ? 'emerald' : 'amber'}>
          {isSource ? t('pipelines.source') : t('pipelines.sink')}
        </NodeBadge>
      }
      subtitle={data.name}
      selected={selected}
    />
  )
}

export function TransformNodeView({ selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  return (
    <NodeCard
      accent="accent"
      icon={Code2}
      title={t('pipelines.transform')}
      subtitle={t('canvas.sql')}
      selected={selected}
    />
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
    <NodeCard
      accent="emerald"
      icon={Layers}
      title={`${t('canvas.dbt')} ${dbtCommandLabel(data.command, t)}`}
      subtitle={data.select || data.projectDir || t('canvas.noProjectSet')}
      selected={selected}
    />
  )
}

export function PythonNodeView({ data, selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  if (data.kind !== 'python') return null
  const firstLine = data.script.trim().split('\n')[0]
  return (
    <NodeCard
      accent="sky"
      icon={Terminal}
      title={t('canvas.python')}
      subtitle={firstLine || t('canvas.noScriptSet')}
      selected={selected}
    />
  )
}

export function EmbeddingNodeView({ data, selected }: NodeProps<DagNode>) {
  const { t } = useI18n()
  if (data.kind !== 'embedding') return null
  return (
    <NodeCard
      accent="fuchsia"
      icon={Sparkles}
      title={t('canvas.embedding')}
      subtitle={`${data.outputColumn || t('canvas.embeddingNoColumn')} · ${data.backend}`}
      selected={selected}
    />
  )
}
