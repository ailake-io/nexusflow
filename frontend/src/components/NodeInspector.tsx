import type {
  ConnectorNodeData,
  ConnectorRole,
  DagNode,
  DbtCommand,
  DbtNodeData,
  TransformNodeData,
} from '@/lib/dag'
import type { ConnectorDescriptor } from '@/lib/api'
import { useI18n } from '@/lib/i18n'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SchemaForm } from '@/components/SchemaForm'
import { Database, Code2, Layers } from 'lucide-react'

interface NodeInspectorProps {
  node: DagNode
  connectors: ConnectorDescriptor[]
  onChange: (
    id: string,
    data: Partial<ConnectorNodeData> | Partial<TransformNodeData> | Partial<DbtNodeData>,
  ) => void
}

/** `data.config` is freely-typed JSON text (edited via textarea when no
 * schema is available) — SchemaForm needs an object to bind fields onto,
 * so an unparseable or empty string just starts the form from scratch. */
function parseConfig(raw: string): Record<string, unknown> {
  if (!raw.trim()) return {}
  try {
    const parsed: unknown = JSON.parse(raw)
    return typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {}
  } catch {
    return {}
  }
}

/**
 * Side panel for the currently-selected canvas node — edits exactly the
 * fields that end up in PipelineSpec/NodeSpec JSON (role/name/config or
 * transform SQL). See lib/dag.ts for the schema these map onto.
 */
export function NodeInspector({ node, connectors, onChange }: NodeInspectorProps) {
  const { t } = useI18n()
  const data = node.data
  if (data.kind === 'connector') {
    const descriptor = connectors.find((c) => c.name === data.connector)
    const schema = descriptor?.config_schema
    const hasFormSchema = Boolean(schema?.properties && Object.keys(schema.properties).length > 0)

    return (
      <aside className="flex w-80 shrink-0 flex-col border-l bg-card">
        <div className="border-b border-white/10 px-4 py-3">
          <div className="flex items-center gap-2">
            <Database className="h-4 w-4 text-primary" />
            <h2 className="text-sm font-semibold text-foreground">{data.connector}</h2>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.connectorNode')}</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <div className="flex flex-col gap-4">
            <div>
              <Label htmlFor="node-role" className="text-xs font-medium">
                {t('canvas.role')}
              </Label>
              <select
                id="node-role"
                value={data.role}
                onChange={(e) => onChange(node.id, { role: e.target.value as ConnectorRole })}
                className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-transparent px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="source">{t('pipelines.source')}</option>
                <option value="sink">{t('pipelines.sink')}</option>
              </select>
            </div>
            <div>
              <Label htmlFor="node-name" className="text-xs font-medium">
                {t('canvas.name')}{' '}
                <span className="text-muted-foreground">({t('common.optional')})</span>
              </Label>
              <Input
                id="node-name"
                value={data.name}
                placeholder={`${data.role}0`}
                onChange={(e) => onChange(node.id, { name: e.target.value })}
                className="mt-1.5"
              />
            </div>
            {hasFormSchema && schema ? (
              <SchemaForm
                schema={schema}
                defs={schema.$defs ?? {}}
                idPrefix="node-config-"
                value={parseConfig(data.config)}
                onChange={(next) => onChange(node.id, { config: JSON.stringify(next, null, 2) })}
              />
            ) : (
              <div>
                <Label htmlFor="node-config" className="text-xs font-medium">
                  {t('canvas.config')} <span className="text-muted-foreground">({t('canvas.configJson')})</span>
                </Label>
                <textarea
                  id="node-config"
                  value={data.config}
                  onChange={(e) => onChange(node.id, { config: e.target.value })}
                  rows={12}
                  spellCheck={false}
                  className="mt-1.5 w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
            )}
          </div>
        </div>
      </aside>
    )
  }

  if (data.kind === 'transform') {
    return (
      <aside className="flex w-80 shrink-0 flex-col border-l bg-card">
        <div className="border-b border-white/10 px-4 py-3">
          <div className="flex items-center gap-2">
            <Code2 className="h-4 w-4 text-accent" />
            <h2 className="text-sm font-semibold text-foreground">{t('canvas.transform')}</h2>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.transformDesc')}</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <Label htmlFor="transform-sql" className="text-xs font-medium">
            {t('canvas.sql')}
          </Label>
          <textarea
            id="transform-sql"
            value={data.sql}
            onChange={(e) => onChange(node.id, { sql: e.target.value })}
            rows={16}
            spellCheck={false}
            placeholder={t('canvas.sqlPlaceholder')}
            className="mt-1.5 w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
      </aside>
    )
  }

  return (
    <aside className="flex w-80 shrink-0 flex-col border-l bg-card">
      <div className="border-b border-white/10 px-4 py-3">
        <div className="flex items-center gap-2">
          <Layers className="h-4 w-4 text-emerald-400" />
          <h2 className="text-sm font-semibold text-foreground">{t('canvas.dbt')}</h2>
        </div>
        <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.dbtDesc')}</p>
      </div>
      <div className="flex-1 overflow-auto p-4">
        <div className="flex flex-col gap-4">
          <div>
            <Label htmlFor="dbt-project-dir" className="text-xs font-medium">
              {t('canvas.projectDir')}
            </Label>
            <Input
              id="dbt-project-dir"
              value={data.projectDir}
              placeholder={t('canvas.projectDirPlaceholder')}
              onChange={(e) => onChange(node.id, { projectDir: e.target.value })}
              className="mt-1.5"
            />
          </div>
          <div>
            <Label htmlFor="dbt-command" className="text-xs font-medium">
              {t('canvas.command')}
            </Label>
            <select
              id="dbt-command"
              value={data.command}
              onChange={(e) => onChange(node.id, { command: e.target.value as DbtCommand })}
              className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-transparent px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
            >
              <option value="run">{t('canvas.dbtRun')}</option>
              <option value="build">{t('canvas.dbtBuild')}</option>
              <option value="test">{t('canvas.dbtTest')}</option>
            </select>
          </div>
          <div>
            <Label htmlFor="dbt-select" className="text-xs font-medium">
              {t('canvas.selectOptional')}
            </Label>
            <Input
              id="dbt-select"
              value={data.select}
              placeholder={t('canvas.selectPlaceholder')}
              onChange={(e) => onChange(node.id, { select: e.target.value })}
              className="mt-1.5"
            />
          </div>
        </div>
      </div>
    </aside>
  )
}
