import type {
  ConnectorNodeData,
  ConnectorRole,
  DagNode,
  DbtCommand,
  DbtNodeData,
  TransformNodeData,
} from '@/lib/dag'
import type { ConnectorDescriptor } from '@/lib/api'
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
          <p className="mt-0.5 text-[10px] text-muted-foreground">Connector node</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <div className="flex flex-col gap-4">
            <div>
              <Label htmlFor="node-role" className="text-xs font-medium">
                Role
              </Label>
              <select
                id="node-role"
                value={data.role}
                onChange={(e) => onChange(node.id, { role: e.target.value as ConnectorRole })}
                className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-transparent px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="source">source</option>
                <option value="sink">sink</option>
              </select>
            </div>
            <div>
              <Label htmlFor="node-name" className="text-xs font-medium">
                Name <span className="text-muted-foreground">(optional)</span>
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
                  Config <span className="text-muted-foreground">(JSON)</span>
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
            <h2 className="text-sm font-semibold text-foreground">Transform</h2>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">SQL transformation</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <Label htmlFor="transform-sql" className="text-xs font-medium">
            SQL
          </Label>
          <textarea
            id="transform-sql"
            value={data.sql}
            onChange={(e) => onChange(node.id, { sql: e.target.value })}
            rows={16}
            spellCheck={false}
            placeholder="SELECT * FROM source"
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
          <h2 className="text-sm font-semibold text-foreground">dbt</h2>
        </div>
        <p className="mt-0.5 text-[10px] text-muted-foreground">Post-load ELT step</p>
      </div>
      <div className="flex-1 overflow-auto p-4">
        <div className="flex flex-col gap-4">
          <div>
            <Label htmlFor="dbt-project-dir" className="text-xs font-medium">
              Project dir
            </Label>
            <Input
              id="dbt-project-dir"
              value={data.projectDir}
              placeholder="/path/to/dbt/project"
              onChange={(e) => onChange(node.id, { projectDir: e.target.value })}
              className="mt-1.5"
            />
          </div>
          <div>
            <Label htmlFor="dbt-command" className="text-xs font-medium">
              Command
            </Label>
            <select
              id="dbt-command"
              value={data.command}
              onChange={(e) => onChange(node.id, { command: e.target.value as DbtCommand })}
              className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-transparent px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
            >
              <option value="run">run</option>
              <option value="build">build (models + tests)</option>
              <option value="test">test</option>
            </select>
          </div>
          <div>
            <Label htmlFor="dbt-select" className="text-xs font-medium">
              Select <span className="text-muted-foreground">(optional)</span>
            </Label>
            <Input
              id="dbt-select"
              value={data.select}
              placeholder="e.g. tag:nightly"
              onChange={(e) => onChange(node.id, { select: e.target.value })}
              className="mt-1.5"
            />
          </div>
        </div>
      </div>
    </aside>
  )
}
