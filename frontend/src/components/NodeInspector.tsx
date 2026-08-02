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
      <aside className="w-72 shrink-0 border-l bg-card p-3">
        <h2 className="mb-3 text-sm font-medium text-muted-foreground">{data.connector}</h2>
        <div className="flex flex-col gap-3">
          <div>
            <Label htmlFor="node-role">Role</Label>
            <select
              id="node-role"
              value={data.role}
              onChange={(e) => onChange(node.id, { role: e.target.value as ConnectorRole })}
              className="mt-1 flex h-8 w-full rounded-lg border border-input bg-transparent px-2.5 text-sm"
            >
              <option value="source">source</option>
              <option value="sink">sink</option>
            </select>
          </div>
          <div>
            <Label htmlFor="node-name">Name (optional)</Label>
            <Input
              id="node-name"
              value={data.name}
              placeholder={`${data.role}0`}
              onChange={(e) => onChange(node.id, { name: e.target.value })}
              className="mt-1"
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
              <Label htmlFor="node-config">Config (JSON)</Label>
              <textarea
                id="node-config"
                value={data.config}
                onChange={(e) => onChange(node.id, { config: e.target.value })}
                rows={10}
                spellCheck={false}
                className="mt-1 w-full rounded-lg border border-input bg-transparent p-2 font-mono text-xs"
              />
            </div>
          )}
        </div>
      </aside>
    )
  }

  if (data.kind === 'transform') {
    return (
      <aside className="w-72 shrink-0 border-l bg-card p-3">
        <h2 className="mb-3 text-sm font-medium text-muted-foreground">transform</h2>
        <Label htmlFor="transform-sql">SQL</Label>
        <textarea
          id="transform-sql"
          value={data.sql}
          onChange={(e) => onChange(node.id, { sql: e.target.value })}
          rows={12}
          spellCheck={false}
          className="mt-1 w-full rounded-lg border border-input bg-transparent p-2 font-mono text-xs"
        />
      </aside>
    )
  }

  return (
    <aside className="w-72 shrink-0 border-l bg-card p-3">
      <h2 className="mb-3 text-sm font-medium text-muted-foreground">dbt (ELT, pós-carga)</h2>
      <div className="flex flex-col gap-3">
        <div>
          <Label htmlFor="dbt-project-dir">Project dir</Label>
          <Input
            id="dbt-project-dir"
            value={data.projectDir}
            placeholder="/path/to/dbt/project"
            onChange={(e) => onChange(node.id, { projectDir: e.target.value })}
            className="mt-1"
          />
        </div>
        <div>
          <Label htmlFor="dbt-command">Command</Label>
          <select
            id="dbt-command"
            value={data.command}
            onChange={(e) => onChange(node.id, { command: e.target.value as DbtCommand })}
            className="mt-1 flex h-8 w-full rounded-lg border border-input bg-transparent px-2.5 text-sm"
          >
            <option value="run">run</option>
            <option value="build">build (models + tests)</option>
            <option value="test">test</option>
          </select>
        </div>
        <div>
          <Label htmlFor="dbt-select">Select (optional)</Label>
          <Input
            id="dbt-select"
            value={data.select}
            placeholder="e.g. tag:nightly"
            onChange={(e) => onChange(node.id, { select: e.target.value })}
            className="mt-1"
          />
        </div>
      </div>
    </aside>
  )
}
