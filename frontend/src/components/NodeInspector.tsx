import type { ConnectorNodeData, ConnectorRole, DagNode, TransformNodeData } from '@/lib/dag'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

interface NodeInspectorProps {
  node: DagNode
  onChange: (id: string, data: Partial<ConnectorNodeData> | Partial<TransformNodeData>) => void
}

/**
 * Side panel for the currently-selected canvas node — edits exactly the
 * fields that end up in PipelineSpec/NodeSpec JSON (role/name/config or
 * transform SQL). See lib/dag.ts for the schema these map onto.
 */
export function NodeInspector({ node, onChange }: NodeInspectorProps) {
  const data = node.data
  if (data.kind === 'connector') {
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
        </div>
      </aside>
    )
  }

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
