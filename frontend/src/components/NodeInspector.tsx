import { useRef } from 'react'
import type {
  AggFunction,
  CaseMode,
  CastType,
  ChunkingStrategy,
  CleanBlockKindTag,
  CleanBlockNodeData,
  CleanBlockSpec,
  ComputeOperator,
  ConnectorNodeData,
  ConnectorRole,
  DagNode,
  DbtCommand,
  DbtNodeData,
  EmbeddingBackend,
  EmbeddingNodeData,
  FilterOperator,
  NullFillStrategy,
  PythonNodeData,
  SelectColumnsMode,
  SortDirection,
  TransformNodeData,
} from '@/lib/dag'
import { isConnectorNode, toCleanBlockSpec } from '@/lib/dag'
import type { ConnectorDescriptor } from '@/lib/api'
import { useI18n } from '@/lib/i18n'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SchemaForm } from '@/components/SchemaForm'
import { NodePreview } from '@/components/NodePreview'
import { CleanBlockPreview } from '@/components/CleanBlockPreview'
import {
  Database,
  Code2,
  Layers,
  Sparkles,
  Terminal,
  Upload,
  Wand2,
} from 'lucide-react'

interface NodeInspectorProps {
  node: DagNode
  /** Full canvas node list — the clean-block panel needs it to find the
   * connected source (for its preview button) and every other clean
   * block's position (to know which ones come "before" this one). */
  allNodes: DagNode[]
  connectors: ConnectorDescriptor[]
  onChange: (
    id: string,
    data:
      | Partial<ConnectorNodeData>
      | Partial<TransformNodeData>
      | Partial<DbtNodeData>
      | Partial<EmbeddingNodeData>
      | Partial<PythonNodeData>
      | Partial<CleanBlockNodeData>,
  ) => void
}

const CLEAN_BLOCK_KINDS: CleanBlockKindTag[] = [
  'filter',
  'select_columns',
  'rename',
  'cast',
  'trim',
  'replace_text',
  'fill_nulls',
  'drop_nulls',
  'dedupe',
  'change_case',
  'computed_column',
  'sort',
  'aggregate',
]
const FILTER_OPERATORS: FilterOperator[] = [
  'eq',
  'ne',
  'gt',
  'lt',
  'gte',
  'lte',
  'contains',
  'starts_with',
  'is_null',
  'is_not_null',
]
const SELECT_MODES: SelectColumnsMode[] = ['keep', 'drop']
const CAST_TYPES: CastType[] = ['int', 'float', 'text', 'date', 'boolean']
const NULL_STRATEGIES: NullFillStrategy['strategy'][] = ['value', 'column_average', 'other_column']
const CASE_MODES: CaseMode[] = ['upper', 'lower', 'title']
const COMPUTE_OPERATORS: ComputeOperator[] = ['add', 'subtract', 'multiply', 'divide', 'concat']
const SORT_DIRECTIONS: SortDirection[] = ['asc', 'desc']
const AGG_FUNCTIONS: AggFunction[] = ['sum', 'avg', 'count', 'count_distinct', 'min', 'max']

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
/** Strips a trailing `-cdc` suffix, e.g. `"postgres-cdc"` -> `"postgres"`. */
function batchNameOf(connector: string): string {
  return connector.endsWith('-cdc') ? connector.slice(0, -'-cdc'.length) : connector
}

export function NodeInspector({ node, allNodes, connectors, onChange }: NodeInspectorProps) {
  const { t } = useI18n()
  const data = node.data
  // Only used by the 'transform'/'python' branches below, but hooks can't
  // be called conditionally — must sit above every early return here.
  const transformFileInputRef = useRef<HTMLInputElement>(null)
  const pythonFileInputRef = useRef<HTMLInputElement>(null)
  if (data.kind === 'connector') {
    const descriptor = connectors.find((c) => c.name === data.connector)
    const schema = descriptor?.config_schema
    const hasFormSchema = Boolean(schema?.properties && Object.keys(schema.properties).length > 0)

    // The toggle only makes sense for a connector with an actual `-cdc`
    // counterpart *linked into this build* (the catalog is dynamic — see
    // ARCHITECTURE.md §3 — so this naturally disappears if a deployment
    // doesn't compile that connector in) — and only for sources: none of
    // the native CDC connectors have a `Sink` impl (ARCHITECTURE.md §7).
    const isCdc = data.connector.endsWith('-cdc')
    const batchName = batchNameOf(data.connector)
    const cdcName = `${batchName}-cdc`
    const hasCdcVariant =
      data.role === 'source' &&
      connectors.some((c) => c.name === batchName) &&
      connectors.some((c) => c.name === cdcName)

    return (
      <aside className="flex h-full w-full flex-col border-l bg-card">
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
                onChange={(e) => {
                  const role = e.target.value as ConnectorRole
                  // None of the native CDC connectors have a `Sink` impl
                  // (ARCHITECTURE.md §7) — switching to sink while one is
                  // selected would otherwise silently persist an
                  // unbuildable node, so fall back to the batch connector.
                  onChange(node.id, role === 'sink' && isCdc ? { role, connector: batchName } : { role })
                }}
                className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-card px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="source">{t('pipelines.source')}</option>
                <option value="sink">{t('pipelines.sink')}</option>
              </select>
            </div>
            {hasCdcVariant && (
              <div>
                <Label className="text-xs font-medium">{t('canvas.mode')}</Label>
                <div className="mt-1.5 grid grid-cols-2 gap-1 rounded-lg border border-input p-1">
                  {(['batch', 'cdc'] as const).map((mode) => {
                    const active = mode === (isCdc ? 'cdc' : 'batch')
                    return (
                      <button
                        key={mode}
                        type="button"
                        onClick={() => {
                          const nextConnector = mode === 'cdc' ? cdcName : batchName
                          if (nextConnector === data.connector) return
                          // Batch and CDC configs share no field shapes
                          // worth preserving (see canvas.modeCdcSwitchWarning)
                          // — start the new mode from an empty config
                          // rather than carrying over stale/invalid keys.
                          onChange(node.id, { connector: nextConnector, config: '{}' })
                        }}
                        className={`rounded-md px-2 py-1 text-xs font-medium transition-colors ${
                          active
                            ? 'bg-primary text-primary-foreground'
                            : 'text-muted-foreground hover:text-foreground'
                        }`}
                      >
                        {mode === 'cdc' ? t('canvas.modeCdc') : t('canvas.modeBatch')}
                      </button>
                    )
                  })}
                </div>
                <p className="mt-1 text-[10px] text-muted-foreground">
                  {t('canvas.modeCdcSwitchWarning')}
                </p>
              </div>
            )}
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
            <NodePreview connector={data.connector} config={parseConfig(data.config)} />
          </div>
        </div>
      </aside>
    )
  }

  if (data.kind === 'transform') {
    return (
      <aside className="flex h-full w-full flex-col border-l bg-card">
        <div className="border-b border-white/10 px-4 py-3">
          <div className="flex items-center gap-2">
            <Code2 className="h-4 w-4 text-accent" />
            <h2 className="text-sm font-semibold text-foreground">{t('canvas.transform')}</h2>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.transformDesc')}</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <div className="flex items-center justify-between">
            <Label htmlFor="transform-sql" className="text-xs font-medium">
              {t('canvas.sql')}
            </Label>
            <button
              type="button"
              onClick={() => transformFileInputRef.current?.click()}
              className="flex items-center gap-1 rounded-md border border-input px-2 py-1 text-[10px] font-medium text-muted-foreground hover:bg-accent hover:text-foreground"
            >
              <Upload className="h-3 w-3" />
              {t('canvas.transformUpload')}
            </button>
            <input
              ref={transformFileInputRef}
              type="file"
              accept=".sql,text/x-sql,text/plain"
              className="hidden"
              onChange={async (e) => {
                const file = e.target.files?.[0]
                // Always reset, even on failure — otherwise re-selecting the
                // exact same filename after an error wouldn't fire this
                // handler a second time (React sees no change).
                e.target.value = ''
                if (!file) return
                try {
                  onChange(node.id, { sql: await file.text() })
                } catch {
                  window.alert(t('canvas.transformUploadError'))
                }
              }}
            />
          </div>
          <textarea
            id="transform-sql"
            value={data.sql}
            onChange={(e) => onChange(node.id, { sql: e.target.value })}
            onDragOver={(e) => e.preventDefault()}
            onDrop={async (e) => {
              e.preventDefault()
              const file = e.dataTransfer.files[0]
              if (!file) return
              try {
                onChange(node.id, { sql: await file.text() })
              } catch {
                window.alert(t('canvas.transformUploadError'))
              }
            }}
            rows={16}
            spellCheck={false}
            placeholder={t('canvas.sqlPlaceholder')}
            className="mt-1.5 w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
      </aside>
    )
  }

  if (data.kind === 'dbt') {
    return (
      <aside className="flex h-full w-full flex-col border-l bg-card">
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
                className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-card px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
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

  if (data.kind === 'python') {
    return (
      <aside className="flex h-full w-full flex-col border-l bg-card">
        <div className="border-b border-white/10 px-4 py-3">
          <div className="flex items-center gap-2">
            <Terminal className="h-4 w-4 text-sky-400" />
            <h2 className="text-sm font-semibold text-foreground">{t('canvas.python')}</h2>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.pythonDesc')}</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <div className="flex flex-col gap-4">
            <div>
              <div className="flex items-center justify-between">
                <Label htmlFor="python-script" className="text-xs font-medium">
                  {t('canvas.pythonScript')}
                </Label>
                <button
                  type="button"
                  onClick={() => pythonFileInputRef.current?.click()}
                  className="flex items-center gap-1 rounded-md border border-input px-2 py-1 text-[10px] font-medium text-muted-foreground hover:bg-accent hover:text-foreground"
                >
                  <Upload className="h-3 w-3" />
                  {t('canvas.pythonUpload')}
                </button>
                <input
                  ref={pythonFileInputRef}
                  type="file"
                  accept=".py,text/x-python"
                  className="hidden"
                  onChange={async (e) => {
                    const file = e.target.files?.[0]
                    // Always reset, even on failure — otherwise re-selecting
                    // the exact same filename after an error wouldn't fire
                    // this handler a second time (React sees no change).
                    e.target.value = ''
                    if (!file) return
                    try {
                      onChange(node.id, { script: await file.text() })
                    } catch {
                      window.alert(t('canvas.pythonUploadError'))
                    }
                  }}
                />
              </div>
              <textarea
                id="python-script"
                value={data.script}
                onChange={(e) => onChange(node.id, { script: e.target.value })}
                onDragOver={(e) => e.preventDefault()}
                onDrop={async (e) => {
                  e.preventDefault()
                  const file = e.dataTransfer.files[0]
                  if (!file) return
                  try {
                    onChange(node.id, { script: await file.text() })
                  } catch {
                    window.alert(t('canvas.pythonUploadError'))
                  }
                }}
                rows={16}
                spellCheck={false}
                placeholder={t('canvas.pythonScriptPlaceholder')}
                className="mt-1.5 w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
            <div>
              <Label htmlFor="python-timeout" className="text-xs font-medium">
                {t('canvas.pythonTimeout')}{' '}
                <span className="text-muted-foreground">({t('common.optional')})</span>
              </Label>
              <Input
                id="python-timeout"
                type="number"
                min={1}
                value={data.timeoutSeconds || ''}
                placeholder="60"
                onChange={(e) =>
                  onChange(node.id, { timeoutSeconds: Number(e.target.value) || 0 })
                }
                className="mt-1.5"
              />
            </div>
          </div>
        </div>
      </aside>
    )
  }

  if (data.kind === 'clean') {
    // Every clean node, left-to-right by canvas position — same order
    // `toPipelineSpec` uses to build `clean_blocks`.
    const cleanNodes = allNodes
      .filter((n): n is DagNode & { data: CleanBlockNodeData } => n.data.kind === 'clean')
      .slice()
      .sort((a, b) => a.position.x - b.position.x)
    const myIndex = cleanNodes.findIndex((n) => n.id === node.id)
    const previewSource = allNodes.find(
      (n) => isConnectorNode(n) && n.data.role === 'source',
    )
    const previewSourceSpec =
      previewSource && isConnectorNode(previewSource)
        ? { connector: previewSource.data.connector, config: parseConfig(previewSource.data.config) }
        : undefined
    let previewBlocks: CleanBlockSpec[] | null = null
    if (myIndex >= 0) {
      try {
        previewBlocks = cleanNodes
          .slice(0, myIndex + 1)
          .map((n) => toCleanBlockSpec(n.data))
      } catch {
        previewBlocks = null
      }
    }

    const set = (patch: Partial<CleanBlockNodeData>) => onChange(node.id, patch)

    return (
      <aside className="flex h-full w-full flex-col border-l bg-card">
        <div className="border-b border-white/10 px-4 py-3">
          <div className="flex items-center gap-2">
            <Wand2 className="h-4 w-4 text-teal-400" />
            <h2 className="text-sm font-semibold text-foreground">{t('canvas.clean.title')}</h2>
          </div>
          <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.clean.desc')}</p>
        </div>
        <div className="flex-1 overflow-auto p-4">
          <div className="flex flex-col gap-4">
            <div>
              <Label htmlFor="clean-name" className="text-xs font-medium">
                {t('canvas.clean.name')}{' '}
                <span className="text-muted-foreground">({t('common.optional')})</span>
              </Label>
              <Input
                id="clean-name"
                value={data.name}
                onChange={(e) => set({ name: e.target.value })}
                className="mt-1.5"
              />
            </div>
            <div>
              <Label htmlFor="clean-kind" className="text-xs font-medium">
                {t('canvas.clean.blockKind')}
              </Label>
              <select
                id="clean-kind"
                value={data.blockKind}
                onChange={(e) => set({ blockKind: e.target.value as CleanBlockKindTag })}
                className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
              >
                {CLEAN_BLOCK_KINDS.map((k) => (
                  <option key={k} value={k}>
                    {t(`canvas.clean.kind.${k}`)}
                  </option>
                ))}
              </select>
            </div>

            {data.blockKind === 'filter' && (
              <>
                <div>
                  <Label htmlFor="clean-column" className="text-xs font-medium">
                    {t('canvas.clean.column')}
                  </Label>
                  <Input
                    id="clean-column"
                    value={data.column}
                    onChange={(e) => set({ column: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-operator" className="text-xs font-medium">
                    {t('canvas.clean.operatorLabel')}
                  </Label>
                  <select
                    id="clean-operator"
                    value={data.operator}
                    onChange={(e) => set({ operator: e.target.value as FilterOperator })}
                    className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                  >
                    {FILTER_OPERATORS.map((op) => (
                      <option key={op} value={op}>
                        {t(`canvas.clean.operator.${op}`)}
                      </option>
                    ))}
                  </select>
                </div>
                {data.operator !== 'is_null' && data.operator !== 'is_not_null' && (
                  <div>
                    <Label htmlFor="clean-value" className="text-xs font-medium">
                      {t('canvas.clean.value')}
                    </Label>
                    <Input
                      id="clean-value"
                      value={data.value}
                      onChange={(e) => set({ value: e.target.value })}
                      className="mt-1.5"
                    />
                  </div>
                )}
              </>
            )}

            {(data.blockKind === 'select_columns' ||
              data.blockKind === 'trim' ||
              data.blockKind === 'drop_nulls' ||
              data.blockKind === 'dedupe') && (
              <>
                {data.blockKind === 'select_columns' && (
                  <div>
                    <Label htmlFor="clean-select-mode" className="text-xs font-medium">
                      {t('canvas.clean.mode')}
                    </Label>
                    <select
                      id="clean-select-mode"
                      value={data.selectMode}
                      onChange={(e) => set({ selectMode: e.target.value as SelectColumnsMode })}
                      className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                    >
                      {SELECT_MODES.map((m) => (
                        <option key={m} value={m}>
                          {t(`canvas.clean.selectMode.${m}`)}
                        </option>
                      ))}
                    </select>
                  </div>
                )}
                <div>
                  <Label htmlFor="clean-columns" className="text-xs font-medium">
                    {t('canvas.clean.columns')}
                  </Label>
                  <Input
                    id="clean-columns"
                    value={data.columns}
                    placeholder="id, nome, valor"
                    onChange={(e) => set({ columns: e.target.value })}
                    className="mt-1.5"
                  />
                  {data.blockKind === 'dedupe' && (
                    <p className="mt-1 text-[10px] text-muted-foreground">
                      {t('canvas.clean.dedupeEmptyHint')}
                    </p>
                  )}
                </div>
              </>
            )}

            {data.blockKind === 'rename' && (
              <>
                <div>
                  <Label htmlFor="clean-from" className="text-xs font-medium">
                    {t('canvas.clean.from')}
                  </Label>
                  <Input
                    id="clean-from"
                    value={data.from}
                    onChange={(e) => set({ from: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-to" className="text-xs font-medium">
                    {t('canvas.clean.to')}
                  </Label>
                  <Input
                    id="clean-to"
                    value={data.to}
                    onChange={(e) => set({ to: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
              </>
            )}

            {data.blockKind === 'cast' && (
              <>
                <div>
                  <Label htmlFor="clean-column" className="text-xs font-medium">
                    {t('canvas.clean.column')}
                  </Label>
                  <Input
                    id="clean-column"
                    value={data.column}
                    onChange={(e) => set({ column: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-data-type" className="text-xs font-medium">
                    {t('canvas.clean.dataTypeLabel')}
                  </Label>
                  <select
                    id="clean-data-type"
                    value={data.dataType}
                    onChange={(e) => set({ dataType: e.target.value as CastType })}
                    className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                  >
                    {CAST_TYPES.map((dt) => (
                      <option key={dt} value={dt}>
                        {t(`canvas.clean.dataType.${dt}`)}
                      </option>
                    ))}
                  </select>
                </div>
              </>
            )}

            {data.blockKind === 'replace_text' && (
              <>
                <div>
                  <Label htmlFor="clean-column" className="text-xs font-medium">
                    {t('canvas.clean.column')}
                  </Label>
                  <Input
                    id="clean-column"
                    value={data.column}
                    onChange={(e) => set({ column: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-find" className="text-xs font-medium">
                    {t('canvas.clean.find')}
                  </Label>
                  <Input
                    id="clean-find"
                    value={data.find}
                    onChange={(e) => set({ find: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-replace" className="text-xs font-medium">
                    {t('canvas.clean.replace')}
                  </Label>
                  <Input
                    id="clean-replace"
                    value={data.replace}
                    onChange={(e) => set({ replace: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
              </>
            )}

            {data.blockKind === 'fill_nulls' && (
              <>
                <div>
                  <Label htmlFor="clean-column" className="text-xs font-medium">
                    {t('canvas.clean.column')}
                  </Label>
                  <Input
                    id="clean-column"
                    value={data.column}
                    onChange={(e) => set({ column: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-null-strategy" className="text-xs font-medium">
                    {t('canvas.clean.nullStrategyLabel')}
                  </Label>
                  <select
                    id="clean-null-strategy"
                    value={data.nullStrategy}
                    onChange={(e) =>
                      set({ nullStrategy: e.target.value as NullFillStrategy['strategy'] })
                    }
                    className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                  >
                    {NULL_STRATEGIES.map((s2) => (
                      <option key={s2} value={s2}>
                        {t(`canvas.clean.nullStrategy.${s2}`)}
                      </option>
                    ))}
                  </select>
                </div>
                {data.nullStrategy === 'value' && (
                  <div>
                    <Label htmlFor="clean-value" className="text-xs font-medium">
                      {t('canvas.clean.value')}
                    </Label>
                    <Input
                      id="clean-value"
                      value={data.value}
                      onChange={(e) => set({ value: e.target.value })}
                      className="mt-1.5"
                    />
                  </div>
                )}
                {data.nullStrategy === 'other_column' && (
                  <div>
                    <Label htmlFor="clean-fallback-column" className="text-xs font-medium">
                      {t('canvas.clean.fallbackColumn')}
                    </Label>
                    <Input
                      id="clean-fallback-column"
                      value={data.fallbackColumn}
                      onChange={(e) => set({ fallbackColumn: e.target.value })}
                      className="mt-1.5"
                    />
                  </div>
                )}
              </>
            )}

            {data.blockKind === 'change_case' && (
              <>
                <div>
                  <Label htmlFor="clean-column" className="text-xs font-medium">
                    {t('canvas.clean.column')}
                  </Label>
                  <Input
                    id="clean-column"
                    value={data.column}
                    onChange={(e) => set({ column: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-case-mode" className="text-xs font-medium">
                    {t('canvas.clean.caseModeLabel')}
                  </Label>
                  <select
                    id="clean-case-mode"
                    value={data.caseMode}
                    onChange={(e) => set({ caseMode: e.target.value as CaseMode })}
                    className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                  >
                    {CASE_MODES.map((m) => (
                      <option key={m} value={m}>
                        {t(`canvas.clean.caseMode.${m}`)}
                      </option>
                    ))}
                  </select>
                </div>
              </>
            )}

            {data.blockKind === 'computed_column' && (
              <>
                <div>
                  <Label htmlFor="clean-output" className="text-xs font-medium">
                    {t('canvas.clean.output')}
                  </Label>
                  <Input
                    id="clean-output"
                    value={data.output}
                    onChange={(e) => set({ output: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-left" className="text-xs font-medium">
                    {t('canvas.clean.left')}
                  </Label>
                  <Input
                    id="clean-left"
                    value={data.left}
                    onChange={(e) => set({ left: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-compute-operator" className="text-xs font-medium">
                    {t('canvas.clean.computeOperatorLabel')}
                  </Label>
                  <select
                    id="clean-compute-operator"
                    value={data.computeOperator}
                    onChange={(e) => set({ computeOperator: e.target.value as ComputeOperator })}
                    className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                  >
                    {COMPUTE_OPERATORS.map((op) => (
                      <option key={op} value={op}>
                        {t(`canvas.clean.computeOperator.${op}`)}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <Label htmlFor="clean-right" className="text-xs font-medium">
                    {t('canvas.clean.right')}
                  </Label>
                  <Input
                    id="clean-right"
                    value={data.right}
                    onChange={(e) => set({ right: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
              </>
            )}

            {data.blockKind === 'sort' && (
              <>
                <div>
                  <Label htmlFor="clean-column" className="text-xs font-medium">
                    {t('canvas.clean.column')}
                  </Label>
                  <Input
                    id="clean-column"
                    value={data.column}
                    onChange={(e) => set({ column: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-direction" className="text-xs font-medium">
                    {t('canvas.clean.directionLabel')}
                  </Label>
                  <select
                    id="clean-direction"
                    value={data.direction}
                    onChange={(e) => set({ direction: e.target.value as SortDirection })}
                    className="mt-1.5 h-9 w-full rounded-md border border-input bg-transparent px-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                  >
                    {SORT_DIRECTIONS.map((d) => (
                      <option key={d} value={d}>
                        {t(`canvas.clean.direction.${d}`)}
                      </option>
                    ))}
                  </select>
                </div>
              </>
            )}

            {data.blockKind === 'aggregate' && (
              <>
                <div>
                  <Label htmlFor="clean-group-by" className="text-xs font-medium">
                    {t('canvas.clean.groupBy')}{' '}
                    <span className="text-muted-foreground">({t('common.optional')})</span>
                  </Label>
                  <Input
                    id="clean-group-by"
                    value={data.groupBy}
                    placeholder="cidade, categoria"
                    onChange={(e) => set({ groupBy: e.target.value })}
                    className="mt-1.5"
                  />
                </div>
                <div>
                  <Label htmlFor="clean-aggregations" className="text-xs font-medium">
                    {t('canvas.clean.aggregations')}
                  </Label>
                  <textarea
                    id="clean-aggregations"
                    value={data.aggregations}
                    onChange={(e) => set({ aggregations: e.target.value })}
                    rows={4}
                    spellCheck={false}
                    placeholder={'valor:sum:total_valor\nid:count:total_linhas'}
                    className="mt-1.5 w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
                  />
                  <p className="mt-1 text-[10px] text-muted-foreground">
                    {t('canvas.clean.aggregationsHint', {
                      functions: AGG_FUNCTIONS.join(', '),
                    })}
                  </p>
                </div>
              </>
            )}

            <CleanBlockPreview source={previewSourceSpec} blocks={previewBlocks} />
          </div>
        </div>
      </aside>
    )
  }

  return (
    <aside className="flex h-full w-full flex-col border-l bg-card">
      <div className="border-b border-white/10 px-4 py-3">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-fuchsia-400" />
          <h2 className="text-sm font-semibold text-foreground">{t('canvas.embedding')}</h2>
        </div>
        <p className="mt-0.5 text-[10px] text-muted-foreground">{t('canvas.embeddingDesc')}</p>
      </div>
      <div className="flex-1 overflow-auto p-4">
        <div className="flex flex-col gap-4">
          <div>
            <Label htmlFor="embedding-source-column" className="text-xs font-medium">
              {t('canvas.embeddingSourceColumn')}
            </Label>
            <Input
              id="embedding-source-column"
              value={data.sourceColumn}
              placeholder="body"
              onChange={(e) => onChange(node.id, { sourceColumn: e.target.value })}
              className="mt-1.5"
            />
          </div>
          <div>
            <Label htmlFor="embedding-output-column" className="text-xs font-medium">
              {t('canvas.embeddingOutputColumn')}
            </Label>
            <Input
              id="embedding-output-column"
              value={data.outputColumn}
              placeholder="embedding"
              onChange={(e) => onChange(node.id, { outputColumn: e.target.value })}
              className="mt-1.5"
            />
          </div>
          <div>
            <Label htmlFor="embedding-dimension" className="text-xs font-medium">
              {t('canvas.embeddingDimension')}
            </Label>
            <Input
              id="embedding-dimension"
              type="number"
              min={1}
              value={data.dimension || ''}
              placeholder="384"
              onChange={(e) => onChange(node.id, { dimension: Number(e.target.value) || 0 })}
              className="mt-1.5"
            />
          </div>

          <div className="h-px bg-white/10" />

          <div>
            <Label htmlFor="embedding-backend" className="text-xs font-medium">
              {t('canvas.embeddingBackend')}
            </Label>
            <select
              id="embedding-backend"
              value={data.backend}
              onChange={(e) => onChange(node.id, { backend: e.target.value as EmbeddingBackend })}
              className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-card px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
            >
              <option value="onnx">{t('canvas.embeddingBackendOnnx')}</option>
              <option value="api">{t('canvas.embeddingBackendApi')}</option>
            </select>
          </div>

          {data.backend === 'onnx' ? (
            <>
              <div>
                <Label htmlFor="embedding-repo" className="text-xs font-medium">
                  {t('canvas.embeddingRepo')}
                </Label>
                <Input
                  id="embedding-repo"
                  value={data.repo}
                  placeholder="sentence-transformers/all-MiniLM-L6-v2"
                  onChange={(e) => onChange(node.id, { repo: e.target.value })}
                  className="mt-1.5"
                />
              </div>
              <div>
                <Label htmlFor="embedding-revision" className="text-xs font-medium">
                  {t('canvas.embeddingRevision')}
                </Label>
                <Input
                  id="embedding-revision"
                  value={data.revision}
                  placeholder="main"
                  onChange={(e) => onChange(node.id, { revision: e.target.value })}
                  className="mt-1.5"
                />
              </div>
              <div>
                <Label htmlFor="embedding-filename" className="text-xs font-medium">
                  {t('canvas.embeddingFilename')}
                </Label>
                <Input
                  id="embedding-filename"
                  value={data.filename}
                  placeholder="model.onnx"
                  onChange={(e) => onChange(node.id, { filename: e.target.value })}
                  className="mt-1.5"
                />
              </div>
              <div>
                <Label htmlFor="embedding-tokenizer" className="text-xs font-medium">
                  {t('canvas.embeddingTokenizer')}
                </Label>
                <Input
                  id="embedding-tokenizer"
                  value={data.tokenizerFilename}
                  placeholder="tokenizer.json"
                  onChange={(e) => onChange(node.id, { tokenizerFilename: e.target.value })}
                  className="mt-1.5"
                />
              </div>
              <div>
                <Label htmlFor="embedding-max-length" className="text-xs font-medium">
                  {t('canvas.embeddingMaxLength')}
                </Label>
                <Input
                  id="embedding-max-length"
                  type="number"
                  min={1}
                  value={data.maxLength || ''}
                  placeholder="128"
                  onChange={(e) => onChange(node.id, { maxLength: Number(e.target.value) || 0 })}
                  className="mt-1.5"
                />
              </div>
            </>
          ) : (
            <>
              <div>
                <Label htmlFor="embedding-base-url" className="text-xs font-medium">
                  {t('canvas.embeddingBaseUrl')}
                </Label>
                <Input
                  id="embedding-base-url"
                  value={data.baseUrl}
                  placeholder="https://api.openai.com/v1"
                  onChange={(e) => onChange(node.id, { baseUrl: e.target.value })}
                  className="mt-1.5"
                />
              </div>
              <div>
                <Label htmlFor="embedding-model" className="text-xs font-medium">
                  {t('canvas.embeddingModel')}
                </Label>
                <Input
                  id="embedding-model"
                  value={data.model}
                  placeholder="text-embedding-3-small"
                  onChange={(e) => onChange(node.id, { model: e.target.value })}
                  className="mt-1.5"
                />
              </div>
              <div>
                <Label htmlFor="embedding-api-key-env" className="text-xs font-medium">
                  {t('canvas.embeddingApiKeyEnv')}{' '}
                  <span className="text-muted-foreground">({t('common.optional')})</span>
                </Label>
                <Input
                  id="embedding-api-key-env"
                  value={data.apiKeyEnv}
                  placeholder="OPENAI_API_KEY"
                  onChange={(e) => onChange(node.id, { apiKeyEnv: e.target.value })}
                  className="mt-1.5"
                />
              </div>
            </>
          )}

          <div className="h-px bg-white/10" />

          <div>
            <Label htmlFor="embedding-strategy" className="text-xs font-medium">
              {t('canvas.embeddingStrategy')}
            </Label>
            <select
              id="embedding-strategy"
              value={data.strategy}
              onChange={(e) => onChange(node.id, { strategy: e.target.value as ChunkingStrategy })}
              className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-card px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
            >
              <option value="fixed_window">{t('canvas.embeddingStrategyFixed')}</option>
              <option value="recursive_character">{t('canvas.embeddingStrategyRecursive')}</option>
              <option value="semantic">{t('canvas.embeddingStrategySemantic')}</option>
            </select>
          </div>
          {data.strategy === 'semantic' ? (
            <div>
              <Label htmlFor="embedding-similarity-threshold" className="text-xs font-medium">
                {t('canvas.embeddingSimilarityThreshold')}
              </Label>
              <Input
                id="embedding-similarity-threshold"
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={data.similarityThreshold}
                placeholder="0.8"
                onChange={(e) =>
                  onChange(node.id, { similarityThreshold: Number(e.target.value) || 0 })
                }
                className="mt-1.5"
              />
              <p className="mt-1 text-[10px] text-muted-foreground">
                {t('canvas.embeddingSimilarityThresholdHint')}
              </p>
            </div>
          ) : (
            <div className="flex gap-3">
              <div className="flex-1">
                <Label htmlFor="embedding-chunk-size" className="text-xs font-medium">
                  {t('canvas.embeddingChunkSize')}
                </Label>
                <Input
                  id="embedding-chunk-size"
                  type="number"
                  min={1}
                  value={data.chunkSize || ''}
                  placeholder="256"
                  onChange={(e) => onChange(node.id, { chunkSize: Number(e.target.value) || 0 })}
                  className="mt-1.5"
                />
              </div>
              <div className="flex-1">
                <Label htmlFor="embedding-overlap" className="text-xs font-medium">
                  {t('canvas.embeddingOverlap')}
                </Label>
                <Input
                  id="embedding-overlap"
                  type="number"
                  min={0}
                  value={data.overlap}
                  placeholder="0"
                  onChange={(e) => onChange(node.id, { overlap: Number(e.target.value) || 0 })}
                  className="mt-1.5"
                />
              </div>
            </div>
          )}
          {data.strategy === 'recursive_character' && (
            <div>
              <Label htmlFor="embedding-separators" className="text-xs font-medium">
                {t('canvas.embeddingSeparators')}{' '}
                <span className="text-muted-foreground">({t('common.optional')})</span>
              </Label>
              <textarea
                id="embedding-separators"
                value={data.separators}
                onChange={(e) => onChange(node.id, { separators: e.target.value })}
                rows={4}
                spellCheck={false}
                placeholder={'\\n\\n\\n " "'}
                className="mt-1.5 w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
              />
              <p className="mt-1 text-[10px] text-muted-foreground">
                {t('canvas.embeddingSeparatorsHint')}
              </p>
            </div>
          )}
        </div>
      </div>
    </aside>
  )
}
