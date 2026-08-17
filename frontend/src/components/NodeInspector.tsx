import type {
  ChunkingStrategy,
  ConnectorNodeData,
  ConnectorRole,
  DagNode,
  DbtCommand,
  DbtNodeData,
  EmbeddingBackend,
  EmbeddingNodeData,
  PythonNodeData,
  TransformNodeData,
} from '@/lib/dag'
import type { ConnectorDescriptor } from '@/lib/api'
import { useI18n } from '@/lib/i18n'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SchemaForm } from '@/components/SchemaForm'
import { Database, Code2, Layers, Sparkles, Terminal } from 'lucide-react'

interface NodeInspectorProps {
  node: DagNode
  connectors: ConnectorDescriptor[]
  onChange: (
    id: string,
    data:
      | Partial<ConnectorNodeData>
      | Partial<TransformNodeData>
      | Partial<DbtNodeData>
      | Partial<EmbeddingNodeData>
      | Partial<PythonNodeData>,
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
/** Strips a trailing `-cdc` suffix, e.g. `"postgres-cdc"` -> `"postgres"`. */
function batchNameOf(connector: string): string {
  return connector.endsWith('-cdc') ? connector.slice(0, -'-cdc'.length) : connector
}

export function NodeInspector({ node, connectors, onChange }: NodeInspectorProps) {
  const { t } = useI18n()
  const data = node.data
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

  if (data.kind === 'dbt') {
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
      <aside className="flex w-80 shrink-0 flex-col border-l bg-card">
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
              <Label htmlFor="python-script" className="text-xs font-medium">
                {t('canvas.pythonScript')}
              </Label>
              <textarea
                id="python-script"
                value={data.script}
                onChange={(e) => onChange(node.id, { script: e.target.value })}
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

  return (
    <aside className="flex w-80 shrink-0 flex-col border-l bg-card">
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
            </select>
          </div>
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
