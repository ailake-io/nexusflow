import type { Edge, Node } from '@xyflow/react'

/** Matches nexus-core::NodeSpec exactly (crates/nexus-core/src/dag.rs). */
export interface NodeSpec {
  name?: string
  connector: string
  config: unknown
}

/** Matches nexus-core::TransformSpec exactly. */
export interface TransformSpec {
  sql: string
}

/** Matches nexus-core::QualityCheckKind exactly — `#[serde(tag = "kind",
 * rename_all = "snake_case")]`, so each variant is `{ kind: "<name>", ...
 * fields }` with no wrapper. */
export type QualityCheckKind =
  | { kind: 'not_null' }
  | { kind: 'unique' }
  | { kind: 'min'; min: number }
  | { kind: 'max'; max: number }
  | { kind: 'accepted_values'; values: string[] }
  /** Fase 27 — checks the pipeline's total output row count, not a named
   *  column (`QualityCheckSpec.column` is ignored for this kind). Either
   *  bound optional; `undefined` means unbounded on that side. */
  | { kind: 'row_count'; min?: number; max?: number }

/** Matches nexus-core::QualityCheckSpec exactly. */
export interface QualityCheckSpec {
  column: string
  check: QualityCheckKind
}

/** Matches nexus-core::PythonTransformSpec exactly — a cleaning/
 * transformation stage run as an isolated `python3` subprocess (mirrors
 * `dbt` in isolation model, not in when it runs — chains after `transform`
 * like a DAG stage, not as a post-load step). */
export interface PythonTransformSpec {
  script: string
  timeout_seconds?: number
}

/** Matches nexus-core::FilterOperator exactly. */
export type FilterOperator =
  | 'eq'
  | 'ne'
  | 'gt'
  | 'lt'
  | 'gte'
  | 'lte'
  | 'contains'
  | 'starts_with'
  | 'is_null'
  | 'is_not_null'

/** Matches nexus-core::SelectColumnsMode exactly. */
export type SelectColumnsMode = 'keep' | 'drop'

/** Matches nexus-core::CaseMode exactly. */
export type CaseMode = 'upper' | 'lower' | 'title'

/** Matches nexus-core::CastType exactly. */
export type CastType = 'int' | 'float' | 'text' | 'date' | 'boolean'

/** Matches nexus-core::ComputeOperator exactly. */
export type ComputeOperator = 'add' | 'subtract' | 'multiply' | 'divide' | 'concat'

/** Matches nexus-core::SortDirection exactly. */
export type SortDirection = 'asc' | 'desc'

/** Matches nexus-core::AggFunction exactly. */
export type AggFunction = 'sum' | 'avg' | 'count' | 'count_distinct' | 'min' | 'max'

/** Matches nexus-core::Aggregation exactly. */
export interface Aggregation {
  column: string
  function: AggFunction
  output: string
}

/** Matches nexus-core::NullFillStrategy exactly — `#[serde(tag =
 * "strategy")]`, flattened into the `fill_nulls` block below. `column` here
 * would collide with `fill_nulls`' own `column` (the target column) at the
 * same JSON level — that's why the Rust side calls this one
 * `fallback_column`, not `column` (a real bug caught by a round-trip test). */
export type NullFillStrategy =
  | { strategy: 'value'; value: string }
  | { strategy: 'column_average' }
  | { strategy: 'other_column'; fallback_column: string }

/** Matches nexus-core::CleanBlockKind exactly — `#[serde(tag = "kind")]`.
 * Fase 30's no-code alternative to the `transform`/`python` nodes: each
 * variant is a configurable operation `compile_clean_blocks` (nexus-core)
 * turns into a SQL fragment, chained as CTEs. */
export type CleanBlockKind =
  | { kind: 'filter'; column: string; operator: FilterOperator; value?: string }
  | { kind: 'select_columns'; mode: SelectColumnsMode; columns: string[] }
  | { kind: 'rename'; from: string; to: string }
  | { kind: 'cast'; column: string; data_type: CastType }
  | { kind: 'trim'; columns: string[] }
  | { kind: 'replace_text'; column: string; find: string; replace: string }
  | ({ kind: 'fill_nulls'; column: string } & NullFillStrategy)
  | { kind: 'drop_nulls'; columns: string[] }
  | { kind: 'dedupe'; columns: string[] }
  | { kind: 'change_case'; column: string; mode: CaseMode }
  | {
      kind: 'computed_column'
      output: string
      left: string
      operator: ComputeOperator
      right: string
    }
  | { kind: 'sort'; column: string; direction: SortDirection }
  | { kind: 'aggregate'; group_by: string[]; aggregations: Aggregation[] }

export type CleanBlockKindTag = CleanBlockKind['kind']

/** Matches nexus-core::CleanBlockSpec exactly (`#[serde(flatten)]` on
 * `kind`, so `name` and the tagged variant's fields sit at the same JSON
 * level). */
export type CleanBlockSpec = { name?: string } & CleanBlockKind

/** Matches nexus-core::DbtCommand exactly. */
export type DbtCommand = 'run' | 'build' | 'test'

/**
 * Matches nexus-core::DbtConfig exactly — ELT mode (Marco 10): dbt runs
 * against the sink warehouse via SQL *after* the raw load succeeds, so this
 * is not a DAG transform node (dbt never touches this pipeline's Arrow
 * batches), just an optional post-load step on the spec itself.
 */
export interface DbtConfig {
  project_dir: string
  command: DbtCommand
  select?: string
}

/** Matches nexus-core::EmbeddingModelSpec exactly — internally tagged on
 * `backend` (`#[serde(tag = "backend")]`), so the JSON carries the
 * discriminator inline rather than as a wrapper key. */
export type EmbeddingModelSpec =
  | {
      backend: 'onnx'
      repo: string
      /** Git revision (branch, tag or commit) inside the HF repo. */
      revision: string
      filename: string
      tokenizer_filename: string
      max_length: number
    }
  | { backend: 'api'; base_url: string; model: string; api_key_env?: string }

/** Matches nexus-core::ChunkingSpec exactly — tagged on `strategy`. */
export type ChunkingSpec =
  | { strategy: 'fixed_window'; chunk_size: number; overlap?: number }
  | { strategy: 'recursive_character'; chunk_size: number; overlap?: number; separators?: string[] }
  | { strategy: 'semantic'; similarity_threshold: number }

/** Matches nexus-core::EmbeddingSpec exactly. */
export interface EmbeddingSpec {
  source_column: string
  output_column: string
  dimension: number
  model: EmbeddingModelSpec
  chunking: ChunkingSpec
}

/**
 * Matches nexus-core::PipelineSpec exactly — this is the JSON the backend's
 * `PipelineSpec::parse` / `Json<PipelineSpec>` extractor deserializes
 * (crates/nexus-core/src/dag.rs). Field names, optionality and defaults must
 * stay byte-for-byte in sync with the Rust struct; never add a field the
 * backend doesn't know about.
 */
export interface PipelineSpec {
  pipeline_id: string
  sources: NodeSpec[]
  transform?: TransformSpec
  sinks: NodeSpec[]
  /** Chunking + embedding stage, applied before the transform (or before
   * the sinks, if there's no transform) — CLAUDE.md §4.3. */
  embedding?: EmbeddingSpec
  /** Cleaning/transformation stage, chained after `transform` (if present)
   * and before the sinks — CLAUDE.md §4.4. */
  python?: PythonTransformSpec
  channel_capacity?: number
  partitions?: number
  dbt?: DbtConfig
  /** Cron expression (5-field Unix or 6-field Quartz) — automatic runs via
   * the server's scheduler. Unset means the pipeline only runs when
   * explicitly triggered. */
  schedule?: string
  /** Upstream pipeline ids this one waits on before an automatic run starts
   * (Fase 26) — empty/unset means no dependency-based triggering.
   * Orthogonal to `schedule` above. */
  depends_on?: { upstream_pipeline_id: string }[]
  /** How multiple `depends_on` entries combine — meaningless with 0 or 1
   * entries. Matches nexus-core::DependencyMode's `#[serde(rename_all =
   * "snake_case")]`. */
  dependency_mode?: 'any' | 'all'
  /** Per-pipeline alert channels, additive to the global env-var-configured
   * ones. Unset means no per-pipeline channels. */
  alerts?: AlertsConfig
  /** Native (dbt-independent) quality checks, evaluated against the
   * pipeline's materialized output — only takes effect on a pipeline with a
   * Transform node (see nexus_core::quality's doc comment). Empty/unset
   * means no checks configured. */
  quality_checks?: QualityCheckSpec[]
  /** Opt-in (Fase 27): fire an alert (through `alerts` above) when this
   * pipeline's output row count is a statistical outlier against its own
   * run history. `false`/unset means row-count history is still tracked,
   * just never alerts on it. */
  anomaly_alerts?: boolean
  /** Deterministic column tokenization (Fase 28) — applied before the SQL
   * transform (if present) and before the sink(s). Matches
   * nexus-core::column_masking::ColumnMaskingSpec exactly. Requires
   * NEXUS_MASKING_SALT to be configured server-side; saving a pipeline
   * with a non-empty list here on a server without that salt set fails at
   * save time. Empty/unset means no masking. */
  masking?: MaskingSpec[]
  /** When true, the spec is saved as a draft and the server skips validation
   * of connector configs/embedding/dbt. Drafts cannot be executed. */
  draft?: boolean
  /** Fase 30 — no-code alternative to `transform`/`python`: an ordered
   * chain of configurable blocks, compiled server-side into the same kind
   * of SQL a hand-written `transform.sql` would be. Mutually exclusive with
   * `transform`/`python`; requires exactly 1 source. Empty/unset means no
   * blocks, same as before this field existed. */
  clean_blocks?: CleanBlockSpec[]
}

/** Matches nexus-core::column_masking::ColumnMaskingSpec exactly. */
export interface MaskingSpec {
  column: string
}

/** Matches nexus-core::WebhookAlertChannel exactly — Slack/Teams/generic
 * Webhook all share this shape (POST a JSON payload built server-side to
 * `url`). `on_failure` defaults to `true` server-side when omitted; the UI
 * always sends both explicitly. */
export interface WebhookAlertChannel {
  url: string
  on_success: boolean
  on_failure: boolean
}

/** Matches nexus-core::PagerDutyAlertChannel exactly. */
export interface PagerDutyAlertChannel {
  routing_key: string
  on_success: boolean
  on_failure: boolean
}

/** Matches nexus-core::EmailAlertChannel exactly. */
export interface EmailAlertChannel {
  smtp_host: string
  smtp_port: number
  username?: string
  password?: string
  from: string
  to: string[]
  on_success: boolean
  on_failure: boolean
}

/** Matches nexus-core::AlertsConfig exactly — per-pipeline alert channels,
 * additive to nexus-server's global env-var-configured channels (which stay
 * failure-only). */
export interface AlertsConfig {
  slack?: WebhookAlertChannel
  teams?: WebhookAlertChannel
  webhook?: WebhookAlertChannel
  pagerduty?: PagerDutyAlertChannel
  email?: EmailAlertChannel
}

export type ConnectorRole = 'source' | 'sink'

export interface ConnectorNodeData extends Record<string, unknown> {
  kind: 'connector'
  connector: string
  role: ConnectorRole
  name: string
  /** Raw JSON text, edited freely in the inspector — parsed on export. */
  config: string
}

export interface TransformNodeData extends Record<string, unknown> {
  kind: 'transform'
  sql: string
}

/** Canvas form of `DbtConfig` — `select`/`projectDir` stay as plain strings
 * (not `string | undefined`) so the inspector's text inputs have something
 * controlled to bind to; `toPipelineSpec` trims/omits empty ones. */
export interface DbtNodeData extends Record<string, unknown> {
  kind: 'dbt'
  projectDir: string
  command: DbtCommand
  select: string
}

/** Canvas form of `PythonTransformSpec` — `timeoutSeconds` stays a plain
 * number (0 means "unset", same convention `toPipelineSpec` uses for
 * `channelCapacity`/`partitions`), so the inspector's number input has
 * something controlled to bind to. */
export interface PythonNodeData extends Record<string, unknown> {
  kind: 'python'
  script: string
  timeoutSeconds: number
}

export type EmbeddingBackend = 'onnx' | 'api'
export type ChunkingStrategy = 'fixed_window' | 'recursive_character' | 'semantic'

/**
 * Canvas form of `EmbeddingSpec` — like `DbtNodeData`, every field is a
 * plain controlled-input value (strings/numbers, never `| undefined`), even
 * the ones that only apply to one `backend`/`strategy` variant. The
 * inspector shows only the subset that matches the currently-selected
 * `backend`/`strategy`; `toPipelineSpec` builds the correctly-tagged union
 * member from those and drops the rest.
 */
export interface EmbeddingNodeData extends Record<string, unknown> {
  kind: 'embedding'
  sourceColumn: string
  outputColumn: string
  dimension: number
  backend: EmbeddingBackend
  // backend: 'onnx'
  repo: string
  revision: string
  filename: string
  tokenizerFilename: string
  maxLength: number
  // backend: 'api'
  baseUrl: string
  model: string
  apiKeyEnv: string
  // chunking (shared by both strategies)
  strategy: ChunkingStrategy
  chunkSize: number
  overlap: number
  similarityThreshold: number
  // strategy: 'recursive_character' only — one separator per line
  separators: string
}

/**
 * Canvas form of `CleanBlockSpec` — same "flat superset, inspector shows
 * only the relevant subset" pattern `EmbeddingNodeData` already uses for
 * its own multi-variant shape. `blockKind` picks which fields matter;
 * `columns` is a single comma-separated string shared by every block kind
 * that takes a column *list* (trim/drop_nulls/dedupe/select_columns),
 * parsed in `toCleanBlockSpec`. `aggregations` is a small text mini-DSL
 * (`col:function:output`, one per line) rather than a repeatable sub-form —
 * simplest thing that works for a config surface this deep; a real
 * multi-row editor is a reasonable follow-up, not done here.
 */
export interface CleanBlockNodeData extends Record<string, unknown> {
  kind: 'clean'
  blockKind: CleanBlockKindTag
  name: string
  // filter / cast / replace_text / fill_nulls / change_case / sort (single column)
  column: string
  operator: FilterOperator
  value: string
  // select_columns / trim / drop_nulls / dedupe (column list)
  selectMode: SelectColumnsMode
  columns: string
  // rename
  from: string
  to: string
  // cast
  dataType: CastType
  // replace_text
  find: string
  replace: string
  // fill_nulls
  nullStrategy: NullFillStrategy['strategy']
  fallbackColumn: string
  // change_case
  caseMode: CaseMode
  // computed_column
  output: string
  left: string
  computeOperator: ComputeOperator
  right: string
  // sort
  direction: SortDirection
  // aggregate
  groupBy: string
  aggregations: string
}

export type DagNodeData =
  | ConnectorNodeData
  | TransformNodeData
  | DbtNodeData
  | EmbeddingNodeData
  | PythonNodeData
  | CleanBlockNodeData
export type DagNode = Node<DagNodeData>

export function isConnectorNode(node: DagNode): node is Node<ConnectorNodeData> {
  return node.data.kind === 'connector'
}

export function isTransformNode(node: DagNode): node is Node<TransformNodeData> {
  return node.data.kind === 'transform'
}

export function isDbtNode(node: DagNode): node is Node<DbtNodeData> {
  return node.data.kind === 'dbt'
}

export function isPythonNode(node: DagNode): node is Node<PythonNodeData> {
  return node.data.kind === 'python'
}

export function isEmbeddingNode(node: DagNode): node is Node<EmbeddingNodeData> {
  return node.data.kind === 'embedding'
}

export function isCleanBlockNode(node: DagNode): node is Node<CleanBlockNodeData> {
  return node.data.kind === 'clean'
}

export class DagSerializationError extends Error {}

/** Minimal translation function injected from the React i18n layer. */
export type DagTranslator = (key: string, vars?: Record<string, string | number>) => string

export interface PipelineMeta {
  pipelineId: string
  channelCapacity?: number
  partitions?: number
  schedule?: string
  /** Plain pipeline ids (canvas form of `PipelineSpec.depends_on`, which
   * wraps each one in `{upstream_pipeline_id}`) — Fase 26. */
  dependsOn?: string[]
  dependencyMode?: 'any' | 'all'
  alerts?: AlertsConfig
  qualityChecks?: QualityCheckSpec[]
  anomalyAlerts?: boolean
  /** Plain column names (canvas form of `PipelineSpec.masking`, which
   * wraps each one in `{column}`) — Fase 28. */
  maskedColumns?: string[]
}

/**
 * Canvas (nodes/edges) -> PipelineSpec JSON. Mirrors the validation in
 * `PipelineSpec::validate` (dag.rs) so obviously-invalid graphs are rejected
 * client-side with the same rules, instead of round-tripping to the server
 * to find out.
 */
export function toPipelineSpec(
  nodes: DagNode[],
  meta: PipelineMeta,
  allowDraft = false,
  t: DagTranslator = defaultT,
): PipelineSpec {
  const err = (key: keyof typeof EN_DAG_ERRORS, vars?: Record<string, string | number>) => {
    throw new DagSerializationError(t(`dag.errors.${key}`, vars))
  }

  if (!meta.pipelineId.trim()) {
    err('pipelineIdEmpty')
  }

  const connectorNodes = nodes.filter(isConnectorNode)
  const transformNodes = nodes.filter(isTransformNode)
  const dbtNodes = nodes.filter(isDbtNode)
  const embeddingNodes = nodes.filter(isEmbeddingNode)
  const pythonNodes = nodes.filter(isPythonNode)
  // Order = left-to-right canvas position, not edges — same convention
  // `fromPipelineSpec` uses when laying blocks back out.
  const cleanNodes = nodes
    .filter(isCleanBlockNode)
    .slice()
    .sort((a, b) => a.position.x - b.position.x)
  if (!allowDraft) {
    if (transformNodes.length > 1) {
      err('atMostOneTransform')
    }
    if (dbtNodes.length > 1) {
      err('atMostOneDbt')
    }
    if (embeddingNodes.length > 1) {
      err('atMostOneEmbedding')
    }
    if (pythonNodes.length > 1) {
      err('atMostOnePython')
    }
    if (cleanNodes.length > 0 && (transformNodes.length > 0 || pythonNodes.length > 0)) {
      err('cleanBlocksExclusiveWithTransformOrPython')
    }
  }

  const sources = connectorNodes
    .filter((n) => n.data.role === 'source')
    .map((n) => toNodeSpec(n, allowDraft, t))
    .filter((s): s is NodeSpec => s !== undefined)
  const sinks = connectorNodes
    .filter((n) => n.data.role === 'sink')
    .map((n) => toNodeSpec(n, allowDraft, t))
    .filter((s): s is NodeSpec => s !== undefined)

  if (!allowDraft) {
    if (sources.length === 0) {
      err('sourcesEmpty')
    }
    if (sinks.length === 0) {
      err('sinksEmpty')
    }
    if (cleanNodes.length > 0 && sources.length !== 1) {
      err('cleanBlocksRequiresOneSource')
    }
  }

  const transform =
    transformNodes.length === 1 ? { sql: transformNodes[0].data.sql } : undefined

  const python: PythonTransformSpec | undefined =
    pythonNodes.length === 1 ? { script: pythonNodes[0].data.script } : undefined
  if (python && pythonNodes[0].data.timeoutSeconds > 0) {
    python.timeout_seconds = pythonNodes[0].data.timeoutSeconds
  }

  if (!allowDraft) {
    if (
      !transform &&
      !python &&
      cleanNodes.length === 0 &&
      (sources.length !== 1 || sinks.length !== 1)
    ) {
      err('strictLinearWithoutTransform')
    }
    if (!transform && python && (sources.length !== 1 || sinks.length !== 1)) {
      err('pythonRequiresLinearWithoutTransform')
    }
    if (transform && !transform.sql.trim()) {
      err('transformSqlEmpty')
    }
    if (python && !python.script.trim()) {
      err('pythonScriptEmpty')
    }
  }

  let dbt: DbtConfig | undefined
  if (dbtNodes.length === 1) {
    const data = dbtNodes[0].data
    if (!allowDraft && !data.projectDir.trim()) {
      err('dbtProjectDirEmpty')
    }
    if (data.projectDir.trim()) {
      dbt = { project_dir: data.projectDir.trim(), command: data.command }
      if (data.select.trim()) dbt.select = data.select.trim()
    }
  }

  const embedding =
    embeddingNodes.length === 1
      ? toEmbeddingSpec(embeddingNodes[0].data, allowDraft, t)
      : undefined

  const cleanBlocks = cleanNodes.map((n) => toCleanBlockSpec(n.data, allowDraft, t))

  const spec: PipelineSpec = {
    pipeline_id: meta.pipelineId,
    sources,
    sinks,
  }
  if (transform) spec.transform = transform
  if (embedding) spec.embedding = embedding
  if (python) spec.python = python
  if (dbt) spec.dbt = dbt
  if (cleanBlocks.length > 0) spec.clean_blocks = cleanBlocks
  if (meta.channelCapacity !== undefined) spec.channel_capacity = meta.channelCapacity
  if (meta.partitions !== undefined) spec.partitions = meta.partitions
  if (meta.schedule?.trim()) spec.schedule = meta.schedule.trim()
  if (meta.dependsOn && meta.dependsOn.length > 0) {
    spec.depends_on = meta.dependsOn.map((upstream_pipeline_id) => ({ upstream_pipeline_id }))
    if (meta.dependencyMode) spec.dependency_mode = meta.dependencyMode
  }
  if (meta.alerts) spec.alerts = meta.alerts
  if (meta.qualityChecks && meta.qualityChecks.length > 0) {
    spec.quality_checks = meta.qualityChecks
  }
  if (meta.anomalyAlerts) spec.anomaly_alerts = true
  if (meta.maskedColumns && meta.maskedColumns.length > 0) {
    spec.masking = meta.maskedColumns.map((column) => ({ column }))
  }
  if (allowDraft) spec.draft = true
  return spec
}

const EN_DAG_ERRORS = {
  pipelineIdEmpty: 'pipeline_id must not be empty',
  atMostOneTransform: 'at most one transform node is allowed',
  atMostOneDbt: 'at most one dbt node is allowed',
  atMostOneEmbedding: 'at most one embedding node is allowed',
  atMostOnePython: 'at most one python node is allowed',
  sourcesEmpty: 'sources must not be empty',
  sinksEmpty: 'sinks must not be empty',
  strictLinearWithoutTransform:
    'without a transform, the pipeline must be strictly linear: exactly 1 source and 1 sink',
  pythonRequiresLinearWithoutTransform:
    'without a SQL transform, a python node still requires exactly 1 source and 1 sink',
  transformSqlEmpty: 'transform.sql must not be empty',
  pythonScriptEmpty: 'python node: script must not be empty',
  dbtProjectDirEmpty: 'dbt node: project_dir must not be empty',
  embeddingSourceColumnEmpty: 'embedding node: source_column must not be empty',
  embeddingOutputColumnEmpty: 'embedding node: output_column must not be empty',
  embeddingDimensionInvalid: 'embedding node: dimension must be > 0',
  embeddingOnnxFieldsRequired:
    'embedding node: repo, filename and tokenizer_filename are required for the onnx backend',
  embeddingOnnxMaxLengthInvalid: 'embedding node: max_length must be > 0',
  embeddingApiFieldsRequired:
    'embedding node: base_url and model are required for the api backend',
  embeddingChunkSizeInvalid: 'embedding node: chunk_size must be > 0',
  embeddingSemanticThresholdInvalid:
    'embedding node: similarity_threshold must be between 0.0 and 1.0',
  configNotValidJson: 'node "{name}": config is not valid JSON',
  connectorNameEmpty: 'every connector node needs a connector name',
  cleanBlocksExclusiveWithTransformOrPython:
    'a clean block cannot be combined with a transform or python node — pick one way to describe the transform stage',
  cleanBlocksRequiresOneSource: 'clean blocks require exactly 1 source (fan-in is not supported yet)',
  cleanBlockColumnEmpty: 'clean block: column must not be empty',
  cleanBlockColumnListEmpty: 'clean block: at least one column is required',
  cleanBlockFilterValueRequired: 'clean block: this operator requires a value',
  cleanBlockFillValueRequired: 'clean block: a fill value is required',
  cleanBlockAggregationMalformed: 'clean block: each aggregation line must be "column:function:output"',
  cleanBlockAggregateEmpty: 'clean block: set at least a group-by column or one aggregation',
}

function defaultT(key: string, vars?: Record<string, string | number>): string {
  const map: Record<string, string> = EN_DAG_ERRORS
  const short = key.replace('dag.errors.', '')
  let value = map[short] ?? key
  if (!vars) return value
  return value.replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? `{${name}}`))
}

function toEmbeddingSpec(
  data: EmbeddingNodeData,
  allowDraft = false,
  t: DagTranslator = defaultT,
): EmbeddingSpec | undefined {
  const err = (key: keyof typeof EN_DAG_ERRORS, vars?: Record<string, string | number>) => {
    throw new DagSerializationError(t(`dag.errors.${key}`, vars))
  }

  if (!allowDraft) {
    if (!data.sourceColumn.trim()) {
      err('embeddingSourceColumnEmpty')
    }
    if (!data.outputColumn.trim()) {
      err('embeddingOutputColumnEmpty')
    }
    if (!(data.dimension > 0)) {
      err('embeddingDimensionInvalid')
    }
  }

  let model: EmbeddingModelSpec | undefined
  if (data.backend === 'onnx') {
    const hasOnnxFields = data.repo.trim() && data.filename.trim() && data.tokenizerFilename.trim()
    if (!allowDraft && !hasOnnxFields) {
      err('embeddingOnnxFieldsRequired')
    }
    if (!allowDraft && !(data.maxLength > 0)) {
      err('embeddingOnnxMaxLengthInvalid')
    }
    if (hasOnnxFields) {
      model = {
        backend: 'onnx',
        repo: data.repo.trim(),
        revision: data.revision.trim() || 'main',
        filename: data.filename.trim(),
        tokenizer_filename: data.tokenizerFilename.trim(),
        max_length: data.maxLength,
      }
    }
  } else {
    const hasApiFields = data.baseUrl.trim() && data.model.trim()
    if (!allowDraft && !hasApiFields) {
      err('embeddingApiFieldsRequired')
    }
    if (hasApiFields) {
      model = { backend: 'api', base_url: data.baseUrl.trim(), model: data.model.trim() }
      if (data.apiKeyEnv.trim()) model.api_key_env = data.apiKeyEnv.trim()
    }
  }

  if (!allowDraft && data.strategy !== 'semantic' && !(data.chunkSize > 0)) {
    err('embeddingChunkSizeInvalid')
  }
  if (
    !allowDraft &&
    data.strategy === 'semantic' &&
    !(data.similarityThreshold >= 0 && data.similarityThreshold <= 1)
  ) {
    err('embeddingSemanticThresholdInvalid')
  }

  if (!model) return undefined

  let chunking: ChunkingSpec
  if (data.strategy === 'fixed_window') {
    chunking = { strategy: 'fixed_window', chunk_size: data.chunkSize, overlap: data.overlap }
  } else if (data.strategy === 'semantic') {
    chunking = {
      strategy: 'semantic',
      similarity_threshold: data.similarityThreshold,
    }
  } else {
    const separators = data.separators
      .split('\n')
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
    chunking = {
      strategy: 'recursive_character',
      chunk_size: data.chunkSize,
      overlap: data.overlap,
      ...(separators.length > 0 ? { separators } : {}),
    }
  }

  return {
    source_column: data.sourceColumn.trim(),
    output_column: data.outputColumn.trim(),
    dimension: data.dimension,
    model,
    chunking,
  }
}

/** Splits a comma-separated column list into trimmed, non-empty names —
 * shared by every block kind that takes a column *list*
 * (select_columns/trim/drop_nulls/dedupe). */
function parseColumnList(raw: string): string[] {
  return raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
}

/** Parses the `aggregations` mini-DSL (`column:function:output`, one per
 * line) into `Aggregation[]`. See `CleanBlockNodeData`'s doc comment for
 * why this is a text format rather than a repeatable sub-form. */
function parseAggregations(raw: string, err: (key: keyof typeof EN_DAG_ERRORS) => never): Aggregation[] {
  return raw
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => {
      const parts = line.split(':').map((p) => p.trim())
      if (parts.length !== 3 || !parts[0] || !parts[2]) {
        err('cleanBlockAggregationMalformed')
      }
      const [column, fn, output] = parts
      return { column, function: fn as AggFunction, output }
    })
}

export function toCleanBlockSpec(
  data: CleanBlockNodeData,
  allowDraft = false,
  t: DagTranslator = defaultT,
): CleanBlockSpec {
  const err = (key: keyof typeof EN_DAG_ERRORS, vars?: Record<string, string | number>): never => {
    throw new DagSerializationError(t(`dag.errors.${key}`, vars))
  }
  const name = data.name.trim() || undefined
  const requireColumn = (value: string) => {
    if (!allowDraft && !value.trim()) err('cleanBlockColumnEmpty')
    return value.trim()
  }

  switch (data.blockKind) {
    case 'filter': {
      const column = requireColumn(data.column)
      const needsValue = data.operator !== 'is_null' && data.operator !== 'is_not_null'
      if (!allowDraft && needsValue && !data.value.trim()) err('cleanBlockFilterValueRequired')
      return {
        name,
        kind: 'filter',
        column,
        operator: data.operator,
        ...(needsValue ? { value: data.value.trim() } : {}),
      }
    }
    case 'select_columns': {
      const columns = parseColumnList(data.columns)
      if (!allowDraft && columns.length === 0) err('cleanBlockColumnListEmpty')
      return { name, kind: 'select_columns', mode: data.selectMode, columns }
    }
    case 'rename': {
      const from = requireColumn(data.from)
      if (!allowDraft && !data.to.trim()) err('cleanBlockColumnEmpty')
      return { name, kind: 'rename', from, to: data.to.trim() }
    }
    case 'cast':
      return { name, kind: 'cast', column: requireColumn(data.column), data_type: data.dataType }
    case 'trim': {
      const columns = parseColumnList(data.columns)
      if (!allowDraft && columns.length === 0) err('cleanBlockColumnListEmpty')
      return { name, kind: 'trim', columns }
    }
    case 'replace_text':
      return {
        name,
        kind: 'replace_text',
        column: requireColumn(data.column),
        find: data.find,
        replace: data.replace,
      }
    case 'fill_nulls': {
      const column = requireColumn(data.column)
      if (data.nullStrategy === 'value') {
        if (!allowDraft && !data.value.trim()) err('cleanBlockFillValueRequired')
        return { name, kind: 'fill_nulls', column, strategy: 'value', value: data.value.trim() }
      }
      if (data.nullStrategy === 'other_column') {
        const fallbackColumn = data.fallbackColumn.trim()
        if (!allowDraft && !fallbackColumn) err('cleanBlockColumnEmpty')
        return {
          name,
          kind: 'fill_nulls',
          column,
          strategy: 'other_column',
          fallback_column: fallbackColumn,
        }
      }
      return { name, kind: 'fill_nulls', column, strategy: 'column_average' }
    }
    case 'drop_nulls': {
      const columns = parseColumnList(data.columns)
      if (!allowDraft && columns.length === 0) err('cleanBlockColumnListEmpty')
      return { name, kind: 'drop_nulls', columns }
    }
    case 'dedupe':
      return { name, kind: 'dedupe', columns: parseColumnList(data.columns) }
    case 'change_case':
      return {
        name,
        kind: 'change_case',
        column: requireColumn(data.column),
        mode: data.caseMode,
      }
    case 'computed_column': {
      if (!allowDraft && !data.output.trim()) err('cleanBlockColumnEmpty')
      const left = requireColumn(data.left)
      if (!allowDraft && !data.right.trim()) err('cleanBlockColumnEmpty')
      return {
        name,
        kind: 'computed_column',
        output: data.output.trim(),
        left,
        operator: data.computeOperator,
        right: data.right.trim(),
      }
    }
    case 'sort':
      return {
        name,
        kind: 'sort',
        column: requireColumn(data.column),
        direction: data.direction,
      }
    case 'aggregate': {
      const groupBy = parseColumnList(data.groupBy)
      const aggregations = parseAggregations(data.aggregations, err)
      if (!allowDraft && groupBy.length === 0 && aggregations.length === 0) {
        err('cleanBlockAggregateEmpty')
      }
      return { name, kind: 'aggregate', group_by: groupBy, aggregations }
    }
  }
}

function toNodeSpec(
  node: Node<ConnectorNodeData>,
  allowDraft = false,
  t: DagTranslator = defaultT,
): NodeSpec | undefined {
  let config: unknown
  try {
    config = node.data.config.trim() === '' ? {} : JSON.parse(node.data.config)
  } catch {
    if (allowDraft) {
      config = {}
    } else {
      throw new DagSerializationError(
        t('dag.errors.configNotValidJson', { name: node.data.name || node.data.connector }),
      )
    }
  }
  if (!node.data.connector.trim()) {
    if (allowDraft) return undefined
    throw new DagSerializationError(t('dag.errors.connectorNameEmpty'))
  }
  const spec: NodeSpec = { connector: node.data.connector, config }
  if (node.data.name.trim()) spec.name = node.data.name.trim()
  return spec
}

const COLUMN_X = { source: 0, transform: 320, python: 480, sink: 640 }
const CLEAN_BLOCK_SPACING = 160
const ROW_HEIGHT = 100

let importNodeId = 1

/**
 * PipelineSpec JSON -> canvas (nodes/edges). Positions aren't part of the
 * backend schema (PipelineSpec has no notion of a canvas), so this lays
 * sources/transform/sinks out in columns — purely a presentation default.
 */
export function fromPipelineSpec(spec: PipelineSpec): { nodes: DagNode[]; edges: Edge[] } {
  const nodes: DagNode[] = []
  const edges: Edge[] = []

  const sourceIds = spec.sources.map((source, i) => {
    const id = `import-${importNodeId++}`
    nodes.push({
      id,
      type: 'connector',
      position: { x: COLUMN_X.source, y: i * ROW_HEIGHT },
      data: {
        kind: 'connector',
        connector: source.connector,
        role: 'source',
        name: source.name ?? '',
        config: JSON.stringify(source.config ?? {}, null, 2),
      },
    })
    return id
  })

  const sinkIds = spec.sinks.map((sink, i) => {
    const id = `import-${importNodeId++}`
    nodes.push({
      id,
      type: 'connector',
      position: { x: COLUMN_X.sink, y: i * ROW_HEIGHT },
      data: {
        kind: 'connector',
        connector: sink.connector,
        role: 'sink',
        name: sink.name ?? '',
        config: JSON.stringify(sink.config ?? {}, null, 2),
      },
    })
    return id
  })

  // Chain of "stage node ids currently feeding the sinks" — starts as the
  // sources themselves, gets replaced by transform's id (if present), then
  // by python's id (if present), so the final wiring below always connects
  // whatever the last present stage is straight to every sink.
  let upstreamIds = sourceIds

  if (spec.transform) {
    const transformId = `import-${importNodeId++}`
    nodes.push({
      id: transformId,
      type: 'transform',
      position: { x: COLUMN_X.transform, y: ((sourceIds.length + sinkIds.length) / 2) * ROW_HEIGHT / 2 },
      data: { kind: 'transform', sql: spec.transform.sql },
    })
    upstreamIds.forEach((id) => {
      edges.push({ id: `${id}-${transformId}`, source: id, target: transformId })
    })
    upstreamIds = [transformId]
  }

  if (spec.python) {
    const pythonId = `import-${importNodeId++}`
    nodes.push({
      id: pythonId,
      type: 'python',
      position: { x: COLUMN_X.python, y: ((sourceIds.length + sinkIds.length) / 2) * ROW_HEIGHT / 2 },
      data: {
        kind: 'python',
        script: spec.python.script,
        timeoutSeconds: spec.python.timeout_seconds ?? 0,
      },
    })
    upstreamIds.forEach((id) => {
      edges.push({ id: `${id}-${pythonId}`, source: id, target: pythonId })
    })
    upstreamIds = [pythonId]
  }

  if (spec.clean_blocks && spec.clean_blocks.length > 0) {
    const y = ((sourceIds.length + sinkIds.length) / 2) * ROW_HEIGHT / 2
    spec.clean_blocks.forEach((block, i) => {
      const cleanId = `import-${importNodeId++}`
      nodes.push({
        id: cleanId,
        type: 'clean',
        position: { x: COLUMN_X.transform + i * CLEAN_BLOCK_SPACING, y },
        data: fromCleanBlockSpec(block),
      })
      upstreamIds.forEach((id) => {
        edges.push({ id: `${id}-${cleanId}`, source: id, target: cleanId })
      })
      upstreamIds = [cleanId]
    })
  }

  upstreamIds.forEach((id) => {
    sinkIds.forEach((sinkId) => {
      edges.push({ id: `${id}-${sinkId}`, source: id, target: sinkId })
    })
  })

  if (spec.embedding) {
    const embeddingId = `import-${importNodeId++}`
    nodes.push({
      id: embeddingId,
      type: 'embedding',
      position: { x: COLUMN_X.source + 160, y: -ROW_HEIGHT },
      data: fromEmbeddingSpec(spec.embedding),
    })
  }

  if (spec.dbt) {
    const dbtId = `import-${importNodeId++}`
    nodes.push({
      id: dbtId,
      type: 'dbt',
      position: { x: COLUMN_X.sink + 320, y: ((sinkIds.length - 1) * ROW_HEIGHT) / 2 },
      data: {
        kind: 'dbt',
        projectDir: spec.dbt.project_dir,
        command: spec.dbt.command,
        select: spec.dbt.select ?? '',
      },
    })
    sinkIds.forEach((sinkId) => {
      edges.push({ id: `${sinkId}-${dbtId}`, source: sinkId, target: dbtId })
    })
  }

  return { nodes, edges }
}

const DEFAULT_EMBEDDING_DATA: EmbeddingNodeData = {
  kind: 'embedding',
  sourceColumn: '',
  outputColumn: '',
  dimension: 384,
  backend: 'onnx',
  repo: '',
  revision: 'main',
  filename: '',
  tokenizerFilename: '',
  maxLength: 128,
  baseUrl: '',
  model: '',
  apiKeyEnv: '',
  strategy: 'fixed_window',
  chunkSize: 256,
  overlap: 0,
  similarityThreshold: 0.8,
  separators: '',
}

function fromEmbeddingSpec(spec: EmbeddingSpec): EmbeddingNodeData {
  const data: EmbeddingNodeData = {
    ...DEFAULT_EMBEDDING_DATA,
    sourceColumn: spec.source_column,
    outputColumn: spec.output_column,
    dimension: spec.dimension,
    backend: spec.model.backend,
    strategy: spec.chunking.strategy,
    chunkSize: 'chunk_size' in spec.chunking ? spec.chunking.chunk_size : 0,
    overlap: 'overlap' in spec.chunking ? (spec.chunking.overlap ?? 0) : 0,
  }
  if (spec.chunking.strategy === 'semantic') {
    data.similarityThreshold = spec.chunking.similarity_threshold
  }
  if (spec.model.backend === 'onnx') {
    data.repo = spec.model.repo
    data.revision = spec.model.revision
    data.filename = spec.model.filename
    data.tokenizerFilename = spec.model.tokenizer_filename
    data.maxLength = spec.model.max_length
  } else {
    data.baseUrl = spec.model.base_url
    data.model = spec.model.model
    data.apiKeyEnv = spec.model.api_key_env ?? ''
  }
  if (spec.chunking.strategy === 'recursive_character') {
    data.separators = (spec.chunking.separators ?? []).join('\n')
  }
  return data
}

/** Every block kind's default canvas data — dropping a fresh block of a
 * given kind onto the canvas starts from this, `blockKind` overridden. */
export const DEFAULT_CLEAN_DATA: CleanBlockNodeData = {
  kind: 'clean',
  blockKind: 'filter',
  name: '',
  column: '',
  operator: 'eq',
  value: '',
  selectMode: 'keep',
  columns: '',
  from: '',
  to: '',
  dataType: 'text',
  find: '',
  replace: '',
  nullStrategy: 'value',
  fallbackColumn: '',
  caseMode: 'upper',
  output: '',
  left: '',
  computeOperator: 'add',
  right: '',
  direction: 'asc',
  groupBy: '',
  aggregations: '',
}

function fromCleanBlockSpec(spec: CleanBlockSpec): CleanBlockNodeData {
  const data: CleanBlockNodeData = { ...DEFAULT_CLEAN_DATA, blockKind: spec.kind, name: spec.name ?? '' }
  switch (spec.kind) {
    case 'filter':
      data.column = spec.column
      data.operator = spec.operator
      data.value = spec.value ?? ''
      break
    case 'select_columns':
      data.selectMode = spec.mode
      data.columns = spec.columns.join(', ')
      break
    case 'rename':
      data.from = spec.from
      data.to = spec.to
      break
    case 'cast':
      data.column = spec.column
      data.dataType = spec.data_type
      break
    case 'trim':
      data.columns = spec.columns.join(', ')
      break
    case 'replace_text':
      data.column = spec.column
      data.find = spec.find
      data.replace = spec.replace
      break
    case 'fill_nulls':
      data.column = spec.column
      data.nullStrategy = spec.strategy
      if (spec.strategy === 'value') data.value = spec.value
      if (spec.strategy === 'other_column') data.fallbackColumn = spec.fallback_column
      break
    case 'drop_nulls':
      data.columns = spec.columns.join(', ')
      break
    case 'dedupe':
      data.columns = spec.columns.join(', ')
      break
    case 'change_case':
      data.column = spec.column
      data.caseMode = spec.mode
      break
    case 'computed_column':
      data.output = spec.output
      data.left = spec.left
      data.computeOperator = spec.operator
      data.right = spec.right
      break
    case 'sort':
      data.column = spec.column
      data.direction = spec.direction
      break
    case 'aggregate':
      data.groupBy = spec.group_by.join(', ')
      data.aggregations = spec.aggregations.map((a) => `${a.column}:${a.function}:${a.output}`).join('\n')
      break
  }
  return data
}
