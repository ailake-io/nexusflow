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

/** Matches nexus-core::PythonTransformSpec exactly — a cleaning/
 * transformation stage run as an isolated `python3` subprocess (mirrors
 * `dbt` in isolation model, not in when it runs — chains after `transform`
 * like a DAG stage, not as a post-load step). */
export interface PythonTransformSpec {
  script: string
  timeout_seconds?: number
}

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
  /** Per-pipeline alert channels, additive to the global env-var-configured
   * ones. Unset means no per-pipeline channels. */
  alerts?: AlertsConfig
  /** When true, the spec is saved as a draft and the server skips validation
   * of connector configs/embedding/dbt. Drafts cannot be executed. */
  draft?: boolean
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

export type DagNodeData =
  | ConnectorNodeData
  | TransformNodeData
  | DbtNodeData
  | EmbeddingNodeData
  | PythonNodeData
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

export class DagSerializationError extends Error {}

/** Minimal translation function injected from the React i18n layer. */
export type DagTranslator = (key: string, vars?: Record<string, string | number>) => string

export interface PipelineMeta {
  pipelineId: string
  channelCapacity?: number
  partitions?: number
  schedule?: string
  alerts?: AlertsConfig
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
  }

  const transform =
    transformNodes.length === 1 ? { sql: transformNodes[0].data.sql } : undefined

  const python: PythonTransformSpec | undefined =
    pythonNodes.length === 1 ? { script: pythonNodes[0].data.script } : undefined
  if (python && pythonNodes[0].data.timeoutSeconds > 0) {
    python.timeout_seconds = pythonNodes[0].data.timeoutSeconds
  }

  if (!allowDraft) {
    if (!transform && !python && (sources.length !== 1 || sinks.length !== 1)) {
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

  const spec: PipelineSpec = {
    pipeline_id: meta.pipelineId,
    sources,
    sinks,
  }
  if (transform) spec.transform = transform
  if (embedding) spec.embedding = embedding
  if (python) spec.python = python
  if (dbt) spec.dbt = dbt
  if (meta.channelCapacity !== undefined) spec.channel_capacity = meta.channelCapacity
  if (meta.partitions !== undefined) spec.partitions = meta.partitions
  if (meta.schedule?.trim()) spec.schedule = meta.schedule.trim()
  if (meta.alerts) spec.alerts = meta.alerts
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
