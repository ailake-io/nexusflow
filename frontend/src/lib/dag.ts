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
  channel_capacity?: number
  partitions?: number
  dbt?: DbtConfig
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

export type DagNodeData = ConnectorNodeData | TransformNodeData | DbtNodeData
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

export class DagSerializationError extends Error {}

export interface PipelineMeta {
  pipelineId: string
  channelCapacity?: number
  partitions?: number
}

/**
 * Canvas (nodes/edges) -> PipelineSpec JSON. Mirrors the validation in
 * `PipelineSpec::validate` (dag.rs) so obviously-invalid graphs are rejected
 * client-side with the same rules, instead of round-tripping to the server
 * to find out.
 */
export function toPipelineSpec(nodes: DagNode[], meta: PipelineMeta): PipelineSpec {
  if (!meta.pipelineId.trim()) {
    throw new DagSerializationError('pipeline_id must not be empty')
  }

  const connectorNodes = nodes.filter(isConnectorNode)
  const transformNodes = nodes.filter(isTransformNode)
  const dbtNodes = nodes.filter(isDbtNode)
  if (transformNodes.length > 1) {
    throw new DagSerializationError('at most one transform node is allowed')
  }
  if (dbtNodes.length > 1) {
    throw new DagSerializationError('at most one dbt node is allowed')
  }

  const sources = connectorNodes
    .filter((n) => n.data.role === 'source')
    .map((n) => toNodeSpec(n))
  const sinks = connectorNodes
    .filter((n) => n.data.role === 'sink')
    .map((n) => toNodeSpec(n))

  if (sources.length === 0) {
    throw new DagSerializationError('sources must not be empty')
  }
  if (sinks.length === 0) {
    throw new DagSerializationError('sinks must not be empty')
  }

  const transform =
    transformNodes.length === 1 ? { sql: transformNodes[0].data.sql } : undefined

  if (!transform && (sources.length !== 1 || sinks.length !== 1)) {
    throw new DagSerializationError(
      'without a transform, the pipeline must be strictly linear: exactly 1 source and 1 sink',
    )
  }
  if (transform && !transform.sql.trim()) {
    throw new DagSerializationError('transform.sql must not be empty')
  }

  let dbt: DbtConfig | undefined
  if (dbtNodes.length === 1) {
    const data = dbtNodes[0].data
    if (!data.projectDir.trim()) {
      throw new DagSerializationError('dbt node: project_dir must not be empty')
    }
    dbt = { project_dir: data.projectDir.trim(), command: data.command }
    if (data.select.trim()) dbt.select = data.select.trim()
  }

  const spec: PipelineSpec = {
    pipeline_id: meta.pipelineId,
    sources,
    sinks,
  }
  if (transform) spec.transform = transform
  if (dbt) spec.dbt = dbt
  if (meta.channelCapacity !== undefined) spec.channel_capacity = meta.channelCapacity
  if (meta.partitions !== undefined) spec.partitions = meta.partitions
  return spec
}

function toNodeSpec(node: Node<ConnectorNodeData>): NodeSpec {
  let config: unknown
  try {
    config = node.data.config.trim() === '' ? {} : JSON.parse(node.data.config)
  } catch {
    throw new DagSerializationError(
      `node "${node.data.name || node.data.connector}": config is not valid JSON`,
    )
  }
  if (!node.data.connector.trim()) {
    throw new DagSerializationError('every connector node needs a connector name')
  }
  const spec: NodeSpec = { connector: node.data.connector, config }
  if (node.data.name.trim()) spec.name = node.data.name.trim()
  return spec
}

const COLUMN_X = { source: 0, transform: 320, sink: 640 }
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

  if (spec.transform) {
    const transformId = `import-${importNodeId++}`
    nodes.push({
      id: transformId,
      type: 'transform',
      position: { x: COLUMN_X.transform, y: ((sourceIds.length + sinkIds.length) / 2) * ROW_HEIGHT / 2 },
      data: { kind: 'transform', sql: spec.transform.sql },
    })
    sourceIds.forEach((sourceId) => {
      edges.push({ id: `${sourceId}-${transformId}`, source: sourceId, target: transformId })
    })
    sinkIds.forEach((sinkId) => {
      edges.push({ id: `${transformId}-${sinkId}`, source: transformId, target: sinkId })
    })
  } else {
    edges.push({ id: `${sourceIds[0]}-${sinkIds[0]}`, source: sourceIds[0], target: sinkIds[0] })
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
