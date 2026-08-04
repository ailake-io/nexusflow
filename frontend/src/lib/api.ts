import type { PipelineSpec } from '@/lib/dag'

/** Matches nexus-core::ConnectorCapability (ARCHITECTURE.md §3). */
export type ConnectorCapability = 'adbc_native' | 'arrow_flight' | 'bridged'

/**
 * A JSON Schema node as `schemars` emits it for a connector's Config struct
 * — only the subset of the spec SchemaForm.tsx actually renders. `$ref`
 * points into the root schema's own `$defs` (schemars never nests `$defs`
 * inside a sub-schema, only at the document root).
 */
export interface JsonSchemaNode {
  type?: string
  properties?: Record<string, JsonSchemaNode>
  required?: string[]
  enum?: string[]
  items?: JsonSchemaNode
  $ref?: string
  description?: string
  default?: unknown
}

export interface ConnectorConfigSchema extends JsonSchemaNode {
  $defs?: Record<string, JsonSchemaNode>
}

/** Matches nexus-server::ConnectorCatalogEntry, as returned by GET /connectors. */
export interface ConnectorDescriptor {
  name: string
  capability: ConnectorCapability
  config_schema: ConnectorConfigSchema
}

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(path: string, init: RequestInit = {}, token?: string): Promise<T> {
  const headers = new Headers(init.headers)
  if (token) headers.set('authorization', `Bearer ${token}`)
  if (init.body) headers.set('content-type', 'application/json')

  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new ApiError(response.status, body?.error ?? response.statusText)
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

export async function login(username: string, password: string): Promise<string> {
  const { token } = await request<{ token: string }>('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  })
  return token
}

export function listConnectors(token: string): Promise<ConnectorDescriptor[]> {
  return request<ConnectorDescriptor[]>('/connectors', {}, token)
}

/** Matches nexus-core::ProgressEvent, as sent over the progress WebSocket. */
export interface ProgressEvent {
  partition_id: string
  batches_written: number
  rows_written: number
  bytes_written: number
}

/** Matches nexus-server::dbt::DbtOutcome::summary_json's shape (Marco 10
 * task #26) — `undefined` when the pipeline has no `dbt` step, or the
 * server build lacks the "dbt" feature. */
export interface DbtRunSummary {
  command: string
  models_total: number
  models_succeeded: number
  models_failed: number
  tests_total: number
  tests_passed: number
  tests_failed: number
  elapsed_time: number
  nodes_in_lineage: number | null
}

/** Matches nexus-server::pipeline_store::RunRecord, as returned by GET /pipelines/{id}/runs. */
export interface RunRecord {
  id: number
  pipeline_id: string
  started_at: string
  finished_at: string | null
  status: 'running' | 'success' | 'failed'
  error: string | null
  stats: unknown
  dbt_summary: DbtRunSummary | null
}

/**
 * POST /pipelines/{id}/run returns **202 Accepted** as soon as the run row
 * and its progress channel exist on the server — the pipeline itself
 * executes in a background task, so the caller gets the new run's id
 * immediately and can subscribe to its progress WebSocket right away (see
 * hooks/useRunProgress.ts). The terminal state (success/failed, stats,
 * error) is read back via `listRuns`.
 */
export function runPipeline(
  token: string,
  spec: { pipeline_id: string },
): Promise<{ run_id: number }> {
  return request<{ run_id: number }>(
    `/pipelines/${encodeURIComponent(spec.pipeline_id)}/run`,
    { method: 'POST', body: JSON.stringify(spec) },
    token,
  )
}

export function listRuns(token: string, pipelineId: string): Promise<RunRecord[]> {
  return request<RunRecord[]>(`/pipelines/${encodeURIComponent(pipelineId)}/runs`, {}, token)
}

/**
 * Browsers' WebSocket API can't set an Authorization header, so the token
 * travels as a query param — the server verifies it directly for this one
 * route instead of through the usual Bearer-header extractor.
 */
export function progressSocketUrl(pipelineId: string, runId: number, token: string): string {
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${protocol}://${window.location.host}/pipelines/${encodeURIComponent(pipelineId)}/runs/${runId}/progress?token=${encodeURIComponent(token)}`
}

/** Matches nexus-server::pipeline_store::NodeSummary — connector name only,
 * never the config blob a node carries (that's where secrets live). */
export interface NodeSummary {
  connector: string
  name: string | null
}

/**
 * Matches nexus-server::pipeline_store::PipelineSummary, as returned by
 * GET /pipelines and GET /pipelines/{id} — the API itself never hands back
 * a persisted connector's config (CLAUDE.md §5 / task #17: "nunca renderiza
 * segredo em plain text"), so there's nothing for the frontend to mask —
 * it's already masked before it gets here.
 */
export interface PipelineSummary {
  pipeline_id: string
  sources: NodeSummary[]
  sinks: NodeSummary[]
  has_transform: boolean
  created_at: string
  updated_at: string
  /** Cron expression, if this pipeline has an automatic schedule — `null`
   * means it only runs when explicitly triggered. */
  schedule: string | null
  /** Status of the most recent run ("running" / "success" / "failed"),
   * `null` if it has never run. */
  last_run_status: 'running' | 'success' | 'failed' | null
  last_run_at: string | null
}

export function listPipelines(token: string): Promise<PipelineSummary[]> {
  return request<PipelineSummary[]>('/pipelines', {}, token)
}

/** POST /pipelines — fails with a 409 ApiError if pipeline_id already exists
 * (use updatePipeline instead in that case). */
export function createPipeline(
  token: string,
  spec: { pipeline_id: string },
): Promise<PipelineSummary> {
  return request<PipelineSummary>(
    '/pipelines',
    { method: 'POST', body: JSON.stringify(spec) },
    token,
  )
}

/** PUT /pipelines/{id} — fails with a 404 ApiError if it doesn't exist yet
 * (use createPipeline instead in that case). */
export function updatePipeline(
  token: string,
  spec: { pipeline_id: string },
): Promise<PipelineSummary> {
  return request<PipelineSummary>(
    `/pipelines/${encodeURIComponent(spec.pipeline_id)}`,
    { method: 'PUT', body: JSON.stringify(spec) },
    token,
  )
}

/** GET /pipelines/{id}/spec — full spec, connector configs (secrets) included.
 * Requires Write role; used only to reload a saved pipeline onto the canvas
 * for editing. Never render this response's node configs anywhere but the
 * canvas inspector. */
export function getPipelineSpec(token: string, pipelineId: string): Promise<PipelineSpec> {
  return request<PipelineSpec>(`/pipelines/${encodeURIComponent(pipelineId)}/spec`, {}, token)
}

export function deletePipeline(token: string, pipelineId: string): Promise<void> {
  return request<void>(
    `/pipelines/${encodeURIComponent(pipelineId)}`,
    { method: 'DELETE' },
    token,
  )
}
