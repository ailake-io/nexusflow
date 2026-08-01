/** Matches nexus-core::ConnectorCapability (ARCHITECTURE.md §3). */
export type ConnectorCapability = 'adbc_native' | 'arrow_flight' | 'bridged'

/** Matches nexus-core::ConnectorDescriptor, as returned by GET /connectors. */
export interface ConnectorDescriptor {
  name: string
  capability: ConnectorCapability
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

/** Matches nexus-server::pipeline_store::RunRecord, as returned by GET /pipelines/{id}/runs. */
export interface RunRecord {
  id: number
  pipeline_id: string
  started_at: string
  finished_at: string | null
  status: 'running' | 'success' | 'failed'
  error: string | null
  stats: unknown
}

/**
 * POST /pipelines/{id}/run doesn't resolve until the whole pipeline
 * finishes — callers that want live progress shouldn't await this before
 * polling `listRuns` for the new run's id (see hooks/useRunProgress.ts).
 */
export function runPipeline(token: string, spec: { pipeline_id: string }): Promise<unknown> {
  return request(
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
}

export function listPipelines(token: string): Promise<PipelineSummary[]> {
  return request<PipelineSummary[]>('/pipelines', {}, token)
}

export function deletePipeline(token: string, pipelineId: string): Promise<void> {
  return request<void>(
    `/pipelines/${encodeURIComponent(pipelineId)}`,
    { method: 'DELETE' },
    token,
  )
}
