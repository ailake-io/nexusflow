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
  /** Present (and `properties` absent) for a free-form `HashMap<String, V>`
   *  Rust field — schemars emits this instead of a fixed `properties` set. */
  additionalProperties?: JsonSchemaNode | boolean
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
  /** `true` for every OSS connector; for an enterprise connector (see
   * `requires_license` below), `true` only if the installed license covers
   * it. ConnectorPalette shows a lock icon when this is `false`. */
  licensed: boolean
  /** Present only for an enterprise-gated connector (its own slug) —
   * absent for OSS. The Store page uses this to tell "always free" apart
   * from "enterprise, and here's whether you own it" (both read
   * `licensed: true` for OSS). */
  requires_license?: string
}

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

let unauthorizedHandler: (() => void) | null = null

/**
 * Registers a callback invoked when an API call receives 401 Unauthorized.
 * The auth layer uses this to clear the stored token and return to the login
 * screen. Ignored for the login endpoint itself (wrong credentials must not
 * log the user out).
 */
export function onUnauthorized(handler: () => void) {
  unauthorizedHandler = handler
}

async function request<T>(path: string, init: RequestInit = {}, token?: string): Promise<T> {
  const headers = new Headers(init.headers)
  if (token) headers.set('authorization', `Bearer ${token}`)
  if (init.body) headers.set('content-type', 'application/json')

  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    if (response.status === 401 && !path.endsWith('/auth/login') && unauthorizedHandler) {
      unauthorizedHandler()
    }
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

/** Matches nexus-server::LicenseStatusResponse, as returned by both
 * GET /license and POST /license (Admin-only, see `docs/ENTERPRISE_LICENSING.md`). */
export interface LicenseStatus {
  active: boolean
  connectors: string[]
  seats: number
  expires_at: number | null
}

export function getLicenseStatus(token: string): Promise<LicenseStatus> {
  return request<LicenseStatus>('/license', {}, token)
}

export function installLicense(token: string, licenseKey: string): Promise<LicenseStatus> {
  return request<LicenseStatus>(
    '/license',
    { method: 'POST', body: JSON.stringify({ license_key: licenseKey }) },
    token,
  )
}

/** Matches nexus-core::ProgressEvent, as sent over the progress WebSocket. */
export interface ProgressEvent {
  partition_id: string
  batches_written: number
  rows_written: number
  bytes_written: number
}

/** Matches nexus-server::hardware_stats::HardwareStats. Sent every ~2s on
 * the same WebSocket as ProgressEvent, wrapped as `{ hardware_stats: ... }`
 * so the client can tell the two message shapes apart (CLAUDE.md §6). */
export interface HardwareStats {
  cpu_percent: number
  memory_used_bytes: number
  memory_total_bytes: number
}

/** Matches nexus-server::progress::RunLogEvent. Persisted server-side (`GET
 * .../logs`, see `listRunLogs`) as well as broadcast live over the same
 * WebSocket as ProgressEvent — the `type: 'log'` tag is only added on the
 * wire for this variant, distinguishing it from the untagged ProgressEvent/
 * hardware_stats shapes below (nexus-server/src/progress.rs's doc comment
 * explains why those two stayed untagged). */
export interface RunLogEvent {
  type: 'log'
  ts: string
  level: 'info' | 'warn' | 'error'
  message: string
}

/** A frame on the progress WebSocket is a bare ProgressEvent, a `{
 * hardware_stats }` wrapper, or a tagged `RunLogEvent` — discriminated by
 * `type === 'log'` first, then the presence of the `hardware_stats` key. */
export type ProgressSocketMessage =
  | ProgressEvent
  | { hardware_stats: HardwareStats }
  | RunLogEvent

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
  spec: PipelineSpec,
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

/** Matches nexus-server::lineage::ResourceKind. */
export type LineageResourceKind = 'table' | 'collection' | 'topic' | 'file'

/** Matches nexus-server::lineage::LineageNode — a saved pipeline, or a
 *  resource one or more pipelines touch (identified by a connector-specific
 *  allowlisted field only, e.g. `table`/`collection`/`topic`; never a raw
 *  connection string). Discriminated by `kind`. */
export type LineageNode =
  | { kind: 'pipeline'; id: string; label: string; has_schedule: boolean }
  | { kind: 'resource'; id: string; label: string; connector: string; resource_kind: LineageResourceKind }

/** Matches nexus-server::lineage::LineageEdge. */
export interface LineageEdge {
  from: string
  to: string
}

/** Matches nexus-server::lineage::LineageGraph, as returned by GET /lineage.
 *  Whole-catalog graph, computed fresh on every request — no polling
 *  needed, saved pipelines change rarely compared to live resource usage. */
export interface LineageGraph {
  nodes: LineageNode[]
  edges: LineageEdge[]
}

export function getLineage(token: string): Promise<LineageGraph> {
  return request<LineageGraph>('/lineage', {}, token)
}

/**
 * Replays a run's execution log after the fact — works whether the run is
 * still going, already finished, or (the reason this exists) was triggered
 * by the scheduler and nobody had the live WebSocket open for it. Same
 * `RunLogEvent` shape as the live `type: 'log'` WebSocket frames, just
 * without the `type` tag (the endpoint returns a plain array, no
 * discrimination needed).
 */
export function listRunLogs(
  token: string,
  pipelineId: string,
  runId: number,
): Promise<Omit<RunLogEvent, 'type'>[]> {
  return request<Omit<RunLogEvent, 'type'>[]>(
    `/pipelines/${encodeURIComponent(pipelineId)}/runs/${runId}/logs`,
    {},
    token,
  )
}

/**
 * The progress WebSocket URL carries no credentials. The JWT is sent via the
 * `Sec-WebSocket-Protocol` subprotocol, which keeps it out of URLs, server
 * logs, and browser history. The browser WebSocket API cannot set a custom
 * `Authorization` header, but it can request a subprotocol; see
 * `progress_ws_handler` on the server.
 */
export function progressSocketUrl(pipelineId: string, runId: number): string {
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
  return `${protocol}://${window.location.host}/pipelines/${encodeURIComponent(pipelineId)}/runs/${runId}/progress`
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

/** Matches nexus-server's Role enum (`#[serde(rename_all = "lowercase")]`) —
 * `Read < Execute < Write < Admin`, ARCHITECTURE.md §10. */
export type Role = 'read' | 'execute' | 'write' | 'admin'

/** Matches nexus-server::UserResponse, as returned by the /users routes
 * (all Admin-only). */
export interface UserRecord {
  username: string
  role: Role
}

export function listUsers(token: string): Promise<UserRecord[]> {
  return request<UserRecord[]>('/users', {}, token)
}

export function createUser(
  token: string,
  username: string,
  password: string,
  role: Role,
): Promise<UserRecord> {
  return request<UserRecord>(
    '/users',
    { method: 'POST', body: JSON.stringify({ username, password, role }) },
    token,
  )
}

export function updateUserRole(token: string, username: string, role: Role): Promise<UserRecord> {
  return request<UserRecord>(
    `/users/${encodeURIComponent(username)}/role`,
    { method: 'PUT', body: JSON.stringify({ role }) },
    token,
  )
}

export function deleteUser(token: string, username: string): Promise<void> {
  return request<void>(`/users/${encodeURIComponent(username)}`, { method: 'DELETE' }, token)
}

/** Decodes the `role` claim from a JWT's payload without verifying the
 * signature — the server is the actual enforcement point on every request
 * this is only used to decide whether to show the Admin nav item at all.
 * Returns `null` on any malformed/unexpected token instead of throwing, so
 * a UI-only decode issue never blocks login. */
export function decodeRoleFromToken(token: string): Role | null {
  try {
    const payload = token.split('.')[1]
    const json = atob(payload.replace(/-/g, '+').replace(/_/g, '/'))
    const claims = JSON.parse(json) as { role?: unknown }
    const role = claims.role
    return role === 'read' || role === 'execute' || role === 'write' || role === 'admin'
      ? role
      : null
  } catch {
    return null
  }
}
