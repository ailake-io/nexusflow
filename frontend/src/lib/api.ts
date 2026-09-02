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

/** Matches nexus-server::preview_adhoc_handler's response
 * (POST /connectors/preview) — same shape GET /pipelines/{id}/preview
 * returns, just for a bare connector/config pair instead of a saved
 * pipeline's node. */
export interface PreviewResult {
  rows: Record<string, unknown>[]
}

export function previewConnector(
  token: string,
  connector: string,
  config: Record<string, unknown>,
  limit = 20,
): Promise<PreviewResult> {
  return request<PreviewResult>(
    '/connectors/preview',
    { method: 'POST', body: JSON.stringify({ connector, config, limit }) },
    token,
  )
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

/**
 * `nexus-licensing` — a separate, centrally-run service (never this
 * `nexus-server` instance itself, see `docs/ENTERPRISE_LICENSING.md`), so
 * these calls go to their own origin instead of through `request()` above.
 * Configured via `VITE_LICENSING_API_URL`; the Store page hides the buy
 * flow entirely when it's unset — the manual "cole a license key" flow
 * (`installLicense` above) always works regardless.
 */
const LICENSING_API_URL = (import.meta.env.VITE_LICENSING_API_URL as string | undefined)?.replace(
  /\/$/,
  '',
)

export function isLicensingConfigured(): boolean {
  return Boolean(LICENSING_API_URL)
}

async function licensingRequest<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.body) headers.set('content-type', 'application/json')
  const response = await fetch(`${LICENSING_API_URL}${path}`, { ...init, headers })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new ApiError(response.status, body?.error ?? response.statusText)
  }
  return response.json() as Promise<T>
}

/** Matches nexus-licensing::Product (its own catalog, separate from this
 * server's `/connectors` — see `docs/ENTERPRISE_LICENSING.md §3`). */
export interface LicensingProduct {
  id: number
  connector_slug: string
  name: string
  price_cents_brl: number
  price_cents_usd: number
  active: boolean
}

export function listLicensingProducts(): Promise<LicensingProduct[]> {
  return licensingRequest<LicensingProduct[]>('/products')
}

export interface CheckoutResponse {
  checkout_url: string
}

export function createCheckout(
  productIds: number[],
  email: string,
  currency: 'brl' | 'usd',
): Promise<CheckoutResponse> {
  return licensingRequest<CheckoutResponse>('/checkout', {
    method: 'POST',
    body: JSON.stringify({ product_ids: productIds, email, currency }),
  })
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

/** Matches nexus-core::pipeline::PartitionStats — one entry per partition/
 *  sink written during a run, embedded in RunRecord.stats below. */
export interface PartitionStats {
  partition_id: string
  batches_written: number
  rows_written: number
  resume_state: string | null
}

/** Matches nexus-server::pipeline_store::RunRecord, as returned by GET /pipelines/{id}/runs. */
export interface RunRecord {
  id: number
  pipeline_id: string
  started_at: string
  finished_at: string | null
  status: 'running' | 'success' | 'failed'
  error: string | null
  stats: PartitionStats[] | null
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

/** Matches nexus-server::resource_stats::ResourceStatsBucket — one averaged
 *  point returned by GET /system/resource-stats. `disk_*` are `null` when
 *  the backend couldn't resolve the data directory's containing mount. */
export interface ResourceStatsBucket {
  bucket_start: string
  cpu_percent: number
  memory_used_bytes: number
  memory_total_bytes: number
  disk_used_bytes: number | null
  disk_total_bytes: number | null
}

/**
 * Historical CPU/memory/disk usage for the Resources tab. `range` is
 * `<number><unit>` (`5m`/`45m`/`3h`/`12d`, unit ∈ m/h/d) — the same 5
 * preset shortcuts the panel offers (`1h`/`6h`/`1d`/`7d`/`30d`) plus
 * whatever custom value the user types. Defaults to `5m` server-side when
 * omitted, matching the panel's own initial state.
 */
export function getResourceStats(token: string, range: string): Promise<ResourceStatsBucket[]> {
  return request<ResourceStatsBucket[]>(
    `/system/resource-stats?range=${encodeURIComponent(range)}`,
    {},
    token,
  )
}

/** Matches nexus-server::dbt_test_result_store::DbtTestOutcome, as returned
 *  by GET /pipelines/{id}/dbt-tests. One row per recorded test result —
 *  history, not just the latest run (dbt's own CLI has no cross-run memory
 *  of this; nexus-server persists it instead of discarding it after the
 *  aggregate pass/fail count already on RunRecord.dbt_summary is derived). */
export interface DbtTestOutcome {
  unique_id: string
  status: 'pass' | 'fail' | 'warn'
  message: string | null
  execution_time: number
}

export function getDbtTestResults(token: string, pipelineId: string): Promise<DbtTestOutcome[]> {
  return request<DbtTestOutcome[]>(
    `/pipelines/${encodeURIComponent(pipelineId)}/dbt-tests`,
    {},
    token,
  )
}

/** Matches nexus-server::quality_check_store's `QualityCheckOutcome` (via
 *  `nexus_core::QualityCheckOutcome`), as returned by
 *  GET /pipelines/{id}/quality-checks. Native, dbt-independent checks
 *  (not_null/unique/min/max/accepted_values) — always registered, never
 *  blocking a run (see `PipelineSpec.quality_checks`'s doc comment). */
export interface QualityCheckOutcome {
  column: string
  check: string
  status: 'pass' | 'fail'
  message: string | null
}

export function getQualityCheckResults(
  token: string,
  pipelineId: string,
): Promise<QualityCheckOutcome[]> {
  return request<QualityCheckOutcome[]>(
    `/pipelines/${encodeURIComponent(pipelineId)}/quality-checks`,
    {},
    token,
  )
}

/** Matches nexus-server::lineage::ResourceKind. */
export type LineageResourceKind = 'table' | 'collection' | 'topic' | 'file'

/** Matches nexus-server::lineage::LineageNode — a saved pipeline, a
 *  resource one or more pipelines touch (identified by a connector-specific
 *  allowlisted field only, e.g. `table`/`collection`/`topic`; never a raw
 *  connection string), or a node from a pipeline's dbt project
 *  (`resource_type` is dbt's own vocabulary — `model`/`source`/`seed`/
 *  `snapshot`). Discriminated by `kind`. */
export type LineageNode =
  | { kind: 'pipeline'; id: string; label: string; has_schedule: boolean }
  | { kind: 'resource'; id: string; label: string; connector: string; resource_kind: LineageResourceKind }
  | { kind: 'dbt_node'; id: string; label: string; resource_type: string }

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

/** Matches nexus-server::pipeline_schema_store::ColumnInfo. */
export interface LineageColumnInfo {
  name: string
  data_type: string
}

/** Matches nexus-server::pipeline_schema_store::ColumnLineageInfo —
 *  `source_columns: null` means the backend's `LogicalPlan` walk couldn't
 *  determine provenance for this output column (an unsupported query shape,
 *  e.g. a `UNION`), not that it has none. */
export interface LineageColumnLineageInfo {
  output_column: string
  source_columns: string[] | null
}

/** Matches nexus-server::pipeline_schema_store::PipelineSchema, returned by
 *  `GET /lineage/{id}/schema`. `column_lineage` is only present when the
 *  pipeline has a SQL transform stage. */
export interface PipelineSchema {
  pipeline_id: string
  source_columns: LineageColumnInfo[]
  output_columns: LineageColumnInfo[]
  column_lineage: LineageColumnLineageInfo[] | null
  captured_at: string
  /** Human-readable diff against the *previous* capture (added/removed/
   *  retyped columns) — `null` on the first-ever capture or when the last
   *  run's schema matched the one before it. */
  last_drift: string | null
}

/** Fetched on demand — clicking a pipeline node in the Lineage tab, not
 *  bundled into `GET /lineage`. Throws `ApiError` with `status: 404` when
 *  the pipeline has never run (nothing captured yet). */
export function getPipelineSchema(token: string, pipelineId: string): Promise<PipelineSchema> {
  return request<PipelineSchema>(`/lineage/${encodeURIComponent(pipelineId)}/schema`, {}, token)
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

/** A resolved source/sink node's own name — `sink0`/`source0` for an
 *  unnamed node, or its explicit `name` — same string
 *  `NodeSpec::resolved_name` produces server-side and what `GET
 *  /pipelines/{id}/preview`'s `node` query param expects. */
export function resolvedNodeName(node: NodeSummary, index: number, prefix: 'source' | 'sink'): string {
  return node.name ?? `${prefix}${index}`
}

/** GET /pipelines/{id}/preview — first `limit` rows (default 50, capped at
 *  500 server-side) of one saved source/sink node, read live via the same
 *  `build_source` path a real run uses. Only works for a connector that can
 *  act as a `Source` — a sink-only connector (milvus/qdrant/lancedb/
 *  pgvector/pinecone/chromadb/webhook) rejects with a 400 `ApiError`. Each
 *  row is a plain JSON object keyed by column name; there is no separate
 *  schema — infer types from the values themselves. */
export function previewNode(
  token: string,
  pipelineId: string,
  node: string,
  limit?: number,
): Promise<{ rows: Record<string, unknown>[] }> {
  const params = new URLSearchParams({ node })
  if (limit) params.set('limit', String(limit))
  return request<{ rows: Record<string, unknown>[] }>(
    `/pipelines/${encodeURIComponent(pipelineId)}/preview?${params.toString()}`,
    {},
    token,
  )
}

/** One entry (file or subdirectory) returned by `browseFilesystem`. */
export interface BrowseEntry {
  name: string
  is_dir: boolean
  size: number | null
}

/** Matches nexus-server::browse::BrowseListing, as returned by
 *  GET /system/browse-fs. */
export interface BrowseListing {
  path: string
  entries: BrowseEntry[]
}

/** Lists a server-side directory's contents for the Canvas "browse path"
 *  file picker (`FileBrowserDialog`) — backs any file-based connector's
 *  `path`/`file_path` config field. `path` omitted/empty lists `/`. */
export function browseFilesystem(token: string, path?: string): Promise<BrowseListing> {
  const params = new URLSearchParams()
  if (path) params.set('path', path)
  const query = params.toString()
  return request<BrowseListing>(`/system/browse-fs${query ? `?${query}` : ''}`, {}, token)
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
