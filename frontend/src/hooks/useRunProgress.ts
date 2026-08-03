import { useCallback, useRef, useState } from 'react'
import {
  listRuns,
  progressSocketUrl,
  runPipeline,
  type DbtRunSummary,
  type ProgressEvent,
  type RunRecord,
} from '@/lib/api'
import type { PipelineSpec } from '@/lib/dag'

export type ExecutionStatus = 'idle' | 'starting' | 'running' | 'success' | 'failed'

export interface PartitionProgress extends ProgressEvent {
  rowsPerSecond: number
  mbPerSecond: number
}

interface UseRunProgressResult {
  status: ExecutionStatus
  runId: number | null
  partitions: Record<string, PartitionProgress>
  error: string | null
  dbtSummary: DbtRunSummary | null
  run: (token: string, spec: PipelineSpec) => void
}

const SETTLE_POLL_INTERVAL_MS = 250
const SETTLE_POLL_MAX_ATTEMPTS = 20

/**
 * Drives the "run + watch live progress" flow (Marco 8 task #16). The run
 * endpoint (POST /pipelines/{id}/run) returns 202 Accepted with the new
 * run's id as soon as the run row and its progress channel exist, so the
 * WebSocket can connect immediately — no polling for the run id. The
 * pipeline executes in a background task on the server; its terminal state
 * is read back from GET /pipelines/{id}/runs once the socket closes
 * (ARCHITECTURE.md §8).
 */
export function useRunProgress(): UseRunProgressResult {
  const [status, setStatus] = useState<ExecutionStatus>('idle')
  const [runId, setRunId] = useState<number | null>(null)
  const [partitions, setPartitions] = useState<Record<string, PartitionProgress>>({})
  const [error, setError] = useState<string | null>(null)
  const [dbtSummary, setDbtSummary] = useState<DbtRunSummary | null>(null)
  const lastSample = useRef<Record<string, { event: ProgressEvent; at: number }>>({})
  const wsRef = useRef<WebSocket | null>(null)

  const applyFinalRecord = useCallback((record: RunRecord) => {
    setStatus(record.status === 'success' ? 'success' : 'failed')
    setError(record.error)
    setDbtSummary(record.dbt_summary)
  }, [])

  const connectSocket = useCallback(
    (token: string, pipelineId: string, id: number) => {
      // Re-fetch the authoritative record once the socket closes instead of
      // trusting the last progress event — a failure can happen between two
      // events. The supervisor closes the progress channel *before* it
      // writes the terminal state, so poll briefly until the record settles.
      const settleFinalRecord = async (attempt: number) => {
        const runs = await listRuns(token, pipelineId).catch(() => [] as RunRecord[])
        const record = runs.find((r) => r.id === id)
        if (record && record.finished_at) {
          applyFinalRecord(record)
        } else if (attempt < SETTLE_POLL_MAX_ATTEMPTS) {
          setTimeout(() => void settleFinalRecord(attempt + 1), SETTLE_POLL_INTERVAL_MS)
        }
        // Still not settled after ~5s: leave the UI in 'running' — the runs
        // list shows the authoritative state on the next page load.
      }

      const ws = new WebSocket(progressSocketUrl(pipelineId, id, token))
      wsRef.current = ws

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data as string) as ProgressEvent
        const now = performance.now()
        const prev = lastSample.current[data.partition_id]
        const elapsedSeconds = prev ? (now - prev.at) / 1000 : 0
        const rowsPerSecond =
          prev && elapsedSeconds > 0
            ? (data.rows_written - prev.event.rows_written) / elapsedSeconds
            : 0
        const mbPerSecond =
          prev && elapsedSeconds > 0
            ? (data.bytes_written - prev.event.bytes_written) / elapsedSeconds / 1_000_000
            : 0
        lastSample.current[data.partition_id] = { event: data, at: now }
        setPartitions((current) => ({
          ...current,
          [data.partition_id]: { ...data, rowsPerSecond, mbPerSecond },
        }))
      }

      ws.onclose = () => {
        void settleFinalRecord(0)
      }
    },
    [applyFinalRecord],
  )

  const run = useCallback(
    async (token: string, spec: PipelineSpec) => {
      setStatus('starting')
      setPartitions({})
      setError(null)
      setDbtSummary(null)
      setRunId(null)
      lastSample.current = {}
      wsRef.current?.close()

      try {
        const { run_id } = await runPipeline(token, spec)
        setRunId(run_id)
        setStatus('running')
        connectSocket(token, spec.pipeline_id, run_id)
      } catch (err) {
        // Validation-level failures (bad body, path/id mismatch) are
        // rejected synchronously with 4xx before any run row exists.
        setStatus('failed')
        setError(err instanceof Error ? err.message : String(err))
      }
    },
    [connectSocket],
  )

  return { status, runId, partitions, error, dbtSummary, run }
}
