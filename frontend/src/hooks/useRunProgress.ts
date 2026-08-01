import { useCallback, useRef, useState } from 'react'
import {
  listRuns,
  progressSocketUrl,
  runPipeline,
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
  run: (token: string, spec: PipelineSpec) => void
}

const POLL_INTERVAL_MS = 250
const POLL_TIMEOUT_MS = 15000

/**
 * Drives the "run + watch live progress" flow (Marco 8 task #16). The run
 * endpoint (POST /pipelines/{id}/run) only resolves once the whole pipeline
 * finishes, so the run's id — needed to open the progress WebSocket — has
 * to be discovered by polling GET /pipelines/{id}/runs concurrently rather
 * than awaited from the POST itself. This works because `start_run`
 * commits to the DB before the (possibly slow) connector work begins, so a
 * poll fired right after the POST sees the new "running" row while that
 * POST is still in flight on the server (ARCHITECTURE.md §8).
 */
export function useRunProgress(): UseRunProgressResult {
  const [status, setStatus] = useState<ExecutionStatus>('idle')
  const [runId, setRunId] = useState<number | null>(null)
  const [partitions, setPartitions] = useState<Record<string, PartitionProgress>>({})
  const [error, setError] = useState<string | null>(null)
  const lastSample = useRef<Record<string, { event: ProgressEvent; at: number }>>({})
  const wsRef = useRef<WebSocket | null>(null)

  const applyFinalRecord = useCallback((record: RunRecord) => {
    setStatus(record.status === 'success' ? 'success' : 'failed')
    setError(record.error)
  }, [])

  const connectSocket = useCallback(
    (token: string, pipelineId: string, id: number) => {
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
        // The run finished (server closes once done) — re-fetch the
        // authoritative record instead of trusting the last progress event,
        // since a failure can happen between two progress events.
        listRuns(token, pipelineId).then((runs) => {
          const record = runs.find((r) => r.id === id)
          if (record) applyFinalRecord(record)
        })
      }
    },
    [applyFinalRecord],
  )

  const run = useCallback(
    async (token: string, spec: PipelineSpec) => {
      setStatus('starting')
      setPartitions({})
      setError(null)
      setRunId(null)
      lastSample.current = {}
      wsRef.current?.close()

      const baseline = await listRuns(token, spec.pipeline_id).catch(() => [] as RunRecord[])
      const baselineMaxId = baseline.reduce((max, r) => Math.max(max, r.id), 0)

      const runPromise = runPipeline(token, spec).catch((err: unknown) => {
        // Validation-level failures (bad body, path/id mismatch) never
        // reach start_run — no run row exists to poll for, so surface
        // directly instead of polling until POLL_TIMEOUT_MS elapses.
        setStatus('failed')
        setError(err instanceof Error ? err.message : String(err))
      })

      const deadline = Date.now() + POLL_TIMEOUT_MS
      const poll = async () => {
        const runs = await listRuns(token, spec.pipeline_id).catch(() => [] as RunRecord[])
        const latest = runs.find((r) => r.id > baselineMaxId)
        if (!latest) {
          if (Date.now() < deadline) setTimeout(poll, POLL_INTERVAL_MS)
          return
        }
        setRunId(latest.id)
        if (latest.status === 'running') {
          setStatus('running')
          connectSocket(token, spec.pipeline_id, latest.id)
        } else {
          applyFinalRecord(latest)
        }
      }
      void poll()
      void runPromise
    },
    [applyFinalRecord, connectSocket],
  )

  return { status, runId, partitions, error, run }
}
