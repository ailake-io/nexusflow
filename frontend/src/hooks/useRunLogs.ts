import { useCallback, useState } from 'react'
import { listRunLogs, type RunLogEvent } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'

interface UseRunLogsResult {
  logs: Omit<RunLogEvent, 'type'>[]
  loading: boolean
  error: string | null
  fetchLogs: (pipelineId: string, runId: number) => void
}

/**
 * On-demand fetch of a run's persisted execution log — the counterpart to
 * `useRunProgress`'s live `logs` array (which only fills in while a
 * WebSocket is open). Used from `RunHistoryPanel` so "ver logs" works for
 * any past run, including one the scheduler fired that nobody was watching
 * live (see `ARCHITECTURE.md` on `RunLogStore`).
 */
export function useRunLogs(): UseRunLogsResult {
  const { token } = useAuth()
  const [logs, setLogs] = useState<Omit<RunLogEvent, 'type'>[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchLogs = useCallback(
    (pipelineId: string, runId: number) => {
      if (!token) return
      setLoading(true)
      setError(null)
      listRunLogs(token, pipelineId, runId)
        .then(setLogs)
        .catch((err) => setError(err instanceof Error ? err.message : String(err)))
        .finally(() => setLoading(false))
    },
    [token],
  )

  return { logs, loading, error, fetchLogs }
}
