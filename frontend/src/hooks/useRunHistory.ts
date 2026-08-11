import { useCallback, useEffect, useState } from 'react'
import { listRuns, type RunRecord } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'

interface UseRunHistoryResult {
  runs: RunRecord[]
  loading: boolean
  error: string | null
  refresh: () => void
}

/** Fetches the run history (GET /pipelines/{id}/runs) for one pipeline —
 * `null` pipelineId means "not open yet", so the panel that renders this
 * on demand doesn't fire a request until the user actually asks to see it. */
export function useRunHistory(pipelineId: string | null): UseRunHistoryResult {
  const { token } = useAuth()
  const [runs, setRuns] = useState<RunRecord[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [generation, setGeneration] = useState(0)

  const refresh = useCallback(() => setGeneration((g) => g + 1), [])

  useEffect(() => {
    if (!token || !pipelineId) return
    let cancelled = false
    setLoading(true)
    setError(null)
    listRuns(token, pipelineId)
      .then((result) => {
        if (!cancelled) setRuns(result)
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, pipelineId, generation])

  return { runs, loading, error, refresh }
}
