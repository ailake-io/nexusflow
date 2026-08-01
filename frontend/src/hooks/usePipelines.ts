import { useCallback, useEffect, useState } from 'react'
import { listPipelines, type PipelineSummary } from '@/lib/api'
import { useAuth } from '@/lib/auth'

interface UsePipelinesResult {
  pipelines: PipelineSummary[]
  loading: boolean
  error: string | null
  refresh: () => void
}

/** Fetches the persisted pipeline catalog (GET /pipelines) for the masked
 * credentials screen (Marco 8 task #17) — summaries only, never a config
 * blob, since that's where connector secrets live. */
export function usePipelines(): UsePipelinesResult {
  const { token } = useAuth()
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [generation, setGeneration] = useState(0)

  const refresh = useCallback(() => setGeneration((g) => g + 1), [])

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    listPipelines(token)
      .then((result) => {
        if (!cancelled) setPipelines(result)
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
  }, [token, generation])

  return { pipelines, loading, error, refresh }
}
