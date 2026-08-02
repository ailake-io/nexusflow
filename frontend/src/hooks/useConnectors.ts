import { useEffect, useState } from 'react'
import { listConnectors, type ConnectorDescriptor } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'

interface UseConnectorsResult {
  connectors: ConnectorDescriptor[]
  loading: boolean
  error: string | null
}

/**
 * Fetches the dynamic connector catalog (GET /connectors) — the canvas's
 * node palette comes from here, never a hardcoded frontend list
 * (ARCHITECTURE.md §3, IMPLEMENTATION_PLAN.md Marco 8).
 */
export function useConnectors(): UseConnectorsResult {
  const { token } = useAuth()
  const [connectors, setConnectors] = useState<ConnectorDescriptor[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    listConnectors(token)
      .then((result) => {
        if (!cancelled) setConnectors(result)
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
  }, [token])

  return { connectors, loading, error }
}
