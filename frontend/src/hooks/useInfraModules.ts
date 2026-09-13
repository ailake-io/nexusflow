import { useEffect, useState } from 'react'
import { listInfraModules, type InfraModuleDescriptor } from '@/lib/api'
import { useAuth } from '@/lib/auth-context'

interface UseInfraModulesResult {
  modules: InfraModuleDescriptor[]
  loading: boolean
  error: string | null
}

/**
 * Mirrors `useConnectors.ts` exactly — fetches GET /infra/modules. An empty
 * result here means one of two things (deliberately indistinguishable to
 * this hook, see `infra.rs`'s doc comment): the enterprise
 * `nexus-infra-terraform` crate isn't linked into this binary at all, or it
 * is but no license covers `infra-terraform-generator` yet. `InfraCanvas`
 * treats both the same way — show the paywall panel.
 */
export function useInfraModules(): UseInfraModulesResult {
  const { token } = useAuth()
  const [modules, setModules] = useState<InfraModuleDescriptor[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    listInfraModules(token)
      .then((result) => {
        if (!cancelled) setModules(result)
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

  return { modules, loading, error }
}
