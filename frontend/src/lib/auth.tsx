import { useCallback, useState, type ReactNode } from 'react'
import { login as apiLogin } from '@/lib/api'
import { AuthContext } from '@/lib/auth-context'

const STORAGE_KEY = 'nexusflow.token'

export function AuthProvider({ children }: { children: ReactNode }) {
  // sessionStorage, not localStorage — a JWT is a bearer credential, no
  // reason to outlive the tab. Re-login on every new session is fine for
  // the MVP self-host scale this targets.
  const [token, setToken] = useState<string | null>(() => sessionStorage.getItem(STORAGE_KEY))

  const login = useCallback(async (username: string, password: string) => {
    const newToken = await apiLogin(username, password)
    sessionStorage.setItem(STORAGE_KEY, newToken)
    setToken(newToken)
  }, [])

  const logout = useCallback(() => {
    sessionStorage.removeItem(STORAGE_KEY)
    setToken(null)
  }, [])

  return <AuthContext value={{ token, login, logout }}>{children}</AuthContext>
}
