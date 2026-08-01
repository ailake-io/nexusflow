import { createContext, use, useCallback, useState, type ReactNode } from 'react'
import { login as apiLogin } from '@/lib/api'

const STORAGE_KEY = 'nexusflow.token'

interface AuthContextValue {
  token: string | null
  login: (username: string, password: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

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

export function useAuth(): AuthContextValue {
  const ctx = use(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
