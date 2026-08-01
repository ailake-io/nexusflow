/** Matches nexus-core::ConnectorCapability (ARCHITECTURE.md §3). */
export type ConnectorCapability = 'adbc_native' | 'arrow_flight' | 'bridged'

/** Matches nexus-core::ConnectorDescriptor, as returned by GET /connectors. */
export interface ConnectorDescriptor {
  name: string
  capability: ConnectorCapability
}

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(path: string, init: RequestInit = {}, token?: string): Promise<T> {
  const headers = new Headers(init.headers)
  if (token) headers.set('authorization', `Bearer ${token}`)
  if (init.body) headers.set('content-type', 'application/json')

  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new ApiError(response.status, body?.error ?? response.statusText)
  }
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
