import { useEffect, useState } from 'react'
import { AlertCircle, Loader2, Shield, Trash2, UserPlus, Users } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { EmptyState } from '@/components/EmptyState'
import { useI18n } from '@/lib/i18n'
import { useAuth } from '@/lib/auth-context'
import {
  createUser,
  deleteUser,
  listUsers,
  updateUserRole,
  type Role,
  type UserRecord,
} from '@/lib/api'

const ROLES: Role[] = ['read', 'execute', 'write', 'admin']

const selectClassName =
  'flex h-9 w-full min-w-0 rounded-lg border border-white/10 bg-card px-3 py-1.5 text-sm text-foreground shadow-xs outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50'

/**
 * Admin-only user management (ROADMAP.md Pendências ativas #9): the backend
 * routes (`GET/POST /users`, `GET/DELETE /users/{username}`,
 * `PUT /users/{username}/role`) already existed and are Admin-role-gated
 * server-side — this is the first UI for them, previously API-only. This
 * component doesn't re-check the role itself; App.tsx only renders it for
 * an Admin-role token, and every request is enforced server-side regardless.
 */
export function UsersPanel() {
  const { t } = useI18n()
  const { token } = useAuth()
  const [users, setUsers] = useState<UserRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [newUsername, setNewUsername] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [newRole, setNewRole] = useState<Role>('read')
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)

  const [updatingUsername, setUpdatingUsername] = useState<string | null>(null)
  const [rowError, setRowError] = useState<string | null>(null)
  const [confirmDeleteUsername, setConfirmDeleteUsername] = useState<string | null>(null)

  const refresh = async () => {
    if (!token) return
    setLoading(true)
    setError(null)
    try {
      setUsers(await listUsers(token))
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void refresh()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!token) return
    setCreating(true)
    setCreateError(null)
    try {
      await createUser(token, newUsername, newPassword, newRole)
      setNewUsername('')
      setNewPassword('')
      setNewRole('read')
      await refresh()
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : String(err))
    } finally {
      setCreating(false)
    }
  }

  const handleRoleChange = async (username: string, role: Role) => {
    if (!token) return
    setUpdatingUsername(username)
    setRowError(null)
    try {
      await updateUserRole(token, username, role)
      await refresh()
    } catch (err) {
      setRowError(err instanceof Error ? err.message : String(err))
    } finally {
      setUpdatingUsername(null)
    }
  }

  const handleDelete = async (username: string) => {
    if (!token) return
    setUpdatingUsername(username)
    setRowError(null)
    try {
      await deleteUser(token, username)
      await refresh()
    } catch (err) {
      setRowError(err instanceof Error ? err.message : String(err))
    } finally {
      setUpdatingUsername(null)
      setConfirmDeleteUsername(null)
    }
  }

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6">
        <h1 className="text-lg font-semibold tracking-tight">{t('admin.title')}</h1>
        <p className="text-xs text-muted-foreground">{t('admin.subtitle')}</p>
      </div>

      <form
        onSubmit={handleCreate}
        className="mb-6 rounded-xl border border-white/10 bg-card p-4"
      >
        <div className="mb-3 flex items-center gap-2 text-sm font-medium text-foreground">
          <UserPlus className="h-4 w-4" />
          {t('admin.createUser')}
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div className="flex-1 min-w-[160px]">
            <label
              htmlFor="new-username"
              className="mb-1 block text-[10px] uppercase tracking-wider text-muted-foreground"
            >
              {t('admin.username')}
            </label>
            <Input
              id="new-username"
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              required
              disabled={creating}
            />
          </div>
          <div className="flex-1 min-w-[160px]">
            <label
              htmlFor="new-password"
              className="mb-1 block text-[10px] uppercase tracking-wider text-muted-foreground"
            >
              {t('admin.password')}
            </label>
            <Input
              id="new-password"
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              required
              minLength={8}
              disabled={creating}
            />
          </div>
          <div className="min-w-[140px]">
            <label
              htmlFor="new-role"
              className="mb-1 block text-[10px] uppercase tracking-wider text-muted-foreground"
            >
              {t('admin.role')}
            </label>
            <select
              id="new-role"
              value={newRole}
              onChange={(e) => setNewRole(e.target.value as Role)}
              disabled={creating}
              className={selectClassName}
              aria-label={t('admin.role')}
            >
              {ROLES.map((r) => (
                <option key={r} value={r}>
                  {t(`admin.roles.${r}`)}
                </option>
              ))}
            </select>
          </div>
          <Button type="submit" disabled={creating} className="gap-1.5">
            {creating ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <UserPlus className="h-3.5 w-3.5" />
            )}
            {creating ? t('admin.creating') : t('admin.createUser')}
          </Button>
        </div>
        {createError && (
          <div className="mt-3 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
            <AlertCircle className="h-4 w-4" />
            {createError}
          </div>
        )}
      </form>

      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}
      {rowError && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {rowError}
        </div>
      )}

      {loading && users.length === 0 && (
        <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('admin.loading')}
        </div>
      )}

      {!loading && users.length === 0 && (
        <EmptyState
          icon={<Users className="h-6 w-6" />}
          title={t('admin.emptyTitle')}
          description={t('admin.emptyDescription')}
        />
      )}

      <div className="flex flex-col gap-2">
        {users.map((u) => (
          <div
            key={u.username}
            className="flex items-center justify-between gap-4 rounded-xl border border-white/10 bg-card p-3"
          >
            <div className="flex items-center gap-2">
              <Shield className="h-4 w-4 text-muted-foreground" />
              <span className="font-medium text-foreground">{u.username}</span>
            </div>
            <div className="flex items-center gap-2">
              <select
                value={u.role}
                onChange={(e) => handleRoleChange(u.username, e.target.value as Role)}
                disabled={updatingUsername === u.username}
                className={`${selectClassName} w-auto`}
              >
                {ROLES.map((r) => (
                  <option key={r} value={r}>
                    {t(`admin.roles.${r}`)}
                  </option>
                ))}
              </select>
              {confirmDeleteUsername === u.username ? (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-red-300">{t('admin.deleteConfirm')}</span>
                  <Button
                    type="button"
                    variant="destructive"
                    size="sm"
                    disabled={updatingUsername === u.username}
                    onClick={() => handleDelete(u.username)}
                  >
                    {updatingUsername === u.username ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      t('admin.confirmDelete')
                    )}
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={updatingUsername === u.username}
                    onClick={() => setConfirmDeleteUsername(null)}
                  >
                    {t('admin.cancelDelete')}
                  </Button>
                </div>
              ) : (
                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  disabled={updatingUsername === u.username}
                  onClick={() => setConfirmDeleteUsername(u.username)}
                  className="gap-1.5"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                  {t('admin.delete')}
                </Button>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

export default UsersPanel
