import { useEffect, useState, type FormEvent } from 'react'
import { AlertCircle, BookText, Loader2, Plus } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { createPrompt, listPrompts, type PromptTemplate } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'

/** One name's full version history, newest first — matches
 *  `GET /prompts`'s own ordering (`prompt_template_store.rs::list`). */
interface PromptGroup {
  name: string
  versions: PromptTemplate[]
}

function groupByName(prompts: PromptTemplate[]): PromptGroup[] {
  const order: string[] = []
  const byName = new Map<string, PromptTemplate[]>()
  for (const p of prompts) {
    if (!byName.has(p.name)) {
      byName.set(p.name, [])
      order.push(p.name)
    }
    byName.get(p.name)!.push(p)
  }
  return order.map((name) => ({ name, versions: byName.get(name)! }))
}

/**
 * "Prompts" tab (LLMOPS_IMPLEMENTATION_PLAN.md Marco L4): lists every
 * saved prompt template grouped by name (newest version first), and a form
 * to save a new version. Never edits/overwrites an existing version —
 * `POST /prompts` always creates the next one (`PromptTemplateStore`'s own
 * immutability guarantee), same principle as license keys/checkpoints.
 */
export function PromptLibrary() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [prompts, setPrompts] = useState<PromptTemplate[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [name, setName] = useState('')
  const [template, setTemplate] = useState('')
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)

  const refresh = () => {
    if (!token) return
    setLoading(true)
    listPrompts(token)
      .then((result) => {
        setPrompts(result)
        setError(null)
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : t('prompts.error')))
      .finally(() => setLoading(false))
  }

  useEffect(refresh, [token, t])

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault()
    if (!token || !name.trim() || !template.trim()) return
    setCreating(true)
    setCreateError(null)
    try {
      await createPrompt(token, name.trim(), template)
      setName('')
      setTemplate('')
      refresh()
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : t('prompts.createError'))
    } finally {
      setCreating(false)
    }
  }

  const groups = groupByName(prompts)

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6">
        <h1 className="text-lg font-semibold tracking-tight">{t('prompts.title')}</h1>
        <p className="text-xs text-muted-foreground">{t('prompts.subtitle')}</p>
      </div>

      <form
        onSubmit={handleCreate}
        className="mb-6 flex flex-col gap-3 rounded-lg border border-white/10 bg-card p-4"
      >
        <div className="flex items-center gap-2 text-xs font-medium text-foreground">
          <Plus className="h-3.5 w-3.5" />
          {t('prompts.newPrompt')}
        </div>
        <div>
          <Label htmlFor="prompt-name" className="text-xs font-medium">
            {t('prompts.name')}
          </Label>
          <Input
            id="prompt-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder={t('prompts.namePlaceholder')}
            className="mt-1 h-8 text-xs"
          />
        </div>
        <div>
          <Label htmlFor="prompt-template" className="text-xs font-medium">
            {t('prompts.template')}
          </Label>
          <textarea
            id="prompt-template"
            value={template}
            onChange={(e) => setTemplate(e.target.value)}
            placeholder={t('prompts.templatePlaceholder')}
            rows={3}
            className="mt-1 w-full rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
          />
        </div>
        {createError && (
          <div className="flex items-center gap-2 text-xs text-red-400">
            <AlertCircle className="h-3.5 w-3.5 shrink-0" />
            {createError}
          </div>
        )}
        <Button
          type="submit"
          size="sm"
          disabled={creating || !name.trim() || !template.trim()}
          className="w-fit gap-1.5"
        >
          {creating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
          {creating ? t('prompts.creating') : t('prompts.create')}
        </Button>
      </form>

      {loading && (
        <div className="flex h-24 items-center justify-center text-xs text-muted-foreground">
          <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />
          {t('prompts.loading')}
        </div>
      )}

      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {!loading && !error && groups.length === 0 && (
        <EmptyState icon={<BookText className="h-6 w-6" />} title={t('prompts.empty')} />
      )}

      {!loading && !error && groups.length > 0 && (
        <div className="flex flex-col gap-4">
          {groups.map((group) => (
            <div key={group.name} className="rounded-lg border border-white/10 bg-card p-4">
              <div className="mb-2 font-mono text-xs font-medium text-foreground">{group.name}</div>
              <div className="flex flex-col gap-2">
                {group.versions.map((p) => (
                  <div
                    key={p.version}
                    className="rounded-md border border-white/5 bg-white/[0.02] p-2.5"
                  >
                    <div className="flex items-center justify-between gap-2 text-[10px] text-muted-foreground">
                      <span className="font-medium text-foreground">
                        {t('prompts.version', { version: p.version })}
                      </span>
                      <span>{t('prompts.createdAt', { created: new Date(p.created_at).toLocaleString() })}</span>
                    </div>
                    <pre className="mt-1.5 whitespace-pre-wrap break-words text-xs text-foreground">
                      {p.template}
                    </pre>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default PromptLibrary
