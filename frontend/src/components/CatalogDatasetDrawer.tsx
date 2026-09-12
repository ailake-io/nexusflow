import { useEffect, useState } from 'react'
import { AlertCircle, Loader2, X } from 'lucide-react'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import {
  getCatalogDataset,
  updateCatalogColumn,
  updateCatalogDataset,
  type CatalogColumn,
  type CatalogDataset,
} from '@/lib/api'

interface ColumnRowProps {
  column: CatalogColumn
  onSaveDescription: (description: string | null) => void
  onTogglePii: (piiFlag: boolean) => void
}

/** Own local state for the description field so typing doesn't fire a
 *  request per keystroke — saved on blur. The PII checkbox has no such
 *  concern (a single discrete toggle), so it saves immediately. */
function ColumnRow({ column, onSaveDescription, onTogglePii }: ColumnRowProps) {
  const { t } = useI18n()
  const [description, setDescription] = useState(column.description ?? '')

  useEffect(() => {
    setDescription(column.description ?? '')
  }, [column.description])

  return (
    <div className="rounded-md border border-white/5 bg-white/[0.02] p-2.5">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-xs text-foreground">{column.name}</span>
        <span className="shrink-0 text-[10px] text-muted-foreground">
          {column.data_type ?? t('catalog.drawer.neverObserved')}
        </span>
      </div>
      <div className="mt-2 flex items-center gap-2">
        <input
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          onBlur={() => {
            if (description !== (column.description ?? '')) {
              onSaveDescription(description.trim() || null)
            }
          }}
          placeholder={t('catalog.drawer.columnDescription')}
          className="flex-1 rounded-md border border-white/10 bg-background px-2 py-1 text-[11px] text-foreground focus:border-primary/40 focus:outline-none"
        />
        <label className="flex shrink-0 items-center gap-1 text-[11px] text-muted-foreground">
          <input
            type="checkbox"
            checked={column.pii_flag}
            onChange={(e) => onTogglePii(e.target.checked)}
          />
          {t('catalog.drawer.columnPii')}
        </label>
      </div>
    </div>
  )
}

interface CatalogDatasetDrawerProps {
  datasetKey: string
  onClose: () => void
  /** Notified after any successful save, so the parent list (which shows a
   *  stale summary of the same dataset) can refresh without a full reload. */
  onUpdated?: (dataset: CatalogDataset) => void
}

/** Detail/edit panel for one cataloged dataset — same slide-in-from-the-
 *  right layout as `LineagePanel`'s `PipelineSchemaPanel`. Dataset-level
 *  fields (description/owner/tags) are edited together with an explicit
 *  Save button; column-level fields (description/PII flag) save
 *  individually as soon as they change (see `ColumnRow` above). */
export function CatalogDatasetDrawer({ datasetKey, onClose, onUpdated }: CatalogDatasetDrawerProps) {
  const { token } = useAuth()
  const { t } = useI18n()
  const [dataset, setDataset] = useState<CatalogDataset | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [description, setDescription] = useState('')
  const [owner, setOwner] = useState('')
  const [tagsText, setTagsText] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveState, setSaveState] = useState<'idle' | 'saved' | 'error'>('idle')

  useEffect(() => {
    if (!token) return
    let cancelled = false
    setLoading(true)
    setError(null)
    getCatalogDataset(token, datasetKey)
      .then((d) => {
        if (cancelled) return
        setDataset(d)
        setDescription(d.description ?? '')
        setOwner(d.owner ?? '')
        setTagsText(d.tags.join(', '))
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : t('catalog.error'))
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [token, datasetKey, t])

  const handleSaveMetadata = async () => {
    if (!token) return
    setSaving(true)
    setSaveState('idle')
    const tags = tagsText
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
    try {
      await updateCatalogDataset(token, datasetKey, {
        description: description.trim() || null,
        owner: owner.trim() || null,
        tags,
      })
      const refreshed = await getCatalogDataset(token, datasetKey)
      setDataset(refreshed)
      setSaveState('saved')
      onUpdated?.(refreshed)
    } catch {
      setSaveState('error')
    } finally {
      setSaving(false)
    }
  }

  const handleColumnEdit = async (
    column: CatalogColumn,
    patch: { description?: string | null; pii_flag?: boolean },
  ) => {
    if (!token || !dataset) return
    const next = {
      description: patch.description !== undefined ? patch.description : column.description,
      pii_flag: patch.pii_flag !== undefined ? patch.pii_flag : column.pii_flag,
    }
    // Optimistic update — reverted below by refetching if the save fails.
    const optimistic: CatalogDataset = {
      ...dataset,
      columns: dataset.columns.map((c) => (c.name === column.name ? { ...c, ...next } : c)),
    }
    setDataset(optimistic)
    try {
      await updateCatalogColumn(token, datasetKey, column.name, next)
      onUpdated?.(optimistic)
    } catch {
      const refreshed = await getCatalogDataset(token, datasetKey).catch(() => null)
      if (refreshed) setDataset(refreshed)
    }
  }

  return (
    <div className="absolute right-0 top-0 z-10 flex h-full w-96 flex-col overflow-hidden border-l border-white/10 bg-card shadow-xl">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-foreground">{t('catalog.drawer.title')}</h2>
          <p className="truncate font-mono text-[11px] text-muted-foreground">{datasetKey}</p>
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label={t('catalog.drawer.close')}
          className="rounded-md p-1.5 text-muted-foreground hover:bg-white/5 hover:text-foreground"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="flex-1 overflow-auto p-4">
        {loading && (
          <div className="flex items-center justify-center py-8 text-xs text-muted-foreground">
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            {t('catalog.loading')}
          </div>
        )}

        {!loading && error && (
          <div className="flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
            <AlertCircle className="h-4 w-4" />
            {error}
          </div>
        )}

        {!loading && dataset && (
          <div className="flex flex-col gap-5">
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                {t('catalog.drawer.description')}
              </label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder={t('catalog.drawer.descriptionPlaceholder')}
                rows={2}
                className="w-full resize-none rounded-md border border-white/10 bg-background px-2 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                {t('catalog.drawer.owner')}
              </label>
              <input
                value={owner}
                onChange={(e) => setOwner(e.target.value)}
                placeholder={t('catalog.drawer.ownerPlaceholder')}
                className="w-full rounded-md border border-white/10 bg-background px-2 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">
                {t('catalog.drawer.tags')}
              </label>
              <input
                value={tagsText}
                onChange={(e) => setTagsText(e.target.value)}
                placeholder={t('catalog.drawer.tagsPlaceholder')}
                className="w-full rounded-md border border-white/10 bg-background px-2 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
              />
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={handleSaveMetadata}
                disabled={saving}
                className="rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {saving ? t('catalog.drawer.saving') : t('catalog.drawer.save')}
              </button>
              {saveState === 'saved' && (
                <span className="text-xs text-emerald-400">{t('catalog.drawer.saved')}</span>
              )}
              {saveState === 'error' && (
                <span className="text-xs text-red-400">{t('catalog.drawer.saveError')}</span>
              )}
            </div>

            <div>
              <div className="mb-2 text-xs font-medium text-muted-foreground">
                {t('catalog.drawer.columnsTitle')}
              </div>
              {dataset.columns.length === 0 ? (
                <p className="text-xs text-muted-foreground">{t('catalog.drawer.noColumns')}</p>
              ) : (
                <div className="flex flex-col gap-2">
                  {dataset.columns.map((col) => (
                    <ColumnRow
                      key={col.name}
                      column={col}
                      onSaveDescription={(description) => handleColumnEdit(col, { description })}
                      onTogglePii={(pii_flag) => handleColumnEdit(col, { pii_flag })}
                    />
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default CatalogDatasetDrawer
