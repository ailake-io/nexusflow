import { useEffect, useMemo, useState } from 'react'
import { AlertCircle, Database, Loader2, ShieldAlert } from 'lucide-react'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { listCatalogDatasets, listCatalogTags, type CatalogDataset } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'
import { CatalogDatasetDrawer } from '@/components/CatalogDatasetDrawer'

function hasPii(dataset: CatalogDataset): boolean {
  return dataset.columns.some((c) => c.pii_flag)
}

/**
 * "Data Catalog" tab (Fase 25): every dataset a saved pipeline has touched,
 * searchable/filterable, with editable description/owner/tags and
 * per-column description/PII flag. Loads the full (unfiltered) list once —
 * same "dataset counts are small, filter client-side" posture the backend
 * itself documents (`data_catalog::CatalogStore::list`) — and applies every
 * filter in-memory here too, avoiding a second server round-trip per
 * keystroke.
 */
export function DataCatalogPanel() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [datasets, setDatasets] = useState<CatalogDataset[]>([])
  const [tags, setTags] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [query, setQuery] = useState('')
  const [tagFilter, setTagFilter] = useState('')
  const [connectorFilter, setConnectorFilter] = useState('')
  const [piiOnly, setPiiOnly] = useState(false)

  const [selectedKey, setSelectedKey] = useState<string | null>(null)

  const loadAll = () => {
    if (!token) return
    setLoading(true)
    Promise.all([listCatalogDatasets(token), listCatalogTags(token)])
      .then(([d, tg]) => {
        setDatasets(d)
        setTags(tg)
        setError(null)
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : t('catalog.error'))
      })
      .finally(() => setLoading(false))
  }

  useEffect(loadAll, [token]) // eslint-disable-line react-hooks/exhaustive-deps

  const connectors = useMemo(
    () => Array.from(new Set(datasets.map((d) => d.connector))).sort(),
    [datasets],
  )

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    return datasets.filter((d) => {
      if (q) {
        const haystack = `${d.dataset_key} ${d.identifier} ${d.description ?? ''}`.toLowerCase()
        if (!haystack.includes(q)) return false
      }
      if (tagFilter && !d.tags.includes(tagFilter)) return false
      if (connectorFilter && d.connector !== connectorFilter) return false
      if (piiOnly && !hasPii(d)) return false
      return true
    })
  }, [datasets, query, tagFilter, connectorFilter, piiOnly])

  // Keeps the list row in sync after an edit in the drawer, without a full
  // reload — the drawer already has the freshest copy of the dataset.
  const handleDatasetUpdated = (updated: CatalogDataset) => {
    setDatasets((prev) => prev.map((d) => (d.dataset_key === updated.dataset_key ? updated : d)))
  }

  return (
    <div className="relative h-full overflow-hidden">
      <div className="h-full overflow-auto p-6">
        <div className="mb-6">
          <h1 className="text-lg font-semibold tracking-tight">{t('catalog.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('catalog.subtitle')}</p>
        </div>

        <div className="mb-4 flex flex-wrap items-center gap-2">
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={t('catalog.searchPlaceholder')}
            className="min-w-[200px] flex-1 rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
          />
          <select
            value={connectorFilter}
            onChange={(e) => setConnectorFilter(e.target.value)}
            className="rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
          >
            <option value="">{t('catalog.allConnectors')}</option>
            {connectors.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
          <select
            value={tagFilter}
            onChange={(e) => setTagFilter(e.target.value)}
            className="rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
          >
            <option value="">{t('catalog.allTags')}</option>
            {tags.map((tag) => (
              <option key={tag} value={tag}>
                {tag}
              </option>
            ))}
          </select>
          <label className="flex items-center gap-1.5 rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground">
            <input type="checkbox" checked={piiOnly} onChange={(e) => setPiiOnly(e.target.checked)} />
            {t('catalog.piiOnly')}
          </label>
        </div>

        {loading && (
          <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
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

        {!loading && !error && datasets.length === 0 && (
          <EmptyState
            icon={<Database className="h-6 w-6" />}
            title={t('catalog.emptyTitle')}
            description={t('catalog.emptyDescription')}
          />
        )}

        {!loading && !error && datasets.length > 0 && filtered.length === 0 && (
          <p className="text-xs text-muted-foreground">{t('catalog.noResults')}</p>
        )}

        {!loading && !error && filtered.length > 0 && (
          <div className="flex flex-col gap-2">
            {filtered.map((d) => (
              <button
                key={d.dataset_key}
                type="button"
                onClick={() => setSelectedKey(d.dataset_key)}
                className="flex flex-col gap-1.5 rounded-lg border border-white/10 bg-card p-3.5 text-left transition-colors hover:border-primary/30"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="font-mono text-xs font-medium text-foreground">{d.identifier}</span>
                  <div className="flex shrink-0 items-center gap-1.5">
                    {hasPii(d) && (
                      <span className="flex items-center gap-1 rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-medium text-amber-400">
                        <ShieldAlert className="h-3 w-3" />
                        {t('catalog.piiBadge')}
                      </span>
                    )}
                    <span className="rounded-full bg-white/5 px-2 py-0.5 text-[10px] text-muted-foreground">
                      {d.connector}
                    </span>
                  </div>
                </div>
                {d.description && (
                  <p className="text-xs text-muted-foreground">{d.description}</p>
                )}
                <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted-foreground">
                  {d.owner && <span>{t('catalog.owner')}: {d.owner}</span>}
                  <span>{t('catalog.columns', { count: d.columns.length })}</span>
                  {d.tags.map((tag) => (
                    <span key={tag} className="rounded-full bg-white/5 px-1.5 py-0.5">
                      {tag}
                    </span>
                  ))}
                </div>
              </button>
            ))}
          </div>
        )}
      </div>

      {selectedKey && (
        <CatalogDatasetDrawer
          datasetKey={selectedKey}
          onClose={() => setSelectedKey(null)}
          onUpdated={handleDatasetUpdated}
        />
      )}
    </div>
  )
}

export default DataCatalogPanel
