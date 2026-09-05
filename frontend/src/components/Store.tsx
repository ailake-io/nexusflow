import { useEffect, useState, type FormEvent } from 'react'
import { Lock, CheckCircle2, Loader2, AlertCircle, KeyRound, ShoppingCart } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useI18n } from '@/lib/i18n'
import { useAuth } from '@/lib/auth-context'
import { useConnectors } from '@/hooks/useConnectors'
import {
  getLicenseStatus,
  installLicense,
  isLicensingConfigured,
  listLicensingProducts,
  createCheckout,
  type LicenseStatus,
  type LicensingProduct,
} from '@/lib/api'

type Currency = 'brl' | 'usd'

/**
 * Full enterprise connector catalog (docs/ENTERPRISE_CONNECTORS.md,
 * 2026-09-05 — 37 crates in the private nexus-connectors-enterprise repo).
 * None of these are registered connectors in *this* (OSS) binary — no
 * crate for them exists in this repo, by design (LICENSING.md §2). Shown
 * here purely for discovery: a plain-OSS user can see what exists and
 * what it takes to unlock it, without the running binary pretending to
 * have a `licensed` state for something it can't build at all. Grouped by
 * category (same grouping as ENTERPRISE_CONNECTORS.md) with one shared
 * blurb per category (`store.category*`) instead of a bespoke line per
 * connector — purchase flow intentionally deferred (ROADMAP.md Fase 12
 * Bloco 2/4 — no payment/pricing exists yet), so no buy button here even
 * though `licensingConfigured` guards one below for the (currently empty)
 * "available now" section.
 */
type EnterpriseCategory =
  | 'dw'
  | 'saas'
  | 'marketing'
  | 'office'
  | 'vector'
  | 'streaming'
  | 'cdc'

const ENTERPRISE_CATALOG: { slug: string; name: string; category: EnterpriseCategory }[] = [
  // Data warehouses / bancos analíticos enterprise
  { slug: 'snowflake', name: 'Snowflake', category: 'dw' },
  { slug: 'bigquery', name: 'BigQuery', category: 'dw' },
  { slug: 'redshift', name: 'Redshift', category: 'dw' },
  { slug: 'databricks', name: 'Databricks', category: 'dw' },
  { slug: 'oracle', name: 'Oracle', category: 'dw' },
  { slug: 'hana', name: 'SAP HANA', category: 'dw' },
  { slug: 'mssql', name: 'SQL Server / Synapse', category: 'dw' },
  { slug: 'teradata', name: 'Teradata', category: 'dw' },
  { slug: 'vertica', name: 'Vertica', category: 'dw' },
  { slug: 'starburst', name: 'Starburst (Trino)', category: 'dw' },
  // SaaS / CRM / ERP
  { slug: 'salesforce', name: 'Salesforce', category: 'saas' },
  { slug: 'hubspot', name: 'HubSpot', category: 'saas' },
  { slug: 'workday', name: 'Workday', category: 'saas' },
  { slug: 'netsuite', name: 'NetSuite', category: 'saas' },
  { slug: 'dynamics365', name: 'Dynamics 365', category: 'saas' },
  { slug: 'servicenow', name: 'ServiceNow', category: 'saas' },
  { slug: 'zendesk', name: 'Zendesk', category: 'saas' },
  // Marketing / Ads / Analytics
  { slug: 'ga4', name: 'Google Analytics 4', category: 'marketing' },
  { slug: 'google-ads', name: 'Google Ads', category: 'marketing' },
  { slug: 'meta-ads', name: 'Meta Ads', category: 'marketing' },
  { slug: 'linkedin-ads', name: 'LinkedIn Ads', category: 'marketing' },
  { slug: 'tiktok-ads', name: 'TikTok Ads', category: 'marketing' },
  { slug: 'x-ads', name: 'X Ads', category: 'marketing' },
  { slug: 'stripe', name: 'Stripe', category: 'marketing' },
  { slug: 'shopify', name: 'Shopify', category: 'marketing' },
  { slug: 'youtube-analytics', name: 'YouTube Analytics', category: 'marketing' },
  // Arquivos de escritório / produtividade
  { slug: 'excel', name: 'Excel', category: 'office' },
  { slug: 'google-sheets', name: 'Google Sheets', category: 'office' },
  { slug: 'sharepoint', name: 'SharePoint', category: 'office' },
  { slug: 'dropbox', name: 'Dropbox', category: 'office' },
  { slug: 'google-drive', name: 'Google Drive', category: 'office' },
  // Vetorial / busca enterprise
  { slug: 'elasticsearch', name: 'Elasticsearch / OpenSearch', category: 'vector' },
  { slug: 'weaviate', name: 'Weaviate', category: 'vector' },
  { slug: 'vertex-vector-search', name: 'Vertex AI Vector Search', category: 'vector' },
  { slug: 'azure-ai-search', name: 'Azure AI Search', category: 'vector' },
  // Streaming enterprise
  { slug: 'kinesis', name: 'Amazon Kinesis', category: 'streaming' },
  { slug: 'pulsar', name: 'Apache Pulsar', category: 'streaming' },
  // CDC avançado
  { slug: 'oracle-cdc', name: 'Oracle CDC', category: 'cdc' },
  { slug: 'mssql-cdc', name: 'SQL Server CDC', category: 'cdc' },
]

const CATEGORY_ORDER: EnterpriseCategory[] = [
  'dw',
  'saas',
  'marketing',
  'office',
  'vector',
  'streaming',
  'cdc',
]

/**
 * Store tab (ROADMAP.md Fase 12): lists enterprise connectors, marks which
 * ones the installed license already covers ("Adquirido"), and lets an
 * Admin install a license key (`POST /license`, same route/RBAC as
 * `UsersPanel`'s user management). Once installed, the exact same
 * `licensed` flag this page reads (`GET /connectors`) also unlocks the
 * connector in the Canvas's `ConnectorPalette` — one source of truth, no
 * separate "purchased" state to keep in sync.
 *
 * "Disponíveis agora" only ever lists connectors the running binary
 * actually has registered with `requires_license` (none in this repo
 * today — see `docs/ENTERPRISE_LICENSING.md`); the enterprise catalog
 * below it is the static list above, grouped by category, never treated
 * as real inventory this binary can build.
 */
export function Store() {
  const { t } = useI18n()
  const { token, role } = useAuth()
  const { connectors, loading, error } = useConnectors()

  const [license, setLicense] = useState<LicenseStatus | null>(null)
  const [licenseError, setLicenseError] = useState<string | null>(null)
  const [licenseKey, setLicenseKey] = useState('')
  const [installing, setInstalling] = useState(false)
  const [installError, setInstallError] = useState<string | null>(null)

  const licensingConfigured = isLicensingConfigured()
  const [products, setProducts] = useState<LicensingProduct[]>([])
  const [buyerEmail, setBuyerEmail] = useState('')
  const [currency, setCurrency] = useState<Currency>('brl')
  const [buyingSlug, setBuyingSlug] = useState<string | null>(null)
  const [buyError, setBuyError] = useState<string | null>(null)

  useEffect(() => {
    if (!licensingConfigured) return
    listLicensingProducts()
      .then(setProducts)
      .catch((err) => setBuyError(err instanceof Error ? err.message : String(err)))
  }, [licensingConfigured])

  const handleBuy = async (slug: string) => {
    const product = products.find((p) => p.connector_slug === slug && p.active)
    if (!product || !buyerEmail.trim()) return
    setBuyingSlug(slug)
    setBuyError(null)
    try {
      const { checkout_url } = await createCheckout([product.id], buyerEmail.trim(), currency)
      window.location.href = checkout_url
    } catch (err) {
      setBuyError(err instanceof Error ? err.message : String(err))
      setBuyingSlug(null)
    }
  }

  const refreshLicense = async () => {
    if (!token) return
    try {
      setLicense(await getLicenseStatus(token))
      setLicenseError(null)
    } catch (err) {
      setLicenseError(err instanceof Error ? err.message : String(err))
    }
  }

  useEffect(() => {
    void refreshLicense()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token])

  const handleInstall = async (e: FormEvent) => {
    e.preventDefault()
    if (!token || !licenseKey.trim()) return
    setInstalling(true)
    setInstallError(null)
    try {
      await installLicense(token, licenseKey.trim())
      setLicenseKey('')
      await refreshLicense()
    } catch (err) {
      setInstallError(err instanceof Error ? err.message : String(err))
    } finally {
      setInstalling(false)
    }
  }

  const enterpriseConnectors = connectors.filter((c) => c.requires_license)

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6">
        <h1 className="text-lg font-semibold tracking-tight">{t('store.title')}</h1>
        <p className="text-xs text-muted-foreground">{t('store.subtitle')}</p>
      </div>

      {licenseError && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {licenseError}
        </div>
      )}

      {license && (
        <div className="mb-6 rounded-xl border border-white/10 bg-card p-4">
          <div className="flex items-center gap-2 text-sm font-medium text-foreground">
            {license.active ? (
              <CheckCircle2 className="h-4 w-4 text-emerald-400" />
            ) : (
              <Lock className="h-4 w-4 text-muted-foreground" />
            )}
            {license.active ? t('store.licenseActive') : t('store.licenseNone')}
          </div>
          {license.active && (
            <p className="mt-1 text-xs text-muted-foreground">
              {t('store.licenseDetails', {
                connectors: license.connectors.length > 0 ? license.connectors.join(', ') : '—',
                seats: license.seats,
                expires: license.expires_at
                  ? new Date(license.expires_at * 1000).toLocaleDateString()
                  : '—',
              })}
            </p>
          )}
        </div>
      )}

      {licensingConfigured && (
        <div className="mb-6 rounded-xl border border-white/10 bg-card p-4">
          <p className="text-xs font-medium text-foreground">{t('store.billingTitle')}</p>
          <div className="mt-1.5 flex flex-wrap items-center gap-2">
            <Input
              type="email"
              value={buyerEmail}
              onChange={(e) => setBuyerEmail(e.target.value)}
              placeholder={t('store.billingEmailPlaceholder')}
              className="max-w-xs flex-1 text-xs"
            />
            <div className="flex overflow-hidden rounded-md border border-white/10">
              {(['brl', 'usd'] as const).map((c) => (
                <button
                  key={c}
                  type="button"
                  onClick={() => setCurrency(c)}
                  className={`px-2.5 py-1.5 text-xs font-medium uppercase transition-colors ${
                    currency === c
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:bg-white/5'
                  }`}
                >
                  {c}
                </button>
              ))}
            </div>
          </div>
          {buyError && <p className="mt-2 text-xs text-red-400">{buyError}</p>}
        </div>
      )}

      {role === 'admin' && (
        <form
          onSubmit={handleInstall}
          className="mb-8 rounded-xl border border-white/10 bg-card p-4"
        >
          <Label htmlFor="license-key" className="text-xs font-medium">
            {t('store.installLicense')}
          </Label>
          <div className="mt-1.5 flex gap-2">
            <Input
              id="license-key"
              value={licenseKey}
              onChange={(e) => setLicenseKey(e.target.value)}
              placeholder={t('store.licenseKeyPlaceholder')}
              className="flex-1 font-mono text-xs"
            />
            <Button type="submit" size="sm" disabled={installing || !licenseKey.trim()}>
              {installing ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <KeyRound className="h-3.5 w-3.5" />
              )}
              {t('store.install')}
            </Button>
          </div>
          {installError && <p className="mt-2 text-xs text-red-400">{installError}</p>}
        </form>
      )}

      {loading && (
        <div className="flex items-center gap-2 py-4 text-xs text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          {t('common.loading')}
        </div>
      )}
      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {error}
        </div>
      )}

      {enterpriseConnectors.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            {t('store.availableNow')}
          </h2>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {enterpriseConnectors.map((c) => {
              const product =
                !c.licensed && c.requires_license
                  ? products.find((p) => p.connector_slug === c.requires_license && p.active)
                  : undefined
              return (
                <div key={c.name} className="rounded-xl border border-white/10 bg-card p-4">
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-foreground">{c.name}</span>
                    {c.licensed ? (
                      <span className="flex items-center gap-1 rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-400">
                        <CheckCircle2 className="h-3 w-3" /> {t('store.acquired')}
                      </span>
                    ) : (
                      <span className="flex items-center gap-1 rounded-full bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-400">
                        <Lock className="h-3 w-3" /> {t('store.locked')}
                      </span>
                    )}
                  </div>
                  {product && (
                    <Button
                      size="sm"
                      variant="outline"
                      className="mt-3 w-full"
                      disabled={buyingSlug === product.connector_slug || !buyerEmail.trim()}
                      onClick={() => handleBuy(product.connector_slug)}
                    >
                      {buyingSlug === product.connector_slug ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <ShoppingCart className="h-3.5 w-3.5" />
                      )}
                      {t('store.buy', {
                        price:
                          currency === 'brl'
                            ? `R$ ${(product.price_cents_brl / 100).toFixed(2)}`
                            : `US$ ${(product.price_cents_usd / 100).toFixed(2)}`,
                      })}
                    </Button>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}

      <div>
        <h2 className="mb-1 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {t('store.comingSoon')}
        </h2>
        <p className="mb-3 text-xs text-muted-foreground">{t('store.enterpriseCatalogNote')}</p>
        {CATEGORY_ORDER.map((category) => (
          <div key={category} className="mb-6">
            <h3 className="mb-2 text-[11px] font-semibold text-muted-foreground">
              {t(`store.category.${category}`)}
            </h3>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {ENTERPRISE_CATALOG.filter((c) => c.category === category).map((c) => (
                <div
                  key={c.slug}
                  className="rounded-xl border border-white/10 bg-card/50 p-4 opacity-80"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-foreground">{c.name}</span>
                    <span className="rounded-full bg-white/5 px-2 py-0.5 text-[10px] font-medium text-muted-foreground">
                      {t('store.comingSoonBadge')}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

export default Store
