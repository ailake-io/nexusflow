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
 * The three LLMOps capability slugs (`capability_registry.rs`,
 * LLMOPS_IMPLEMENTATION_PLAN.md Marco L8) — license-check targets for
 * features that live in *this* public binary, not connector crates.
 * `GET /connectors` deliberately never lists them (`ConnectorCapability::
 * Capability` is filtered out server-side, `lib.rs`'s
 * `list_connectors_handler` — the Canvas must never offer them as a
 * pipeline node), so unlike `enterpriseConnectors` below this list is
 * static here and cross-checked against `license.connectors` directly
 * instead of a `licensed` flag from the API. Sold as separate line items
 * (`nexus-licensing` products, one `connector_slug` per capability) —
 * `createCheckout` already accepts multiple `product_ids` in one purchase
 * if a buyer wants more than one, but each keeps its own price/on-off
 * state, e.g. Generation Traceability is not bundled into the others.
 *
 * `llm-lineage-tracking`'s label is "Generation Traceability (RAG)", not
 * "Data Quality & Lineage" — it only gates `GET /lineage/generation/{id}`
 * (which prompt/model/row produced one specific RAG answer, `rag.rs`'s
 * `generation_detail_handler`). The Quality tab (`quality_check_store.rs`)
 * and the base pipeline-lineage graph (`GET /lineage`) stay OSS, ungated —
 * a broader label here would make a buyer think they're paying for those
 * too.
 */
const LLMOPS_CAPABILITIES: { slug: string; labelKey: string }[] = [
  { slug: 'llm-lineage-tracking', labelKey: 'store.capability.generationTraceability' },
  { slug: 'reactive-rag-cdc', labelKey: 'store.capability.reactiveRag' },
  { slug: 'git-history-github-sync', labelKey: 'store.capability.gitHistorySync' },
]

/**
 * Ads connectors pulled out of `connectors-all`/`connectors-all-no-embeddings`
 * (`nexus-connectors-enterprise`'s `bin/Cargo.toml`) on 2026-09-10 — none
 * validated against a real ad account yet (`docs/ENTERPRISE_CONNECTORS.md`
 * §3, `docs/PENDING_REAL_ACCOUNT_VALIDATION.md`). The crates still exist and
 * compile standalone, they just aren't in any binary shipped to a customer
 * right now, so `GET /connectors` never lists them — same reasoning as
 * `LLMOPS_CAPABILITIES` above (static list here, not derived from the API),
 * but rendered as a plain "Em breve" badge, no buy flow: there's no license
 * slug to gate and no product to sell for a connector that isn't compiled
 * into the binary at all. Remove an entry here the same day it goes back
 * into `connectors-all` after passing a real test.
 */
const ADS_CONNECTORS_COMING_SOON: { slug: string; name: string }[] = [
  { slug: 'google-ads', name: 'Google Ads' },
  { slug: 'linkedin-ads', name: 'LinkedIn Ads' },
  { slug: 'meta-ads', name: 'Meta Ads' },
  { slug: 'tiktok-ads', name: 'TikTok Ads' },
  { slug: 'x-ads', name: 'X Ads' },
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
 * "Disponíveis agora" lists every connector the running binary actually
 * has registered with `requires_license` (`GET /connectors`) — as of
 * 2026-09-10 that's the enterprise catalog minus the 5 ads connectors
 * (see `ADS_CONNECTORS_COMING_SOON` above), which do get their own static
 * "Em breve" section since they're real, just not shippable yet.
 *
 * "LLMOps" is a third, separate section below it: those three slugs
 * aren't connectors at all (see `LLMOPS_CAPABILITIES` above), so they
 * can't come from the same `enterpriseConnectors` list.
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

      <div className="mb-8">
        <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {t('store.comingSoon')}
        </h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {ADS_CONNECTORS_COMING_SOON.map((c) => (
            <div key={c.slug} className="rounded-xl border border-white/10 bg-card p-4 opacity-70">
              <div className="flex items-center justify-between">
                <span className="font-medium text-foreground">{c.name}</span>
                <span className="flex items-center gap-1 rounded-full bg-white/5 px-2 py-0.5 text-[10px] font-medium text-muted-foreground">
                  {t('store.comingSoonBadge')}
                </span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {licensingConfigured && (
        <div className="mb-8">
          <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            {t('store.capabilitiesTitle')}
          </h2>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {LLMOPS_CAPABILITIES.map((cap) => {
              const acquired = license?.connectors.includes(cap.slug) ?? false
              const product = !acquired
                ? products.find((p) => p.connector_slug === cap.slug && p.active)
                : undefined
              return (
                <div key={cap.slug} className="rounded-xl border border-white/10 bg-card p-4">
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-foreground">{t(cap.labelKey)}</span>
                    {acquired ? (
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

    </div>
  )
}

export default Store
