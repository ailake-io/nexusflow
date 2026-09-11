import { useState, type DragEvent } from 'react'
import { Box, Loader2, AlertCircle, Search } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import type { InfraModuleDescriptor } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'

interface InfraModulePaletteProps {
  modules: InfraModuleDescriptor[]
  loading: boolean
  error: string | null
}

function onDragStart(event: DragEvent<HTMLDivElement>, moduleId: string) {
  event.dataTransfer.setData('application/nexusflow-infra-module', moduleId)
  event.dataTransfer.effectAllowed = 'move'
}

/**
 * Mirrors `ConnectorPalette.tsx` exactly (search box, drag-to-canvas), but
 * grouped by `category` ("network", "iam", "data", "ai", "cicd", ...) since
 * the Infra catalog is small-but-varied rather than one flat list of
 * connector names — a buyer scanning for "something CI/CD-ish" benefits
 * from grouping in a way a data-connector list didn't need.
 */
export function InfraModulePalette({ modules, loading, error }: InfraModulePaletteProps) {
  const { t } = useI18n()
  const [query, setQuery] = useState('')

  const filtered = query.trim()
    ? modules.filter((m) => m.name.toLowerCase().includes(query.trim().toLowerCase()))
    : modules

  const byCategory = new Map<string, InfraModuleDescriptor[]>()
  for (const m of filtered) {
    const list = byCategory.get(m.category) ?? []
    list.push(m)
    byCategory.set(m.category, list)
  }

  return (
    <aside className="flex w-60 shrink-0 flex-col border-r bg-card">
      <div className="border-b border-white/10 px-4 py-3">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {t('infra.modules')}
        </h2>
        <p className="mt-0.5 text-[10px] text-muted-foreground/70">{t('infra.dragToCanvas')}</p>
        <div className="relative mt-2">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={t('infra.searchModules')}
            aria-label={t('infra.searchModules')}
            className="h-7 w-full rounded-md border border-input bg-transparent pl-7 pr-2 text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
      </div>

      <div className="flex-1 overflow-auto p-3">
        {loading && (
          <div className="flex items-center gap-2 py-4 text-xs text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            {t('common.loading')}
          </div>
        )}
        {error && (
          <div className="flex items-start gap-2 rounded-md border border-red-500/20 bg-red-500/10 p-2 text-xs text-red-400">
            <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            {error}
          </div>
        )}
        {!loading && !error && modules.length === 0 && (
          <EmptyState
            icon={<Box className="h-5 w-5" />}
            title={t('infra.noModules')}
            description={t('infra.noModulesDesc')}
            className="p-3"
          />
        )}
        {[...byCategory.entries()].map(([category, mods]) => (
          <div key={category} className="mb-4">
            <h3 className="mb-1.5 px-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground/70">
              {category}
            </h3>
            <div className="flex flex-col gap-1.5">
              {mods.map((mod) => (
                <div
                  key={mod.id}
                  draggable
                  onDragStart={(e) => onDragStart(e, mod.id)}
                  role="button"
                  tabIndex={0}
                  aria-label={t('infra.moduleNode', { name: mod.name })}
                  className="group flex cursor-grab items-center gap-2.5 rounded-lg border border-white/10 bg-white/[0.02] px-3 py-2 text-sm transition-colors hover:border-primary/30 hover:bg-primary/5 active:cursor-grabbing focus:outline-none focus:ring-2 focus:ring-primary/50"
                >
                  <Box className="h-3.5 w-3.5 text-muted-foreground group-hover:text-primary" />
                  <span className="font-medium text-foreground">{mod.name}</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </aside>
  )
}
