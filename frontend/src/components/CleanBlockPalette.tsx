import { useState, type DragEvent } from 'react'
import { Search, Wand2 } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import type { CleanBlockKindTag } from '@/lib/dag'

/** Fase 30's v1 catalog, in the same order `ROADMAP.md` lists it. Unlike
 * `ConnectorPalette` (populated from `GET /connectors`), this list is
 * hardcoded — block kinds are a fixed part of the product, not a plugin
 * catalog. */
const CLEAN_BLOCK_KINDS: CleanBlockKindTag[] = [
  'filter',
  'select_columns',
  'rename',
  'cast',
  'trim',
  'replace_text',
  'fill_nulls',
  'drop_nulls',
  'dedupe',
  'change_case',
  'computed_column',
  'sort',
  'aggregate',
]

export const CLEAN_BLOCK_DRAG_TYPE = 'application/nexusflow-clean-block'

function onDragStart(event: DragEvent<HTMLDivElement>, blockKind: CleanBlockKindTag) {
  event.dataTransfer.setData(CLEAN_BLOCK_DRAG_TYPE, blockKind)
  event.dataTransfer.effectAllowed = 'move'
}

/**
 * No-code transformation palette (Fase 30) — sibling of `ConnectorPalette`,
 * shown in the same slot behind a "Transformações" tab (`DagCanvas.tsx`).
 * Drag a block onto the canvas to add it, same drag-and-drop affordance as
 * a connector, just a different `dataTransfer` MIME type so the canvas's
 * `onDrop` can tell the two apart.
 */
export function CleanBlockPalette() {
  const { t } = useI18n()
  const [query, setQuery] = useState('')

  const filtered = query.trim()
    ? CLEAN_BLOCK_KINDS.filter((k) =>
        t(`canvas.clean.kind.${k}`).toLowerCase().includes(query.trim().toLowerCase()),
      )
    : CLEAN_BLOCK_KINDS

  return (
    <aside className="flex flex-1 flex-col overflow-hidden">
      <div className="border-b border-white/10 px-4 py-3">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {t('canvas.transformations')}
        </h2>
        <p className="mt-0.5 text-[10px] text-muted-foreground/70">{t('canvas.dragToCanvas')}</p>
        <div className="relative mt-2">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={t('canvas.searchTransformations')}
            aria-label={t('canvas.searchTransformations')}
            className="h-7 w-full rounded-md border border-input bg-transparent pl-7 pr-2 text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
          />
        </div>
      </div>

      <div className="flex-1 overflow-auto p-3">
        {filtered.length === 0 && (
          <p className="px-1 py-4 text-center text-xs text-muted-foreground">
            {t('canvas.noConnectorsMatch', { query: query.trim() })}
          </p>
        )}
        <div className="flex flex-col gap-1.5">
          {filtered.map((kind) => (
            <div
              key={kind}
              draggable
              onDragStart={(e) => onDragStart(e, kind)}
              role="button"
              tabIndex={0}
              aria-label={t('canvas.connectorNode', { name: t(`canvas.clean.kind.${kind}`) })}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault()
                  // Keyboard-initiated drag start — see ConnectorPalette's
                  // identical handler for why this exists (B35).
                  const dataTransfer = new DataTransfer()
                  dataTransfer.setData(CLEAN_BLOCK_DRAG_TYPE, kind)
                  const event = new DragEvent('dragstart', { dataTransfer })
                  onDragStart(event as unknown as DragEvent<HTMLDivElement>, kind)
                }
              }}
              className="group flex cursor-grab items-center gap-2.5 rounded-lg border border-white/10 bg-white/[0.02] px-3 py-2 text-sm transition-colors hover:border-teal-400/30 hover:bg-teal-400/5 active:cursor-grabbing focus:outline-none focus:ring-2 focus:ring-teal-400/50"
            >
              <Wand2 className="h-3.5 w-3.5 text-muted-foreground group-hover:text-teal-400" />
              <span className="font-medium text-foreground">{t(`canvas.clean.kind.${kind}`)}</span>
            </div>
          ))}
        </div>
      </div>
    </aside>
  )
}
