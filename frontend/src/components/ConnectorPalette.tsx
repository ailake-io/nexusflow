import type { DragEvent } from 'react'
import { Database, Loader2, AlertCircle, Lock } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import type { ConnectorDescriptor } from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'

interface ConnectorPaletteProps {
  connectors: ConnectorDescriptor[]
  loading: boolean
  error: string | null
}

function onDragStart(event: DragEvent<HTMLDivElement>, connectorName: string) {
  event.dataTransfer.setData('application/nexusflow-connector', connectorName)
  event.dataTransfer.effectAllowed = 'move'
}

/**
 * Node palette — drag a connector onto the canvas to add it as a node.
 * Populated from GET /connectors (useConnectors), never a hardcoded list
 * (ARCHITECTURE.md §3).
 */
export function ConnectorPalette({ connectors, loading, error }: ConnectorPaletteProps) {
  const { t } = useI18n()

  return (
    <aside className="flex w-60 shrink-0 flex-col border-r bg-card">
      <div className="border-b border-white/10 px-4 py-3">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {t('canvas.connectors')}
        </h2>
        <p className="mt-0.5 text-[10px] text-muted-foreground/70">{t('canvas.dragToCanvas')}</p>
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
        {!loading && connectors.length === 0 && (
          <EmptyState
            icon={<Database className="h-5 w-5" />}
            title={t('canvas.noConnectors')}
            description={t('canvas.noConnectorsDesc')}
            className="p-3"
          />
        )}
        <div className="flex flex-col gap-1.5">
          {connectors.map((connector) => (
            <div
              key={connector.name}
              draggable
              onDragStart={(e) => onDragStart(e, connector.name)}
              role="button"
              tabIndex={0}
              aria-label={t('canvas.connectorNode', { name: connector.name })}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault()
                  // Keyboard-initiated drag start — the canvas still needs a
                  // pointer drop, but this makes the item focusable and
                  // actionable for screen-reader/keyboard users (B35).
                  const dataTransfer = new DataTransfer()
                  dataTransfer.setData('application/nexusflow-connector', connector.name)
                  const event = new DragEvent('dragstart', { dataTransfer })
                  onDragStart(event as unknown as DragEvent<HTMLDivElement>, connector.name)
                }
              }}
              className="group flex cursor-grab items-center gap-2.5 rounded-lg border border-white/10 bg-white/[0.02] px-3 py-2 text-sm transition-colors hover:border-primary/30 hover:bg-primary/5 active:cursor-grabbing focus:outline-none focus:ring-2 focus:ring-primary/50"
            >
              <Database className="h-3.5 w-3.5 text-muted-foreground group-hover:text-primary" />
              <span className="font-medium text-foreground">{connector.name}</span>
              {connector.requires_license && !connector.licensed && (
                <Lock
                  className="ml-auto h-3 w-3 shrink-0 text-amber-400"
                  aria-label={t('canvas.connectorLocked')}
                />
              )}
            </div>
          ))}
        </div>
      </div>
    </aside>
  )
}
