import type { DragEvent } from 'react'
import type { ConnectorDescriptor } from '@/lib/api'

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
  return (
    <aside className="w-56 shrink-0 border-r bg-card p-3">
      <h2 className="mb-3 text-sm font-medium text-muted-foreground">Connectors</h2>
      {loading && <p className="text-sm text-muted-foreground">Loading…</p>}
      {error && <p className="text-sm text-destructive">{error}</p>}
      <div className="flex flex-col gap-2">
        {connectors.map((connector) => (
          <div
            key={connector.name}
            draggable
            onDragStart={(e) => onDragStart(e, connector.name)}
            className="cursor-grab rounded-md border bg-background px-3 py-2 text-sm active:cursor-grabbing"
          >
            {connector.name}
          </div>
        ))}
      </div>
    </aside>
  )
}
