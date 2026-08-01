import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import type { PipelineMeta } from '@/lib/dag'

interface PipelineIoPanelProps {
  meta: PipelineMeta
  onMetaChange: (meta: PipelineMeta) => void
  onExport: () => string
  onImport: (json: string) => void
  onRun: () => void
  running: boolean
}

/**
 * Toolbar strip for the DAG <-> PipelineSpec JSON round-trip (task #15):
 * pipeline-level fields (pipeline_id/channel_capacity/partitions) plus a
 * text area that either shows the exported JSON or accepts pasted JSON to
 * load onto the canvas.
 */
export function PipelineIoPanel({
  meta,
  onMetaChange,
  onExport,
  onImport,
  onRun,
  running,
}: PipelineIoPanelProps) {
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)

  const handleExport = () => {
    try {
      setText(onExport())
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const handleImport = () => {
    try {
      onImport(text)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="flex flex-col gap-2 border-b bg-card p-3">
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <Label htmlFor="pipeline-id">pipeline_id</Label>
          <Input
            id="pipeline-id"
            value={meta.pipelineId}
            onChange={(e) => onMetaChange({ ...meta, pipelineId: e.target.value })}
            className="mt-1 w-48"
          />
        </div>
        <div>
          <Label htmlFor="partitions">partitions</Label>
          <Input
            id="partitions"
            type="number"
            min={1}
            value={meta.partitions ?? ''}
            placeholder="1"
            onChange={(e) =>
              onMetaChange({
                ...meta,
                partitions: e.target.value ? Number(e.target.value) : undefined,
              })
            }
            className="mt-1 w-24"
          />
        </div>
        <div>
          <Label htmlFor="channel-capacity">channel_capacity</Label>
          <Input
            id="channel-capacity"
            type="number"
            min={1}
            value={meta.channelCapacity ?? ''}
            placeholder="100"
            onChange={(e) =>
              onMetaChange({
                ...meta,
                channelCapacity: e.target.value ? Number(e.target.value) : undefined,
              })
            }
            className="mt-1 w-24"
          />
        </div>
        <Button type="button" onClick={handleExport}>
          Export JSON
        </Button>
        <Button type="button" variant="outline" onClick={handleImport}>
          Load JSON
        </Button>
        <Button type="button" variant="secondary" onClick={onRun} disabled={running}>
          {running ? 'Running…' : 'Run'}
        </Button>
      </div>
      {error && <p className="text-sm text-destructive">{error}</p>}
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        rows={6}
        spellCheck={false}
        placeholder="PipelineSpec JSON — Export fills this in, Load reads from it"
        className="w-full rounded-lg border border-input bg-transparent p-2 font-mono text-xs"
      />
    </div>
  )
}
