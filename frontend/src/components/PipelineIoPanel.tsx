import { useState } from 'react'
import {
  Save,
  Download,
  Upload,
  Play,
  Loader2,
  CheckCircle2,
  AlertCircle,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { FieldHint } from '@/components/FieldHint'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import type { PipelineMeta } from '@/lib/dag'

interface PipelineIoPanelProps {
  meta: PipelineMeta
  onMetaChange: (meta: PipelineMeta) => void
  onExport: () => string
  onImport: (json: string) => void
  onRun: () => void
  onSave: () => Promise<void>
  running: boolean
  saving: boolean
}

/**
 * Toolbar strip for the DAG <-> PipelineSpec JSON round-trip (task #15):
 * pipeline-level fields (pipeline_id/channel_capacity/partitions) plus a
 * text area that either shows the exported JSON or accepts pasted JSON to
 * load onto the canvas. Save persists the current canvas to the server
 * (POST/PUT /pipelines) — without it the pipeline only ever exists in this
 * browser tab and never shows up in the Pipelines list or the scheduler.
 */
export function PipelineIoPanel({
  meta,
  onMetaChange,
  onExport,
  onImport,
  onRun,
  onSave,
  running,
  saving,
}: PipelineIoPanelProps) {
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

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

  const handleSave = async () => {
    setError(null)
    setSaved(false)
    try {
      await onSave()
      setSaved(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="flex flex-col gap-3 border-b border-white/10 bg-card p-4">
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <Label htmlFor="pipeline-id" className="text-xs font-medium">
            pipeline_id
          </Label>
          <Input
            id="pipeline-id"
            value={meta.pipelineId}
            onChange={(e) => onMetaChange({ ...meta, pipelineId: e.target.value })}
            placeholder="my-pipeline"
            className="mt-1.5 w-56"
          />
        </div>
        <div>
          <Label htmlFor="partitions" className="text-xs font-medium">
            partitions
          </Label>
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
            className="mt-1.5 w-28"
          />
        </div>
        <div>
          <Label htmlFor="channel-capacity" className="text-xs font-medium">
            channel_capacity
          </Label>
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
            className="mt-1.5 w-28"
          />
        </div>
        <div>
          <div className="flex items-center gap-1.5">
            <Label htmlFor="schedule" className="text-xs font-medium">
              schedule (cron)
            </Label>
            <FieldHint
              text={
                'Optional. Unix cron with 5 fields (minute hour day-of-month month ' +
                'day-of-week), e.g. "0 */6 * * *" runs every 6 hours, ' +
                '"*/15 * * * *" every 15 minutes, "0 3 * * *" daily at 3 AM. ' +
                'Leave blank to run manually only (Run button or API).'
              }
            />
          </div>
          <Input
            id="schedule"
            value={meta.schedule ?? ''}
            placeholder="e.g. 0 */6 * * *"
            onChange={(e) => onMetaChange({ ...meta, schedule: e.target.value })}
            className="mt-1.5 w-52"
          />
        </div>

        <div className="ml-auto flex items-center gap-2">
          <Button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="gap-1.5"
          >
            {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
            {saving ? 'Saving…' : 'Save'}
          </Button>
          <Button type="button" variant="outline" onClick={handleExport} className="gap-1.5">
            <Download className="h-3.5 w-3.5" />
            Export JSON
          </Button>
          <Button type="button" variant="outline" onClick={handleImport} className="gap-1.5">
            <Upload className="h-3.5 w-3.5" />
            Load JSON
          </Button>
          <Button
            type="button"
            variant="secondary"
            onClick={onRun}
            disabled={running}
            className="gap-1.5 bg-primary text-primary-foreground hover:bg-primary/90"
          >
            {running ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
            {running ? 'Running…' : 'Run'}
          </Button>
        </div>
      </div>

      {(error || saved) && (
        <div className="flex flex-wrap items-center gap-2">
          {error && (
            <div className="flex items-center gap-2 rounded-md border border-red-500/20 bg-red-500/10 px-3 py-1.5 text-xs text-red-400">
              <AlertCircle className="h-3.5 w-3.5" />
              {error}
            </div>
          )}
          {saved && !error && (
            <div className="flex items-center gap-2 rounded-md border border-emerald-500/20 bg-emerald-500/10 px-3 py-1.5 text-xs text-emerald-400">
              <CheckCircle2 className="h-3.5 w-3.5" />
              Pipeline saved.
            </div>
          )}
        </div>
      )}

      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        rows={5}
        spellCheck={false}
        placeholder="PipelineSpec JSON — Export fills this in, Load reads from it"
        className="w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
      />
    </div>
  )
}
