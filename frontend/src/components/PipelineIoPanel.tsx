import { useState } from 'react'
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
        <div>
          <div className="flex items-center gap-1.5">
            <Label htmlFor="schedule">schedule (cron)</Label>
            <FieldHint
              text={
                'Opcional. Formato Unix de 5 campos (minuto hora dia-do-mês mês ' +
                'dia-da-semana), ex.: "0 */6 * * *" roda a cada 6 horas, ' +
                '"*/15 * * * *" a cada 15 minutos, "0 3 * * *" toda madrugada às 3h. ' +
                'Deixe em branco pra rodar só manualmente (botão Run ou API).'
              }
            />
          </div>
          <Input
            id="schedule"
            value={meta.schedule ?? ''}
            placeholder="ex.: 0 */6 * * *"
            onChange={(e) => onMetaChange({ ...meta, schedule: e.target.value })}
            className="mt-1 w-48"
          />
        </div>
        <Button type="button" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </Button>
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
      {saved && !error && <p className="text-sm text-muted-foreground">Pipeline salvo.</p>}
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
