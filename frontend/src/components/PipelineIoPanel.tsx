import { useRef, useState } from 'react'
import { parse as parseYaml } from 'yaml'
import {
  Save,
  Download,
  Upload,
  FileUp,
  Play,
  Loader2,
  CheckCircle2,
  AlertCircle,
  FilePlus,
  Timer,
  Bell,
} from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import { Button } from '@/components/ui/button'
import { FieldHint } from '@/components/FieldHint'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { AlertsConfigDialog } from '@/components/AlertsConfigDialog'
import type { PipelineMeta } from '@/lib/dag'

interface PipelineIoPanelProps {
  meta: PipelineMeta
  onMetaChange: (meta: PipelineMeta) => void
  onExport: () => string
  onImport: (json: string) => void
  onRun: () => void
  onSave: () => Promise<void>
  onNewPipeline: () => void
  running: boolean
  saving: boolean
  autoSaveEnabled: boolean
  onToggleAutoSave: () => void
  autoSaveStatus: string | null
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
  onNewPipeline,
  running,
  saving,
  autoSaveEnabled,
  onToggleAutoSave,
  autoSaveStatus,
}: PipelineIoPanelProps) {
  const { t } = useI18n()
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)
  const [alertsOpen, setAlertsOpen] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleExport = () => {
    try {
      setText(onExport())
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  // Shared by both the pasted-JSON textarea and the file upload below —
  // `parsed` already went through JSON.parse or YAML's parser (a superset
  // of JSON, so both paths land here the same way).
  const validateAndImport = (parsed: unknown, raw: string) => {
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error(t('ioPanel.importNotObject'))
    }
    const obj = parsed as Record<string, unknown>
    if (typeof obj.pipeline_id !== 'string' || !obj.pipeline_id.trim()) {
      throw new Error(t('ioPanel.importMissingPipelineId'))
    }
    if (!Array.isArray(obj.sources) || !Array.isArray(obj.sinks)) {
      throw new Error(t('ioPanel.importMissingSourcesSinks'))
    }
    onImport(raw)
  }

  const handleImport = () => {
    try {
      validateAndImport(JSON.parse(text), text)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    // Always reset, even on failure — otherwise re-selecting the exact same
    // filename after an error wouldn't fire this handler a second time.
    e.target.value = ''
    if (!file) return
    try {
      const raw = await file.text()
      const parsed: unknown = parseYaml(raw)
      setText(JSON.stringify(parsed, null, 2))
      validateAndImport(parsed, JSON.stringify(parsed))
      setError(null)
    } catch (err) {
      setError(t('ioPanel.loadFileError', { msg: err instanceof Error ? err.message : String(err) }))
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
            {t('ioPanel.pipelineId')}
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
            {t('ioPanel.partitions')}
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
            {t('ioPanel.channelCapacity')}
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
              {t('ioPanel.schedule')}
            </Label>
            <FieldHint text={t('ioPanel.scheduleHint')} />
          </div>
          <Input
            id="schedule"
            value={meta.schedule ?? ''}
            placeholder={t('ioPanel.schedulePlaceholder')}
            onChange={(e) => onMetaChange({ ...meta, schedule: e.target.value })}
            className="mt-1.5 w-52"
          />
        </div>

        <div className="ml-auto flex items-center gap-2">
          <Button type="button" variant="outline" onClick={onNewPipeline} className="gap-1.5">
            <FilePlus className="h-3.5 w-3.5" />
            {t('ioPanel.newPipeline')}
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => setAlertsOpen(true)}
            className="gap-1.5"
          >
            <Bell className="h-3.5 w-3.5" />
            {t('ioPanel.alerts')}
          </Button>
          <Button
            type="button"
            variant={autoSaveEnabled ? 'default' : 'outline'}
            onClick={onToggleAutoSave}
            disabled={!meta.pipelineId.trim()}
            title={!meta.pipelineId.trim() ? t('ioPanel.autoSaveNeedsId') : ''}
            className="gap-1.5"
          >
            <Timer className="h-3.5 w-3.5" />
            {autoSaveEnabled ? t('ioPanel.autoSaveOn') : t('ioPanel.autoSave')}
          </Button>
          <Button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="gap-1.5"
          >
            {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
            {saving ? t('ioPanel.saving') : t('ioPanel.save')}
          </Button>
          <Button type="button" variant="outline" onClick={handleExport} className="gap-1.5">
            <Download className="h-3.5 w-3.5" />
            {t('ioPanel.exportJson')}
          </Button>
          <Button type="button" variant="outline" onClick={handleImport} className="gap-1.5">
            <Upload className="h-3.5 w-3.5" />
            {t('ioPanel.loadJson')}
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => fileInputRef.current?.click()}
            className="gap-1.5"
          >
            <FileUp className="h-3.5 w-3.5" />
            {t('ioPanel.loadFile')}
          </Button>
          <input
            ref={fileInputRef}
            type="file"
            accept=".json,.yaml,.yml,application/json,text/yaml"
            className="hidden"
            onChange={handleFileUpload}
          />
          <Button
            type="button"
            variant="secondary"
            onClick={onRun}
            disabled={running}
            className="gap-1.5 bg-primary text-primary-foreground hover:bg-primary/90"
          >
            {running ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
            {running ? t('ioPanel.running') : t('ioPanel.run')}
          </Button>
        </div>
      </div>

      {(error || saved || autoSaveStatus) && (
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
              {t('ioPanel.saved')}
            </div>
          )}
          {autoSaveStatus && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Timer className="h-3.5 w-3.5" />
              {autoSaveStatus}
            </div>
          )}
        </div>
      )}

      <textarea
        aria-label={t('ioPanel.placeholder')}
        value={text}
        onChange={(e) => setText(e.target.value)}
        rows={5}
        spellCheck={false}
        placeholder={t('ioPanel.placeholder')}
        className="w-full rounded-lg border border-input bg-transparent p-3 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-ring"
      />

      <AlertsConfigDialog
        open={alertsOpen}
        onOpenChange={setAlertsOpen}
        alerts={meta.alerts}
        onChange={(alerts) => onMetaChange({ ...meta, alerts })}
      />
    </div>
  )
}
