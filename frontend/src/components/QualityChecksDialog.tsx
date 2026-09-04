import { Plus, Trash2 } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import type { QualityCheckKind, QualityCheckSpec } from '@/lib/dag'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

interface QualityChecksDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  checks?: QualityCheckSpec[]
  onChange: (checks: QualityCheckSpec[] | undefined) => void
}

const KIND_OPTIONS: QualityCheckKind['kind'][] = [
  'not_null',
  'unique',
  'min',
  'max',
  'accepted_values',
]

function defaultKind(kind: QualityCheckKind['kind']): QualityCheckKind {
  switch (kind) {
    case 'min':
      return { kind: 'min', min: 0 }
    case 'max':
      return { kind: 'max', max: 0 }
    case 'accepted_values':
      return { kind: 'accepted_values', values: [] }
    default:
      return { kind }
  }
}

/**
 * Per-pipeline native quality check configuration — `PipelineSpec.quality_checks`
 * (nexus_core::quality). Only takes effect on a pipeline with a Transform
 * node (evaluated against the fully materialized output); a check never
 * blocks a run, it's only recorded (see `QualityPanel`'s "native checks"
 * section for the results). Same controlled-list pattern as
 * `AlertsConfigDialog`, but simpler — one flat list, not per-channel cards.
 */
export function QualityChecksDialog({
  open,
  onOpenChange,
  checks,
  onChange,
}: QualityChecksDialogProps) {
  const { t } = useI18n()
  const list = checks ?? []

  const update = (next: QualityCheckSpec[]) => {
    onChange(next.length === 0 ? undefined : next)
  }

  const addCheck = () => {
    update([...list, { column: '', check: { kind: 'not_null' } }])
  }

  const removeCheck = (index: number) => {
    update(list.filter((_, i) => i !== index))
  }

  const updateCheck = (index: number, patch: Partial<QualityCheckSpec>) => {
    update(list.map((c, i) => (i === index ? { ...c, ...patch } : c)))
  }

  const updateKind = (index: number, kind: QualityCheckKind['kind']) => {
    updateCheck(index, { check: defaultKind(kind) })
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{t('quality.checksDialogTitle')}</DialogTitle>
          <p className="text-sm text-muted-foreground">{t('quality.checksDialogSubtitle')}</p>
        </DialogHeader>

        <div className="flex flex-col gap-3">
          {list.length === 0 && (
            <p className="text-xs text-muted-foreground">{t('quality.checksDialogEmpty')}</p>
          )}
          {list.map((c, i) => (
            <fieldset key={i} className="rounded-lg border border-white/10 p-3">
              <div className="flex items-start gap-2">
                <div className="flex-1">
                  <Label className="text-xs">{t('quality.checksDialogColumn')}</Label>
                  <Input
                    value={c.column}
                    placeholder="id"
                    onChange={(e) => updateCheck(i, { column: e.target.value })}
                    className="mt-1"
                  />
                </div>
                <div className="flex-1">
                  <Label className="text-xs">{t('quality.checksDialogKind')}</Label>
                  <select
                    value={c.check.kind}
                    onChange={(e) => updateKind(i, e.target.value as QualityCheckKind['kind'])}
                    className="mt-1 w-full rounded-md border border-white/10 bg-background px-3 py-1.5 text-xs text-foreground focus:border-primary/40 focus:outline-none"
                  >
                    {KIND_OPTIONS.map((kind) => (
                      <option key={kind} value={kind}>
                        {t(`quality.checkKind.${kind}`)}
                      </option>
                    ))}
                  </select>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  onClick={() => removeCheck(i)}
                  className="mt-5"
                  aria-label={t('quality.checksDialogRemove')}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </div>

              {c.check.kind === 'min' && (
                <div className="mt-2">
                  <Label className="text-xs">{t('quality.checksDialogMin')}</Label>
                  <Input
                    type="number"
                    value={c.check.min}
                    onChange={(e) =>
                      updateCheck(i, { check: { kind: 'min', min: Number(e.target.value) || 0 } })
                    }
                    className="mt-1"
                  />
                </div>
              )}
              {c.check.kind === 'max' && (
                <div className="mt-2">
                  <Label className="text-xs">{t('quality.checksDialogMax')}</Label>
                  <Input
                    type="number"
                    value={c.check.max}
                    onChange={(e) =>
                      updateCheck(i, { check: { kind: 'max', max: Number(e.target.value) || 0 } })
                    }
                    className="mt-1"
                  />
                </div>
              )}
              {c.check.kind === 'accepted_values' && (
                <div className="mt-2">
                  <Label className="text-xs">{t('quality.checksDialogAcceptedValues')}</Label>
                  <Input
                    value={c.check.values.join(', ')}
                    placeholder="active, inactive"
                    onChange={(e) =>
                      updateCheck(i, {
                        check: {
                          kind: 'accepted_values',
                          values: e.target.value
                            .split(',')
                            .map((s) => s.trim())
                            .filter(Boolean),
                        },
                      })
                    }
                    className="mt-1"
                  />
                </div>
              )}
            </fieldset>
          ))}

          <Button type="button" variant="outline" onClick={addCheck} className="gap-1.5 self-start">
            <Plus className="h-3.5 w-3.5" />
            {t('quality.checksDialogAdd')}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
