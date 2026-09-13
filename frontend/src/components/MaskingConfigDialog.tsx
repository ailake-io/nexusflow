import { useI18n } from '@/lib/i18n'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

interface MaskingConfigDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  maskedColumns?: string[]
  onChange: (maskedColumns: string[] | undefined) => void
}

/**
 * Deterministic column tokenization configuration (Fase 28) —
 * `PipelineSpec.masking`. Same controlled, writes-straight-through pattern
 * as `AlertsConfigDialog`/`QualityChecksDialog`: a plain comma-separated
 * column-name list, same input shape `QualityChecksDialog`'s
 * `accepted_values` field already uses for a string list, since masking
 * doesn't need per-column extra fields the way a quality check's
 * min/max does — every masked column is tokenized the same way
 * (deterministic HMAC-SHA256, see `nexus-core::column_masking`'s doc
 * comment for why this is one-way tokenization, not reversible
 * encryption).
 */
export function MaskingConfigDialog({
  open,
  onOpenChange,
  maskedColumns,
  onChange,
}: MaskingConfigDialogProps) {
  const { t } = useI18n()
  const value = (maskedColumns ?? []).join(', ')

  const handleChange = (raw: string) => {
    const columns = raw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
    onChange(columns.length > 0 ? columns : undefined)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{t('masking.title')}</DialogTitle>
          <p className="text-sm text-muted-foreground">{t('masking.subtitle')}</p>
        </DialogHeader>

        <div className="flex flex-col gap-3">
          <div>
            <Label className="text-xs">{t('masking.columns')}</Label>
            <Input
              value={value}
              placeholder={t('masking.columnsPlaceholder')}
              onChange={(e) => handleChange(e.target.value)}
              className="mt-1"
            />
          </div>
          <p className="text-[11px] text-muted-foreground">{t('masking.requiresSaltHint')}</p>
        </div>
      </DialogContent>
    </Dialog>
  )
}

export default MaskingConfigDialog
