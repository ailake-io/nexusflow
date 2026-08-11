import { useState } from 'react'
import { Info } from 'lucide-react'
import { useI18n } from '@/lib/i18n'

/**
 * Small info button next to a form field's label — click to reveal the
 * field's explanation (from the connector's JSON Schema `description`,
 * itself sourced from a Rust doc comment on the Config struct field) in a
 * popover, instead of always showing the text and cluttering a form that
 * can have a dozen fields. Click again to hide it.
 */
export function FieldHint({ text }: { text: string }) {
  const { t } = useI18n()
  const [open, setOpen] = useState(false)

  return (
    <span className="relative inline-flex">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-label={t('common.whatGoesHere')}
        aria-expanded={open}
        className="flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground transition-colors hover:text-primary"
      >
        <Info className="h-3.5 w-3.5" />
      </button>
      {open && (
        <span
          role="tooltip"
          className="absolute left-0 top-5 z-50 w-56 rounded-lg border border-white/10 bg-popover p-2.5 text-xs leading-relaxed text-popover-foreground shadow-lg"
        >
          {text}
        </span>
      )}
    </span>
  )
}
