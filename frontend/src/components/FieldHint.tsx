import { useEffect, useId, useRef, useState } from 'react'
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
  const tooltipId = useId()
  const buttonRef = useRef<HTMLButtonElement>(null)
  const tooltipRef = useRef<HTMLSpanElement>(null)

  useEffect(() => {
    if (!open) return

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') {
        setOpen(false)
        buttonRef.current?.focus()
      }
    }

    function onClickOutside(e: MouseEvent) {
      const target = e.target as Node
      if (
        !buttonRef.current?.contains(target) &&
        !tooltipRef.current?.contains(target)
      ) {
        setOpen(false)
      }
    }

    document.addEventListener('keydown', onKeyDown)
    document.addEventListener('mousedown', onClickOutside)
    return () => {
      document.removeEventListener('keydown', onKeyDown)
      document.removeEventListener('mousedown', onClickOutside)
    }
  }, [open])

  return (
    <span className="relative inline-flex">
      <button
        ref={buttonRef}
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-label={t('common.whatGoesHere')}
        aria-expanded={open}
        aria-describedby={open ? tooltipId : undefined}
        className="flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground transition-colors hover:text-primary"
      >
        <Info className="h-3.5 w-3.5" />
      </button>
      {open && (
        <span
          ref={tooltipRef}
          id={tooltipId}
          role="tooltip"
          className="absolute left-0 top-5 z-50 w-56 rounded-lg border border-white/10 bg-popover p-2.5 text-xs leading-relaxed text-popover-foreground shadow-lg"
        >
          {text}
        </span>
      )}
    </span>
  )
}
