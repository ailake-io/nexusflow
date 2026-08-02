import { useState } from 'react'

/**
 * Small "ⓘ" button next to a form field's label — click to reveal the
 * field's explanation (from the connector's JSON Schema `description`,
 * itself sourced from a Rust doc comment on the Config struct field) in a
 * popover, instead of always showing the text and cluttering a form that
 * can have a dozen fields. Click again to hide it.
 */
export function FieldHint({ text }: { text: string }) {
  const [open, setOpen] = useState(false)

  return (
    <span className="relative inline-flex">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-label="O que colocar neste campo?"
        aria-expanded={open}
        className="flex h-4 w-4 items-center justify-center rounded-full border border-muted-foreground/50 text-[10px] leading-none text-muted-foreground hover:border-foreground hover:text-foreground"
      >
        i
      </button>
      {open && (
        <span
          role="tooltip"
          className="absolute left-0 top-5 z-50 w-56 rounded-md border bg-popover p-2 text-xs leading-snug text-popover-foreground shadow-md"
        >
          {text}
        </span>
      )}
    </span>
  )
}
