import type { ReactNode } from 'react'
import { Handle, Position } from '@xyflow/react'
import type { LucideIcon } from 'lucide-react'
import { cn } from '@/lib/utils'

/** Shared visual shell for every canvas node (pipeline connectors, transform,
 * dbt, python, embedding and infra modules): dark glass card, colored icon
 * chip, thin accent line on top and glowing handles. One place for the look,
 * so the node types only pick an accent and say what to show.
 *
 * Tailwind only generates classes it can see as complete literals, so every
 * accent spells out its full class names here instead of building them from
 * the accent name. */

export type NodeAccent = 'primary' | 'accent' | 'emerald' | 'sky' | 'fuchsia'

const ACCENTS: Record<
  NodeAccent,
  {
    icon: string
    chip: string
    line: string
    hover: string
    selected: string
    handle: string
  }
> = {
  primary: {
    icon: 'text-primary',
    chip: 'bg-primary/15 ring-primary/30',
    line: 'via-primary/80',
    hover: 'hover:border-primary/40',
    selected: 'border-primary/70 ring-2 ring-primary/25 shadow-lg shadow-primary/20',
    handle: '!border-primary',
  },
  accent: {
    icon: 'text-accent',
    chip: 'bg-accent/15 ring-accent/30',
    line: 'via-accent/80',
    hover: 'hover:border-accent/40',
    selected: 'border-accent/70 ring-2 ring-accent/25 shadow-lg shadow-accent/20',
    handle: '!border-accent',
  },
  emerald: {
    icon: 'text-emerald-400',
    chip: 'bg-emerald-400/15 ring-emerald-400/30',
    line: 'via-emerald-400/80',
    hover: 'hover:border-emerald-400/40',
    selected: 'border-emerald-400/70 ring-2 ring-emerald-400/25 shadow-lg shadow-emerald-400/20',
    handle: '!border-emerald-400',
  },
  sky: {
    icon: 'text-sky-400',
    chip: 'bg-sky-400/15 ring-sky-400/30',
    line: 'via-sky-400/80',
    hover: 'hover:border-sky-400/40',
    selected: 'border-sky-400/70 ring-2 ring-sky-400/25 shadow-lg shadow-sky-400/20',
    handle: '!border-sky-400',
  },
  fuchsia: {
    icon: 'text-fuchsia-400',
    chip: 'bg-fuchsia-400/15 ring-fuchsia-400/30',
    line: 'via-fuchsia-400/80',
    hover: 'hover:border-fuchsia-400/40',
    selected: 'border-fuchsia-400/70 ring-2 ring-fuchsia-400/25 shadow-lg shadow-fuchsia-400/20',
    handle: '!border-fuchsia-400',
  },
}

interface NodeCardProps {
  accent: NodeAccent
  icon: LucideIcon
  title: ReactNode
  /** Small line under the title, next to `badge` when both are given. */
  subtitle?: ReactNode
  badge?: ReactNode
  selected?: boolean
  /** Which connection points to render. Defaults to both. */
  handles?: 'both' | 'target' | 'source'
  className?: string
}

export function NodeCard({
  accent,
  icon: Icon,
  title,
  subtitle,
  badge,
  selected,
  handles = 'both',
  className,
}: NodeCardProps) {
  const a = ACCENTS[accent]
  const handleClass = cn(
    '!h-3 !w-3 !rounded-full !border-2 !bg-background transition-transform',
    'group-hover:!scale-125',
    a.handle,
  )

  return (
    <div
      className={cn(
        'group relative min-w-[10rem] max-w-[16rem] overflow-hidden rounded-xl border bg-card',
        'bg-gradient-to-b from-white/[0.06] to-transparent transition-all duration-150',
        selected ? a.selected : cn('border-white/10 shadow-md shadow-black/30', a.hover),
        className,
      )}
    >
      <span
        aria-hidden
        className={cn(
          'pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent to-transparent',
          a.line,
        )}
      />
      {handles !== 'source' && (
        <Handle type="target" position={Position.Left} className={handleClass} />
      )}
      <div className="flex items-center gap-2.5 px-3 py-2.5">
        <div
          className={cn('grid h-8 w-8 shrink-0 place-items-center rounded-lg ring-1', a.chip)}
        >
          <Icon className={cn('h-4 w-4', a.icon)} />
        </div>
        <div className="min-w-0">
          <div className="truncate text-[13px] font-semibold leading-tight tracking-tight text-foreground">
            {title}
          </div>
          {(badge || subtitle) && (
            <div className="mt-1 flex items-center gap-1.5 text-[11px] leading-none text-muted-foreground">
              {badge}
              {subtitle && <span className="truncate">{subtitle}</span>}
            </div>
          )}
        </div>
      </div>
      {handles !== 'target' && (
        <Handle type="source" position={Position.Right} className={handleClass} />
      )}
    </div>
  )
}

/** Small pill used by node cards to mark a role (source/sink, ...). */
export function NodeBadge({
  tone,
  children,
}: {
  tone: 'emerald' | 'amber'
  children: ReactNode
}) {
  const styles =
    tone === 'emerald'
      ? { pill: 'bg-emerald-500/10 text-emerald-400 ring-emerald-500/25', dot: 'bg-emerald-400' }
      : { pill: 'bg-amber-500/10 text-amber-400 ring-amber-500/25', dot: 'bg-amber-400' }
  return (
    <span
      className={cn(
        'inline-flex shrink-0 items-center gap-1 rounded-full px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ring-1',
        styles.pill,
      )}
    >
      <span className={cn('h-1 w-1 rounded-full', styles.dot)} />
      {children}
    </span>
  )
}
