import type { ReactNode } from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

const statusBadgeVariants = cva(
  'inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs font-medium transition-colors',
  {
    variants: {
      variant: {
        success:
          'border-emerald-500/30 bg-emerald-500/10 text-emerald-400',
        failed:
          'border-red-500/30 bg-red-500/10 text-red-400',
        running:
          'border-cyan-500/30 bg-cyan-500/10 text-cyan-400',
        idle:
          'border-white/10 bg-white/5 text-muted-foreground',
        warning:
          'border-amber-500/30 bg-amber-500/10 text-amber-400',
      },
    },
    defaultVariants: {
      variant: 'idle',
    },
  },
)

interface StatusBadgeProps
  extends VariantProps<typeof statusBadgeVariants> {
  children: ReactNode
  className?: string
  pulse?: boolean
}

export function StatusBadge({
  variant,
  children,
  className,
  pulse = false,
}: StatusBadgeProps) {
  return (
    <span className={cn(statusBadgeVariants({ variant }), className)}>
      <span
        className={cn(
          'h-1.5 w-1.5 rounded-full',
          variant === 'success' && 'bg-emerald-400',
          variant === 'failed' && 'bg-red-400',
          variant === 'running' && 'bg-cyan-400',
          variant === 'warning' && 'bg-amber-400',
          variant === 'idle' && 'bg-muted-foreground/60',
          pulse && 'animate-pulse-soft',
        )}
      />
      {children}
    </span>
  )
}
