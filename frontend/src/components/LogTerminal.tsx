import { useEffect, useRef } from 'react'
import { useI18n } from '@/lib/i18n'

export interface LogLine {
  ts: string
  level: 'info' | 'warn' | 'error'
  message: string
}

interface LogTerminalProps {
  logs: LogLine[]
  autoScroll?: boolean
}

const LEVEL_CLASSES: Record<LogLine['level'], string> = {
  info: 'text-muted-foreground',
  warn: 'text-amber-400',
  error: 'text-red-400',
}

/**
 * Monospace, terminal-style log list shared by `ExecutionPanel` (live runs,
 * fed by the progress WebSocket's `type: 'log'` frames) and
 * `RunHistoryPanel` (past runs, fed by `GET .../logs` via `useRunLogs`) —
 * same visual, two different data sources, see `ARCHITECTURE.md` on
 * `RunLogStore`/`RunLogger`.
 */
export function LogTerminal({ logs, autoScroll = true }: LogTerminalProps) {
  const { t } = useI18n()
  const bottomRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (autoScroll) bottomRef.current?.scrollIntoView({ block: 'end' })
  }, [logs.length, autoScroll])

  if (logs.length === 0) {
    return <div className="p-3 text-xs text-muted-foreground">{t('execution.logs.empty')}</div>
  }

  return (
    <div className="max-h-96 overflow-y-auto rounded-lg border border-white/10 bg-black/30 p-2 font-mono text-xs">
      {logs.map((line, i) => (
        <div key={i} className="flex gap-2 py-0.5">
          <span className="shrink-0 text-muted-foreground/60">
            {new Date(line.ts).toLocaleTimeString()}
          </span>
          <span className={`shrink-0 uppercase ${LEVEL_CLASSES[line.level]}`}>{line.level}</span>
          <span className="whitespace-pre-wrap break-all text-foreground/90">{line.message}</span>
        </div>
      ))}
      <div ref={bottomRef} />
    </div>
  )
}
