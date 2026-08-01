import type { ExecutionStatus, PartitionProgress } from '@/hooks/useRunProgress'

interface ExecutionPanelProps {
  status: ExecutionStatus
  runId: number | null
  partitions: Record<string, PartitionProgress>
  error: string | null
}

const STATUS_LABEL: Record<ExecutionStatus, string> = {
  idle: 'Idle',
  starting: 'Starting…',
  running: 'Running',
  success: 'Success',
  failed: 'Failed',
}

const STATUS_CLASS: Record<ExecutionStatus, string> = {
  idle: 'text-muted-foreground',
  starting: 'text-muted-foreground',
  running: 'text-primary',
  success: 'text-emerald-600 dark:text-emerald-400',
  failed: 'text-destructive',
}

/** Live execution panel (Marco 8 task #16) — consumes the progress
 * WebSocket from Marco 7 (task #9): rows/s and MB/s per partition/sink,
 * computed client-side from the cumulative counters each event carries. */
export function ExecutionPanel({ status, runId, partitions, error }: ExecutionPanelProps) {
  if (status === 'idle') return null

  const rows = Object.values(partitions)

  return (
    <div className="border-t bg-card p-3">
      <div className="mb-2 flex items-center gap-2 text-sm">
        <span className={`font-medium ${STATUS_CLASS[status]}`}>{STATUS_LABEL[status]}</span>
        {runId !== null && <span className="text-muted-foreground">run #{runId}</span>}
      </div>
      {rows.length > 0 && (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-left text-muted-foreground">
              <th className="pr-4 font-normal">Partition</th>
              <th className="pr-4 font-normal">Rows</th>
              <th className="pr-4 font-normal">Rows/s</th>
              <th className="pr-4 font-normal">MB</th>
              <th className="font-normal">MB/s</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => (
              <tr key={p.partition_id}>
                <td className="pr-4">{p.partition_id}</td>
                <td className="pr-4">{p.rows_written.toLocaleString()}</td>
                <td className="pr-4">{p.rowsPerSecond.toFixed(0)}</td>
                <td className="pr-4">{(p.bytes_written / 1_000_000).toFixed(2)}</td>
                <td>{p.mbPerSecond.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {error && <p className="mt-2 text-xs text-destructive">{error}</p>}
    </div>
  )
}
