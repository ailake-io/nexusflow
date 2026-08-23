import { useCallback, useEffect, useRef, useState } from 'react'
import { AlertCircle, Cpu, Gauge, HardDrive, Loader2, MemoryStick } from 'lucide-react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { ValueType } from 'recharts/types/component/DefaultTooltipContent'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { getResourceStats, type ResourceStatsBucket } from '@/lib/api'

/** Tab always opens on this range — not one of the 5 preset shortcuts
 *  below, matching what was asked: "sempre iniciando com 5 minutos". */
const DEFAULT_RANGE = '5m'
const PRESETS = ['1h', '6h', '1d', '7d', '30d'] as const

const POLL_INTERVAL_MS = 30_000

function formatBytes(bytes: number): string {
  const gb = bytes / 1_000_000_000
  if (gb >= 1) return `${gb.toFixed(1)} GB`
  return `${(bytes / 1_000_000).toFixed(0)} MB`
}

function formatBucketTime(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

interface StatCardProps {
  icon: typeof Cpu
  label: string
  current: string
  children: React.ReactNode
}

function StatCard({ icon: Icon, label, current, children }: StatCardProps) {
  return (
    <div className="rounded-lg border border-white/10 bg-card p-4">
      <div className="mb-1 flex items-center justify-between">
        <span className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
          <Icon className="h-3.5 w-3.5" />
          {label}
        </span>
        <span className="text-sm font-semibold text-foreground">{current}</span>
      </div>
      <div className="h-40 w-full">{children}</div>
    </div>
  )
}

/**
 * "Resources" tab: CPU/memory/disk history for the machine running the
 * NexusFlow backend — independent of any pipeline run (unlike the
 * per-run `hardware_stats` frame ExecutionPanel shows). Backed by
 * GET /system/resource-stats, which is continuously sampled server-side
 * (nexus-server::resource_stats::spawn) regardless of whether this tab is
 * ever open.
 */
export function ResourcesPanel() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [range, setRange] = useState(DEFAULT_RANGE)
  const [customInput, setCustomInput] = useState('')
  const [buckets, setBuckets] = useState<ResourceStatsBucket[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const rangeRef = useRef(range)
  rangeRef.current = range

  const refresh = useCallback(async () => {
    if (!token) return
    try {
      const data = await getResourceStats(token, rangeRef.current)
      setBuckets(data)
      setError(null)
    } catch {
      setError(t('resources.error'))
    } finally {
      setLoading(false)
    }
  }, [token, t])

  useEffect(() => {
    setLoading(true)
    refresh()
  }, [range, refresh])

  useEffect(() => {
    let id: ReturnType<typeof setInterval> | null = null

    function start() {
      id = setInterval(refresh, POLL_INTERVAL_MS)
    }

    function stop() {
      if (id !== null) {
        clearInterval(id)
        id = null
      }
    }

    function handleVisibility() {
      if (document.hidden) {
        stop()
      } else {
        start()
      }
    }

    start()
    document.addEventListener('visibilitychange', handleVisibility)
    return () => {
      stop()
      document.removeEventListener('visibilitychange', handleVisibility)
    }
  }, [refresh])

  const applyCustomRange = () => {
    const value = customInput.trim()
    if (!value) return
    if (!/^\d+[mhd]$/.test(value)) {
      setError(t('resources.invalidRange'))
      return
    }
    setRange(value)
  }

  const latest = buckets.length > 0 ? buckets[buckets.length - 1] : null
  const chartData = buckets.map((b) => ({
    ...b,
    time: formatBucketTime(b.bucket_start),
    memory_used_gb: b.memory_used_bytes / 1_000_000_000,
    memory_total_gb: b.memory_total_bytes / 1_000_000_000,
    disk_used_gb: b.disk_used_bytes !== null ? b.disk_used_bytes / 1_000_000_000 : null,
    disk_total_gb: b.disk_total_bytes !== null ? b.disk_total_bytes / 1_000_000_000 : null,
  }))
  const hasDiskData = buckets.some((b) => b.disk_used_bytes !== null)

  return (
    <div className="h-full overflow-auto p-6">
      <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">{t('resources.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('resources.subtitle')}</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {PRESETS.map((preset) => (
            <button
              key={preset}
              type="button"
              onClick={() => {
                setCustomInput('')
                setRange(preset)
              }}
              className={`rounded-md border px-2.5 py-1 text-xs font-medium transition-colors ${
                range === preset && customInput === ''
                  ? 'border-primary/40 bg-primary/10 text-primary'
                  : 'border-white/10 text-muted-foreground hover:bg-white/5'
              }`}
            >
              {preset}
            </button>
          ))}
          <input
            type="text"
            value={customInput}
            onChange={(e) => setCustomInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && applyCustomRange()}
            placeholder={t('resources.customPlaceholder')}
            className="w-28 rounded-md border border-white/10 bg-background px-2 py-1 text-xs text-foreground placeholder:text-muted-foreground/60 focus:border-primary/40 focus:outline-none"
          />
          <button
            type="button"
            onClick={applyCustomRange}
            className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-medium text-muted-foreground hover:bg-white/5"
          >
            {t('resources.apply')}
          </button>
        </div>
      </div>

      {loading && buckets.length === 0 && (
        <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          {t('resources.loading')}
        </div>
      )}

      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-xs text-red-400">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
      )}

      {buckets.length > 0 && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <StatCard
            icon={Cpu}
            label={t('resources.cpu')}
            current={latest ? `${latest.cpu_percent.toFixed(0)}%` : '—'}
          >
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis dataKey="time" tick={{ fontSize: 10 }} minTickGap={30} />
                <YAxis tick={{ fontSize: 10 }} domain={[0, 100]} unit="%" />
                <Tooltip
                  contentStyle={{ fontSize: 12, background: '#111', border: '1px solid #333' }}
                  formatter={(value: ValueType | undefined) => [`${Number(Array.isArray(value) ? value[0] : value).toFixed(1)}%`, t('resources.cpu')]}
                />
                <Area
                  type="monotone"
                  dataKey="cpu_percent"
                  stroke="#38bdf8"
                  fill="#38bdf8"
                  fillOpacity={0.2}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </StatCard>

          <StatCard
            icon={MemoryStick}
            label={t('resources.memory')}
            current={
              latest ? `${formatBytes(latest.memory_used_bytes)} / ${formatBytes(latest.memory_total_bytes)}` : '—'
            }
          >
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis dataKey="time" tick={{ fontSize: 10 }} minTickGap={30} />
                <YAxis tick={{ fontSize: 10 }} unit=" GB" />
                <Tooltip
                  contentStyle={{ fontSize: 12, background: '#111', border: '1px solid #333' }}
                  formatter={(value: ValueType | undefined) => [`${Number(Array.isArray(value) ? value[0] : value).toFixed(2)} GB`, t('resources.memory')]}
                />
                <Area
                  type="monotone"
                  dataKey="memory_used_gb"
                  stroke="#a78bfa"
                  fill="#a78bfa"
                  fillOpacity={0.2}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </StatCard>

          <StatCard
            icon={HardDrive}
            label={t('resources.disk')}
            current={
              latest && hasDiskData
                ? `${formatBytes(latest.disk_used_bytes ?? 0)} / ${formatBytes(latest.disk_total_bytes ?? 0)}`
                : t('resources.noDiskData')
            }
          >
            {hasDiskData ? (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                  <XAxis dataKey="time" tick={{ fontSize: 10 }} minTickGap={30} />
                  <YAxis tick={{ fontSize: 10 }} unit=" GB" />
                  <Tooltip
                    contentStyle={{ fontSize: 12, background: '#111', border: '1px solid #333' }}
                    formatter={(value: ValueType | undefined) => [`${Number(Array.isArray(value) ? value[0] : value).toFixed(2)} GB`, t('resources.disk')]}
                  />
                  <Area
                    type="monotone"
                    dataKey="disk_used_gb"
                    stroke="#fb923c"
                    fill="#fb923c"
                    fillOpacity={0.2}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex h-full items-center justify-center text-xs text-muted-foreground">
                <Gauge className="mr-1.5 h-3.5 w-3.5" />
                {t('resources.noDiskData')}
              </div>
            )}
          </StatCard>
        </div>
      )}
    </div>
  )
}

export default ResourcesPanel
