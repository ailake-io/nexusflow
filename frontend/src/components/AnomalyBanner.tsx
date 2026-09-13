import { AlertTriangle, ShieldAlert, ShieldCheck } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import type { AnomalyStatus } from '@/lib/api'

interface AnomalyBannerProps {
  status: AnomalyStatus
}

/**
 * Current anomaly status for one tracked metric (Fase 27) — `rows_written`
 * today, see `AnomalyStatus`'s doc comment. Purely presentational: the
 * parent (`QualityPanel`) owns fetching `GET /pipelines/{id}/anomalies`
 * and deciding whether to render this at all (nothing to show with zero
 * history).
 */
export function AnomalyBanner({ status }: AnomalyBannerProps) {
  const { t } = useI18n()

  if (status.history_size < 1) {
    return (
      <div className="flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.02] p-3 text-xs text-muted-foreground">
        <ShieldCheck className="h-4 w-4 shrink-0" />
        {t('quality.anomaly.notEnoughHistory')}
      </div>
    )
  }

  const style =
    status.severity === 'critical'
      ? 'border-red-500/30 bg-red-500/10 text-red-400'
      : status.severity === 'warning'
        ? 'border-amber-500/30 bg-amber-500/10 text-amber-300'
        : 'border-white/10 bg-white/[0.02] text-muted-foreground'
  const Icon =
    status.severity === 'critical' ? ShieldAlert : status.severity === 'warning' ? AlertTriangle : ShieldCheck
  const statusText =
    status.severity === 'critical'
      ? t('quality.anomaly.critical')
      : status.severity === 'warning'
        ? t('quality.anomaly.warning')
        : t('quality.anomaly.noAnomaly')

  return (
    <div className={`rounded-lg border p-3 text-xs ${style}`}>
      <div className="flex items-center gap-2 font-medium">
        <Icon className="h-4 w-4 shrink-0" />
        {statusText}
      </div>
      <div className="mt-1.5 text-[11px] opacity-80">
        {t('quality.anomaly.latestValue', {
          value: status.latest_value.toLocaleString(),
          runId: status.latest_run_id,
        })}
      </div>
      <div className="text-[11px] opacity-80">
        {t('quality.anomaly.baseline', {
          mean: status.baseline_mean.toFixed(1),
          stddev: status.baseline_stddev.toFixed(1),
          count: status.history_size,
        })}
      </div>
    </div>
  )
}

export default AnomalyBanner
