import type { ReactNode } from 'react'
import { useI18n } from '@/lib/i18n'
import type {
  AlertsConfig,
  EmailAlertChannel,
  PagerDutyAlertChannel,
  WebhookAlertChannel,
} from '@/lib/dag'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

interface AlertsConfigDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  alerts?: AlertsConfig
  onChange: (alerts: AlertsConfig | undefined) => void
}

/**
 * Per-pipeline alert channel configuration — additive to nexus-server's
 * global, env-var-configured channels (which stay failure-only). Every edit
 * here writes straight through to the parent's `meta.alerts` via `onChange`
 * (no local draft state to lose on close/reopen) — same controlled pattern
 * `PipelineIoPanel` already uses for `meta`.
 */
export function AlertsConfigDialog({ open, onOpenChange, alerts, onChange }: AlertsConfigDialogProps) {
  const { t } = useI18n()

  const update = (patch: Partial<AlertsConfig>) => {
    const next: AlertsConfig = { ...alerts, ...patch }
    // Drop `alerts` entirely once every channel is off — matches
    // `PipelineSpec.alerts?: AlertsConfig`'s "unset means none" contract
    // instead of persisting an all-empty object.
    const isEmpty = !next.slack && !next.teams && !next.webhook && !next.pagerduty && !next.email
    onChange(isEmpty ? undefined : next)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{t('alerts.title')}</DialogTitle>
          <p className="text-sm text-muted-foreground">{t('alerts.subtitle')}</p>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          <WebhookSection
            label={t('alerts.slack')}
            urlPlaceholder="https://hooks.slack.com/services/…"
            channel={alerts?.slack}
            onChange={(c) => update({ slack: c })}
          />
          <WebhookSection
            label={t('alerts.teams')}
            urlPlaceholder="https://….webhook.office.com/…"
            channel={alerts?.teams}
            onChange={(c) => update({ teams: c })}
          />
          <WebhookSection
            label={t('alerts.webhook')}
            urlPlaceholder="https://…"
            channel={alerts?.webhook}
            onChange={(c) => update({ webhook: c })}
          />
          <PagerDutySection channel={alerts?.pagerduty} onChange={(c) => update({ pagerduty: c })} />
          <EmailSection channel={alerts?.email} onChange={(c) => update({ email: c })} />
        </div>
      </DialogContent>
    </Dialog>
  )
}

interface ChannelCardProps {
  label: string
  enabled: boolean
  onEnabledChange: (enabled: boolean) => void
  onSuccess: boolean
  onFailure: boolean
  onToggleSuccess: (value: boolean) => void
  onToggleFailure: (value: boolean) => void
  children: ReactNode
}

/** Shared shell for every channel section: enable checkbox, success/failure
 * toggles, and channel-specific fields (passed as children) shown only when
 * enabled. */
function ChannelCard({
  label,
  enabled,
  onEnabledChange,
  onSuccess,
  onFailure,
  onToggleSuccess,
  onToggleFailure,
  children,
}: ChannelCardProps) {
  const { t } = useI18n()
  return (
    <fieldset className="rounded-lg border border-white/10 p-3">
      <legend className="px-1">
        <label className="flex items-center gap-2 text-sm font-medium">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => onEnabledChange(e.target.checked)}
            className="h-4 w-4 rounded border-input bg-transparent text-primary accent-primary outline-none focus:ring-2 focus:ring-ring"
          />
          {label}
        </label>
      </legend>
      {enabled && (
        <div className="flex flex-col gap-3 pt-1">
          {children}
          <div className="flex gap-4 pt-1">
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <input
                type="checkbox"
                checked={onSuccess}
                onChange={(e) => onToggleSuccess(e.target.checked)}
                className="h-3.5 w-3.5 rounded border-input bg-transparent text-primary accent-primary outline-none focus:ring-2 focus:ring-ring"
              />
              {t('alerts.notifyOnSuccess')}
            </label>
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <input
                type="checkbox"
                checked={onFailure}
                onChange={(e) => onToggleFailure(e.target.checked)}
                className="h-3.5 w-3.5 rounded border-input bg-transparent text-primary accent-primary outline-none focus:ring-2 focus:ring-ring"
              />
              {t('alerts.notifyOnFailure')}
            </label>
          </div>
        </div>
      )}
    </fieldset>
  )
}

interface WebhookSectionProps {
  label: string
  urlPlaceholder: string
  channel?: WebhookAlertChannel
  onChange: (channel: WebhookAlertChannel | undefined) => void
}

function WebhookSection({ label, urlPlaceholder, channel, onChange }: WebhookSectionProps) {
  const { t } = useI18n()
  return (
    <ChannelCard
      label={label}
      enabled={channel !== undefined}
      onEnabledChange={(enabled) =>
        onChange(enabled ? { url: '', on_success: false, on_failure: true } : undefined)
      }
      onSuccess={channel?.on_success ?? false}
      onFailure={channel?.on_failure ?? true}
      onToggleSuccess={(v) => channel && onChange({ ...channel, on_success: v })}
      onToggleFailure={(v) => channel && onChange({ ...channel, on_failure: v })}
    >
      <div>
        <Label className="text-xs">{t('alerts.webhookUrl')}</Label>
        <Input
          type="password"
          value={channel?.url ?? ''}
          placeholder={urlPlaceholder}
          onChange={(e) => channel && onChange({ ...channel, url: e.target.value })}
          className="mt-1"
        />
      </div>
    </ChannelCard>
  )
}

interface PagerDutySectionProps {
  channel?: PagerDutyAlertChannel
  onChange: (channel: PagerDutyAlertChannel | undefined) => void
}

function PagerDutySection({ channel, onChange }: PagerDutySectionProps) {
  const { t } = useI18n()
  return (
    <ChannelCard
      label={t('alerts.pagerduty')}
      enabled={channel !== undefined}
      onEnabledChange={(enabled) =>
        onChange(enabled ? { routing_key: '', on_success: false, on_failure: true } : undefined)
      }
      onSuccess={channel?.on_success ?? false}
      onFailure={channel?.on_failure ?? true}
      onToggleSuccess={(v) => channel && onChange({ ...channel, on_success: v })}
      onToggleFailure={(v) => channel && onChange({ ...channel, on_failure: v })}
    >
      <div>
        <Label className="text-xs">{t('alerts.routingKey')}</Label>
        <Input
          type="password"
          value={channel?.routing_key ?? ''}
          onChange={(e) => channel && onChange({ ...channel, routing_key: e.target.value })}
          className="mt-1"
        />
      </div>
    </ChannelCard>
  )
}

interface EmailSectionProps {
  channel?: EmailAlertChannel
  onChange: (channel: EmailAlertChannel | undefined) => void
}

function EmailSection({ channel, onChange }: EmailSectionProps) {
  const { t } = useI18n()
  return (
    <ChannelCard
      label={t('alerts.email')}
      enabled={channel !== undefined}
      onEnabledChange={(enabled) =>
        onChange(
          enabled
            ? {
                smtp_host: '',
                smtp_port: 587,
                from: '',
                to: [],
                on_success: false,
                on_failure: true,
              }
            : undefined,
        )
      }
      onSuccess={channel?.on_success ?? false}
      onFailure={channel?.on_failure ?? true}
      onToggleSuccess={(v) => channel && onChange({ ...channel, on_success: v })}
      onToggleFailure={(v) => channel && onChange({ ...channel, on_failure: v })}
    >
      <div className="flex gap-2">
        <div className="flex-1">
          <Label className="text-xs">{t('alerts.smtpHost')}</Label>
          <Input
            value={channel?.smtp_host ?? ''}
            onChange={(e) => channel && onChange({ ...channel, smtp_host: e.target.value })}
            className="mt-1"
          />
        </div>
        <div className="w-24">
          <Label className="text-xs">{t('alerts.smtpPort')}</Label>
          <Input
            type="number"
            value={channel?.smtp_port ?? 587}
            onChange={(e) =>
              channel && onChange({ ...channel, smtp_port: Number(e.target.value) || 587 })
            }
            className="mt-1"
          />
        </div>
      </div>
      <div className="flex gap-2">
        <div className="flex-1">
          <Label className="text-xs">{t('alerts.smtpUsername')}</Label>
          <Input
            value={channel?.username ?? ''}
            onChange={(e) => channel && onChange({ ...channel, username: e.target.value })}
            className="mt-1"
          />
        </div>
        <div className="flex-1">
          <Label className="text-xs">{t('alerts.smtpPassword')}</Label>
          <Input
            type="password"
            value={channel?.password ?? ''}
            onChange={(e) => channel && onChange({ ...channel, password: e.target.value })}
            className="mt-1"
          />
        </div>
      </div>
      <div>
        <Label className="text-xs">{t('alerts.emailFrom')}</Label>
        <Input
          value={channel?.from ?? ''}
          placeholder="nexus@example.com"
          onChange={(e) => channel && onChange({ ...channel, from: e.target.value })}
          className="mt-1"
        />
      </div>
      <div>
        <Label className="text-xs">{t('alerts.emailTo')}</Label>
        <Input
          value={channel?.to?.join(', ') ?? ''}
          placeholder="ops@example.com, oncall@example.com"
          onChange={(e) =>
            channel &&
            onChange({
              ...channel,
              to: e.target.value
                .split(',')
                .map((s) => s.trim())
                .filter(Boolean),
            })
          }
          className="mt-1"
        />
      </div>
    </ChannelCard>
  )
}
