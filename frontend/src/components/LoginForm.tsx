import { useState, type FormEvent } from 'react'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { ApiError } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Logo } from '@/components/Logo'
import { LanguageToggle } from '@/components/LanguageToggle'
import { AlertCircle, ArrowRight, Database, Shield } from 'lucide-react'

export function LoginForm() {
  const { login } = useAuth()
  const { t } = useI18n()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await login(username, password)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : t('auth.loginFailed'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex min-h-screen w-full bg-background">
      <div className="relative hidden w-1/2 flex-col justify-between overflow-hidden bg-card p-10 lg:flex">
        <div className="absolute inset-0 tech-grid opacity-70" />
        <div className="absolute -right-24 -top-24 h-80 w-80 rounded-full bg-primary/20 blur-3xl" />
        <div className="absolute -bottom-24 -left-24 h-80 w-80 rounded-full bg-accent/20 blur-3xl" />

        <div className="relative z-10">
          <Logo />
        </div>

        <div className="relative z-10 max-w-sm">
          <h2 className="text-3xl font-semibold tracking-tight text-foreground">
            {t('auth.headline')
              .split(/(visually|visualmente)/i)
              .map((part, i) =>
                /^(visually|visualmente)$/i.test(part) ? (
                  <span key={i} className="text-primary">{part}</span>
                ) : (
                  part
                ),
              )}
          </h2>
          <p className="mt-3 text-sm leading-relaxed text-muted-foreground">
            {t('auth.subheadline')}
          </p>

          <div className="mt-8 flex flex-col gap-3">
            <div className="flex items-center gap-3 rounded-lg border border-white/5 bg-white/[0.02] p-3">
              <Database className="h-5 w-5 text-primary" />
              <div>
                <div className="text-xs font-medium text-foreground">{t('auth.featureConnectors')}</div>
                <div className="text-[10px] text-muted-foreground">{t('auth.featureConnectorsDesc')}</div>
              </div>
            </div>
            <div className="flex items-center gap-3 rounded-lg border border-white/5 bg-white/[0.02] p-3">
              <Shield className="h-5 w-5 text-accent" />
              <div>
                <div className="text-xs font-medium text-foreground">{t('auth.featureSecurity')}</div>
                <div className="text-[10px] text-muted-foreground">{t('auth.featureSecurityDesc')}</div>
              </div>
            </div>
          </div>
        </div>

        <div className="relative z-10 flex items-center justify-between">
          <span className="text-[10px] text-muted-foreground">
            {t('auth.copyright', { year: String(new Date().getFullYear()) })}
          </span>
          <LanguageToggle />
        </div>
      </div>

      <div className="flex w-full flex-col items-center justify-center p-6 lg:w-1/2 lg:p-10">
        <div className="w-full max-w-sm animate-slide-in-up">
          <div className="mb-8 flex items-center justify-between lg:hidden">
            <Logo />
            <LanguageToggle />
          </div>

          <Card className="border-white/10 bg-card/80 backdrop-blur">
            <CardHeader className="space-y-1">
              <CardTitle className="text-lg font-semibold">{t('auth.welcomeBack')}</CardTitle>
              <CardDescription className="text-xs text-muted-foreground">
                {t('auth.enterCredentials')}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleSubmit} className="flex flex-col gap-4">
                <div className="flex flex-col gap-2">
                  <Label htmlFor="username" className="text-xs font-medium">
                    {t('auth.username')}
                  </Label>
                  <Input
                    id="username"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    autoComplete="username"
                    placeholder="admin"
                    required
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="password" className="text-xs font-medium">
                    {t('auth.password')}
                  </Label>
                  <Input
                    id="password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    autoComplete="current-password"
                    placeholder="••••••••"
                    required
                  />
                </div>
                {error && (
                  <div className="flex items-start gap-2 rounded-md border border-red-500/20 bg-red-500/10 p-2 text-xs text-red-400">
                    <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    {error}
                  </div>
                )}
                <Button type="submit" disabled={submitting} className="gap-2">
                  {submitting ? t('auth.signingIn') : t('auth.signIn')}
                  {!submitting && <ArrowRight className="h-3.5 w-3.5" />}
                </Button>
              </form>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}
