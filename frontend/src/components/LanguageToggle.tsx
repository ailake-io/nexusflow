import { Globe } from 'lucide-react'
import { useI18n, type Language } from '@/lib/i18n'
import { Button } from '@/components/ui/button'

export function LanguageToggle() {
  const { language, setLanguage, t } = useI18n()

  const toggle = () => {
    const next: Language = language === 'en' ? 'pt' : 'en'
    setLanguage(next)
  }

  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={toggle}
      className="gap-1.5 text-xs text-muted-foreground hover:text-foreground"
      aria-label={t('languages.' + language)}
    >
      <Globe className="h-3.5 w-3.5" />
      {language.toUpperCase()}
    </Button>
  )
}
