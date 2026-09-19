import { Moon, Sun } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import { useTheme } from '@/lib/theme'
import { Button } from '@/components/ui/button'

/** Icon button showing the theme you would switch *to* (sun while dark, moon
 * while light), same ghost style as `LanguageToggle` next to it. */
export function ThemeToggle() {
  const { theme, toggleTheme } = useTheme()
  const { t } = useI18n()
  const label = theme === 'light' ? t('theme.toDark') : t('theme.toLight')
  const Icon = theme === 'light' ? Moon : Sun

  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={toggleTheme}
      className="text-muted-foreground hover:text-foreground"
      aria-label={label}
      title={label}
    >
      <Icon className="h-3.5 w-3.5" />
    </Button>
  )
}
