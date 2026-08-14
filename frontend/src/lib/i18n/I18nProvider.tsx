import { useEffect, useState, type ReactNode } from 'react'
import { I18nContext } from './I18nContext'
import { translations, type Language } from './translations'

const STORAGE_KEY = 'nexusflow-language'

function detectLanguage(): Language {
  if (typeof window === 'undefined') return 'en'
  const stored = window.localStorage.getItem(STORAGE_KEY)
  if (stored === 'en' || stored === 'pt') return stored
  const browser = navigator.language.toLowerCase()
  return browser.startsWith('pt') ? 'pt' : 'en'
}

function getNestedValue(obj: Record<string, unknown>, path: string): string | undefined {
  const parts = path.split('.')
  let current: unknown = obj
  for (const part of parts) {
    if (current === null || typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }
  return typeof current === 'string' ? current : undefined
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const [language, setLanguageState] = useState<Language>(detectLanguage)

  const setLanguage = (lang: Language) => {
    setLanguageState(lang)
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(STORAGE_KEY, lang)
    }
  }

  useEffect(() => {
    const stored = window.localStorage.getItem(STORAGE_KEY)
    if (stored === 'en' || stored === 'pt') {
      setLanguageState(stored)
    }
  }, [])

  useEffect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.lang = language
    }
  }, [language])

  const t = (key: string, vars?: Record<string, string | number>): string => {
    const messages = translations[language] as Record<string, unknown>
    let value = getNestedValue(messages, key)
    if (value === undefined) {
      // Fallback to English if key is missing.
      const fallback = translations.en as Record<string, unknown>
      value = getNestedValue(fallback, key)
    }
    if (value === undefined) return key
    if (!vars) return value
    return value.replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? `{${name}}`))
  }

  return (
    <I18nContext.Provider value={{ language, setLanguage, t }}>
      {children}
    </I18nContext.Provider>
  )
}
