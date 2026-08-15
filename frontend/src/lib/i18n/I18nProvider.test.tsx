import { describe, expect, it } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { I18nProvider } from './I18nProvider'
import { useI18n } from './useI18n'

function LanguageLabel() {
  const { t, language, setLanguage } = useI18n()
  return (
    <div>
      <span data-testid="lang">{language}</span>
      <span data-testid="label">{t('nav.canvas')}</span>
      <button onClick={() => setLanguage('pt')}>switch</button>
    </div>
  )
}

describe('I18nProvider', () => {
  it('renders with detected language and switches', () => {
    render(
      <I18nProvider>
        <LanguageLabel />
      </I18nProvider>,
    )

    expect(screen.getByTestId('label').textContent).toBe('Canvas')
    fireEvent.click(screen.getByText('switch'))
    expect(screen.getByTestId('lang').textContent).toBe('pt')
    expect(screen.getByTestId('label').textContent).toBe('Canvas')
  })
})
