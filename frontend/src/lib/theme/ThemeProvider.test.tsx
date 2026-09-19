import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { I18nProvider } from '@/lib/i18n'
import { ThemeToggle } from '@/components/ThemeToggle'
import { ThemeProvider, THEME_STORAGE_KEY } from './ThemeProvider'
import { useTheme } from './useTheme'

function ThemeProbe() {
  const { theme, setTheme } = useTheme()
  return (
    <div>
      <span data-testid="theme">{theme}</span>
      <button onClick={() => setTheme('light')}>set-light</button>
    </div>
  )
}

function renderWithProviders(ui: React.ReactNode) {
  return render(
    <ThemeProvider>
      <I18nProvider>{ui}</I18nProvider>
    </ThemeProvider>,
  )
}

const isLight = () => document.documentElement.classList.contains('light')

describe('ThemeProvider', () => {
  beforeEach(() => {
    localStorage.clear()
    localStorage.setItem('nexusflow-language', 'en')
    document.documentElement.classList.remove('light')
  })

  afterEach(() => {
    cleanup()
    document.documentElement.classList.remove('light')
  })

  it('defaults to dark, which is the absence of the light class', () => {
    renderWithProviders(<ThemeProbe />)
    expect(screen.getByTestId('theme').textContent).toBe('dark')
    expect(isLight()).toBe(false)
  })

  it('restores a saved light theme on load', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'light')
    renderWithProviders(<ThemeProbe />)
    expect(screen.getByTestId('theme').textContent).toBe('light')
    expect(isLight()).toBe(true)
  })

  it('ignores an unknown stored value and falls back to dark', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'solarized')
    renderWithProviders(<ThemeProbe />)
    expect(screen.getByTestId('theme').textContent).toBe('dark')
    expect(isLight()).toBe(false)
  })

  it('setTheme applies the class and persists the choice', () => {
    renderWithProviders(<ThemeProbe />)
    fireEvent.click(screen.getByText('set-light'))
    expect(isLight()).toBe(true)
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light')
  })

  it('useTheme outside the provider throws a clear error', () => {
    expect(() => render(<ThemeProbe />)).toThrow('useTheme must be used within ThemeProvider')
  })
})

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorage.clear()
    localStorage.setItem('nexusflow-language', 'en')
    document.documentElement.classList.remove('light')
  })

  afterEach(() => {
    cleanup()
    document.documentElement.classList.remove('light')
  })

  it('offers the theme you would switch to and toggles both ways', () => {
    renderWithProviders(<ThemeToggle />)

    fireEvent.click(screen.getByRole('button', { name: 'Switch to light theme' }))
    expect(isLight()).toBe(true)
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light')

    fireEvent.click(screen.getByRole('button', { name: 'Switch to dark theme' }))
    expect(isLight()).toBe(false)
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark')
  })
})
