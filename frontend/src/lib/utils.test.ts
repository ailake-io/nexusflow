import { describe, expect, it } from 'vitest'
import { cn, formatDuration } from './utils'

describe('cn', () => {
  it('merges class names and resolves tailwind conflicts', () => {
    expect(cn('px-2 py-1', 'px-4')).toBe('py-1 px-4')
  })

  it('ignores falsy values', () => {
    const active = false
    expect(cn('base', active && 'active', null, undefined)).toBe('base')
  })
})

describe('formatDuration', () => {
  it('returns null while the run has not finished', () => {
    expect(formatDuration('2026-01-01T00:00:00Z', null)).toBeNull()
  })

  it('formats sub-second durations as <1s', () => {
    expect(formatDuration('2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.400Z')).toBe('<1s')
  })

  it('formats seconds only', () => {
    expect(formatDuration('2026-01-01T00:00:00Z', '2026-01-01T00:00:42Z')).toBe('42s')
  })

  it('formats minutes and seconds', () => {
    expect(formatDuration('2026-01-01T00:00:00Z', '2026-01-01T00:02:34Z')).toBe('2m 34s')
  })

  it('formats hours, minutes and seconds', () => {
    expect(formatDuration('2026-01-01T00:00:00Z', '2026-01-01T01:05:03Z')).toBe('1h 5m 3s')
  })

  it('returns null for malformed timestamps', () => {
    expect(formatDuration('not-a-date', '2026-01-01T00:00:00Z')).toBeNull()
  })
})
