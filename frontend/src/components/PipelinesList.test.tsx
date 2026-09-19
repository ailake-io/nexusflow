import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { I18nProvider } from '@/lib/i18n/I18nProvider'
import { PipelinesList } from './PipelinesList'
import * as api from '@/lib/api'
import type { PipelineSummary } from '@/lib/api'

vi.mock('@/lib/auth-context', () => ({
  useAuth: () => ({ token: 'tok' }),
}))

const pipeline: PipelineSummary = {
  pipeline_id: 'p1',
  sources: [{ connector: 'csv', name: null }],
  sinks: [{ connector: 'csv', name: null }],
  has_transform: false,
  created_at: '2026-09-19T00:00:00Z',
  updated_at: '2026-09-19T00:00:00Z',
  schedule: null,
  depends_on: [],
  dependency_mode: 'any',
  last_run_status: null,
  last_run_at: null,
}

const refresh = vi.fn()

vi.mock('@/hooks/usePipelines', () => ({
  usePipelines: () => ({ pipelines: [pipeline], loading: false, error: null, refresh }),
}))

function renderList() {
  return render(
    <I18nProvider>
      <PipelinesList onEdit={vi.fn()} />
    </I18nProvider>,
  )
}

function openConfirm() {
  fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
}

describe('PipelinesList delete confirmation', () => {
  let deleteSpy: MockInstance<typeof api.deletePipeline>

  beforeEach(() => {
    localStorage.setItem('nexusflow.language', 'en')
    deleteSpy = vi.spyOn(api, 'deletePipeline').mockResolvedValue(undefined)
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    refresh.mockClear()
  })

  it('offers one checkbox per history category, all unchecked by default', () => {
    renderList()
    openConfirm()

    for (const label of ['LLM eval', 'Quality checks', 'dbt tests', 'Schema', 'Run volume']) {
      const box = screen.getByLabelText(label) as HTMLInputElement
      expect(box.checked).toBe(false)
    }
  })

  it('deletes all history by default (empty keep list)', async () => {
    renderList()
    openConfirm()
    fireEvent.click(screen.getByRole('button', { name: 'Yes, delete' }))

    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith('tok', 'p1', []))
  })

  it('passes only the ticked categories as the keep list', async () => {
    renderList()
    openConfirm()
    fireEvent.click(screen.getByLabelText('LLM eval'))
    fireEvent.click(screen.getByLabelText('Run volume'))
    fireEvent.click(screen.getByRole('button', { name: 'Yes, delete' }))

    await waitFor(() =>
      expect(deleteSpy).toHaveBeenCalledWith('tok', 'p1', ['llm_eval', 'volume']),
    )
  })

  it('un-ticking a category removes it from the keep list', async () => {
    renderList()
    openConfirm()
    fireEvent.click(screen.getByLabelText('dbt tests'))
    fireEvent.click(screen.getByLabelText('dbt tests'))
    fireEvent.click(screen.getByRole('button', { name: 'Yes, delete' }))

    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith('tok', 'p1', []))
  })

  it('resets the choices each time the confirmation is reopened', () => {
    renderList()
    openConfirm()
    fireEvent.click(screen.getByLabelText('Schema'))
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    openConfirm()

    expect((screen.getByLabelText('Schema') as HTMLInputElement).checked).toBe(false)
  })
})
