import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'
import {
  ApiError,
  deletePipeline,
  login,
  onUnauthorized,
  progressSocketUrl,
  runPipeline,
} from './api'

describe('request', () => {
  beforeEach(() => {
    onUnauthorized(null as unknown as () => void)
  })

 afterEach(() => {
    vi.restoreAllMocks()
  })

  it('returns parsed JSON on success', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ token: 'abc' }),
    }))

    const token = await login('admin', 'secret')
    expect(token).toBe('abc')
  })

  it('throws ApiError with message from body on failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 400,
      json: () => Promise.resolve({ error: 'bad request' }),
    }))

    await expect(login('admin', 'secret')).rejects.toThrow(ApiError)
    await expect(login('admin', 'secret')).rejects.toThrow('bad request')
  })

  it('invokes unauthorized handler on 401 (except login)', async () => {
    const handler = vi.fn()
    onUnauthorized(handler)

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      json: () => Promise.resolve({ error: 'unauthorized' }),
    }))

    await expect(runPipeline('token', { pipeline_id: 'p1' } as unknown as import('@/lib/dag').PipelineSpec)).rejects.toThrow()
    expect(handler).toHaveBeenCalledOnce()
  })

  it('does not invoke unauthorized handler on login 401', async () => {
    const handler = vi.fn()
    onUnauthorized(handler)

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      json: () => Promise.resolve({ error: 'invalid credentials' }),
    }))

    await expect(login('admin', 'wrong')).rejects.toThrow()
    expect(handler).not.toHaveBeenCalled()
  })
})

describe('progressSocketUrl', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('uses ws for http and encodes path segments', () => {
    vi.stubGlobal('window', { location: { protocol: 'http:', host: 'localhost:8080' } } as Window & typeof globalThis)

    const url = progressSocketUrl('p/1', 42)
    expect(url).toBe('ws://localhost:8080/pipelines/p%2F1/runs/42/progress')
  })

  it('uses wss for https', () => {
    vi.stubGlobal('window', { location: { protocol: 'https:', host: 'app.example' } } as Window & typeof globalThis)

    const url = progressSocketUrl('p1', 1)
    expect(url).toBe('wss://app.example/pipelines/p1/runs/1/progress')
  })
})

describe('deletePipeline', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  const stubNoContent = () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 204 })
    vi.stubGlobal('fetch', fetchMock)
    return fetchMock
  }

  it('sends no keep param by default, so all history is deleted', async () => {
    const fetchMock = stubNoContent()
    await deletePipeline('tok', 'p1')
    expect(fetchMock.mock.calls[0][0]).toBe('/pipelines/p1')
    expect(fetchMock.mock.calls[0][1].method).toBe('DELETE')
  })

  it('sends the chosen history kinds as a comma-separated keep param', async () => {
    const fetchMock = stubNoContent()
    await deletePipeline('tok', 'p1', ['llm_eval', 'volume'])
    expect(fetchMock.mock.calls[0][0]).toBe('/pipelines/p1?keep=llm_eval,volume')
  })

  it('percent-encodes the pipeline id', async () => {
    const fetchMock = stubNoContent()
    await deletePipeline('tok', 'my pipeline/1', ['dbt'])
    expect(fetchMock.mock.calls[0][0]).toBe('/pipelines/my%20pipeline%2F1?keep=dbt')
  })
})
