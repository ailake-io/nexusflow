import { useEffect, useState, type FormEvent } from 'react'
import { MessageSquare, Send, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import {
  listPipelines,
  ragQuery,
  RAG_CAPABLE_SINK_CONNECTORS,
  type PipelineSummary,
} from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'

interface ChatMessage {
  role: 'user' | 'assistant'
  text: string
  contextKeys?: string[]
  generationId?: number
}

/** RAG chat (LLMOPS_IMPLEMENTATION_PLAN.md Marco L5) — thin UI over
 * `POST /rag/query`, which already does the real work (embed the question,
 * search the pipeline's vector sink, answer grounded in the result). No
 * server-side conversation state: each question is independent, same as
 * the endpoint itself. */
export function RagChatPanel() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([])
  const [pipelineId, setPipelineId] = useState('')
  const [question, setQuestion] = useState('')
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [asking, setAsking] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!token) return
    listPipelines(token)
      .then((all) =>
        setPipelines(
          all.filter((p) => p.sinks.some((s) => RAG_CAPABLE_SINK_CONNECTORS.includes(s.connector))),
        ),
      )
      .catch(() => setPipelines([]))
  }, [token])

  const handleAsk = (event: FormEvent) => {
    event.preventDefault()
    if (!token || !pipelineId || !question.trim() || asking) return
    const asked = question
    setMessages((prev) => [...prev, { role: 'user', text: asked }])
    setQuestion('')
    setAsking(true)
    setError(null)
    ragQuery(token, pipelineId, asked)
      .then((res) =>
        setMessages((prev) => [
          ...prev,
          {
            role: 'assistant',
            text: res.answer,
            contextKeys: res.context_keys,
            generationId: res.generation_id,
          },
        ]),
      )
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setAsking(false))
  }

  return (
    <div className="flex h-full flex-col gap-4 p-6">
      <div>
        <h1 className="text-lg font-semibold">{t('rag.title')}</h1>
        <p className="text-sm text-muted-foreground">{t('rag.subtitle')}</p>
      </div>

      <select
        className="w-full max-w-md rounded-md border border-white/10 bg-card px-3 py-2 text-sm"
        value={pipelineId}
        onChange={(e) => setPipelineId(e.target.value)}
      >
        <option value="">{t('rag.pipelinePlaceholder')}</option>
        {pipelines.map((p) => (
          <option key={p.pipeline_id} value={p.pipeline_id}>
            {p.pipeline_id}
          </option>
        ))}
      </select>
      {pipelines.length === 0 && (
        <p className="text-xs text-muted-foreground">{t('rag.noPipelines')}</p>
      )}

      <div className="flex-1 overflow-auto rounded-xl border border-white/10 bg-card/30 p-4">
        {messages.length === 0 ? (
          <EmptyState icon={<MessageSquare className="h-6 w-6" />} title={t('rag.empty')} />
        ) : (
          <div className="flex flex-col gap-3">
            {messages.map((m, i) => (
              <div
                key={i}
                className={m.role === 'user' ? 'ml-auto max-w-[80%] text-right' : 'max-w-[80%]'}
              >
                <div
                  className={
                    m.role === 'user'
                      ? 'inline-block rounded-lg bg-primary/20 px-3 py-2 text-sm'
                      : 'inline-block rounded-lg bg-white/5 px-3 py-2 text-sm'
                  }
                >
                  {m.text}
                </div>
                {m.contextKeys && m.contextKeys.length > 0 && (
                  <div className="mt-1 text-xs text-muted-foreground">
                    {t('rag.sources')}: {m.contextKeys.join(', ')}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {error && <p className="text-sm text-red-400">{error}</p>}

      <form onSubmit={handleAsk} className="flex gap-2">
        <input
          className="flex-1 rounded-md border border-white/10 bg-card px-3 py-2 text-sm"
          placeholder={t('rag.questionPlaceholder')}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          disabled={!pipelineId}
        />
        <Button type="submit" disabled={!pipelineId || !question.trim() || asking}>
          {asking ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
          {asking ? t('rag.asking') : t('rag.ask')}
        </Button>
      </form>
    </div>
  )
}

export default RagChatPanel
