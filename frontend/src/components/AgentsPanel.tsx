import { useCallback, useEffect, useState } from 'react'
import { Bot, Plus, Loader2, Check, X, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import {
  listAgents,
  getAgentSpec,
  createAgent,
  updateAgent,
  deleteAgent,
  runAgent,
  listAgentRuns,
  getAgentRun,
  approveAgentStep,
  rejectAgentStep,
  type AgentSpec,
  type AgentSummary,
  type AgentToolConfig,
  type AgentToolKind,
  type AgentRun,
  type AgentRunDetail,
  type ApprovalMode,
} from '@/lib/api'
import { EmptyState } from '@/components/EmptyState'

const TOOL_KINDS: AgentToolKind['kind'][] = [
  'query_data',
  'search_vectors',
  'run_pipeline',
  'call_webhook',
  'generate_chart',
]

function defaultToolFor(kind: AgentToolKind['kind']): AgentToolKind {
  switch (kind) {
    case 'query_data':
      return { kind, source: { connector: 'postgres', config: {} } }
    case 'search_vectors':
      return { kind, pipeline_id: '', top_k: 5 }
    case 'run_pipeline':
      return { kind, pipeline_id: '', wait_for_result: false }
    case 'call_webhook':
      return { kind, url: '', method: 'POST' }
    case 'generate_chart':
      return {
        kind,
        source: { connector: 'postgres', config: {} },
        script: 'def visualize(df):\n    ...\n',
      }
  }
}

function emptyAgent(): AgentSpec {
  return {
    agent_id: '',
    name: '',
    prompt: { name: '' },
    model: { backend: 'api', base_url: 'https://api.openai.com/v1', model: 'gpt-4o-mini' },
    tools: [{ tool: defaultToolFor('search_vectors'), approval: 'auto' }],
    max_steps: 8,
  }
}

const RUN_STATUS_VARIANT: Record<AgentRun['status'], 'success' | 'failed' | 'running' | 'warning' | 'idle'> = {
  running: 'running',
  waiting_approval: 'warning',
  completed: 'success',
  failed: 'failed',
  max_steps_reached: 'warning',
}

export function AgentsPanel() {
  const { token } = useAuth()
  const { t } = useI18n()
  const [agents, setAgents] = useState<AgentSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [editing, setEditing] = useState<AgentSpec | null>(null)
  const [saving, setSaving] = useState(false)

  const [runs, setRuns] = useState<AgentRun[]>([])
  const [question, setQuestion] = useState('')
  const [running, setRunning] = useState(false)
  const [selectedRun, setSelectedRun] = useState<AgentRunDetail | null>(null)

  const refresh = () => {
    if (!token) return
    setLoading(true)
    listAgents(token)
      .then((r) => {
        setAgents(r)
        setError(null)
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setLoading(false))
  }

  useEffect(refresh, [token])

  const refreshRuns = useCallback(
    (agentId: string) => {
      if (!token) return
      listAgentRuns(token, agentId)
        .then(setRuns)
        .catch(() => setRuns([]))
    },
    [token],
  )

  useEffect(() => {
    if (selectedId) refreshRuns(selectedId)
    else setRuns([])
    setSelectedRun(null)
  }, [selectedId, refreshRuns])

  // Poll while a run is active — same lightweight approach the rest of
  // this frontend uses for pipeline runs, no WebSocket for agents in v1.
  useEffect(() => {
    if (!token || !selectedRun) return
    if (selectedRun.status !== 'running' && selectedRun.status !== 'waiting_approval') return
    const timer = setInterval(() => {
      getAgentRun(token, selectedRun.agent_id, selectedRun.id).then(setSelectedRun)
    }, 2000)
    return () => clearInterval(timer)
  }, [token, selectedRun])

  const startNew = () => {
    setSelectedId(null)
    setEditing(emptyAgent())
  }

  const startEdit = (id: string) => {
    if (!token) return
    setSelectedId(id)
    getAgentSpec(token, id).then(setEditing)
  }

  const save = () => {
    if (!token || !editing) return
    setSaving(true)
    const isNew = !agents.some((a) => a.agent_id === editing.agent_id)
    const call = isNew ? createAgent(token, editing) : updateAgent(token, editing)
    call
      .then(() => {
        setEditing(null)
        refresh()
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setSaving(false))
  }

  const remove = (id: string) => {
    if (!token) return
    deleteAgent(token, id).then(() => {
      if (selectedId === id) setSelectedId(null)
      refresh()
    })
  }

  const triggerRun = () => {
    if (!token || !selectedId || !question.trim()) return
    setRunning(true)
    runAgent(token, selectedId, question)
      .then(() => {
        setQuestion('')
        refreshRuns(selectedId)
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setRunning(false))
  }

  const openRun = (run: AgentRun) => {
    if (!token) return
    getAgentRun(token, run.agent_id, run.id).then(setSelectedRun)
  }

  const decide = (stepId: number, approve: boolean) => {
    if (!token || !selectedRun) return
    const call = approve
      ? approveAgentStep(token, selectedRun.id, stepId)
      : rejectAgentStep(token, selectedRun.id, stepId)
    call.then(() => getAgentRun(token, selectedRun.agent_id, selectedRun.id).then(setSelectedRun))
  }

  return (
    <div className="flex h-full gap-4 p-6">
      <div className="w-72 shrink-0 space-y-3">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold">{t('agents.title')}</h1>
          <Button size="sm" onClick={startNew}>
            <Plus className="h-4 w-4" />
            {t('agents.newAgent')}
          </Button>
        </div>
        {loading ? (
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        ) : agents.length === 0 ? (
          <EmptyState icon={<Bot className="h-6 w-6" />} title={t('agents.empty')} />
        ) : (
          <ul className="space-y-1">
            {agents.map((a) => (
              <li key={a.agent_id}>
                <button
                  className={`flex w-full items-center justify-between rounded-md px-3 py-2 text-left text-sm hover:bg-white/5 ${
                    selectedId === a.agent_id ? 'bg-white/10' : ''
                  }`}
                  onClick={() => startEdit(a.agent_id)}
                >
                  <span>{a.name}</span>
                  <Trash2
                    className="h-3.5 w-3.5 text-muted-foreground hover:text-red-400"
                    onClick={(e) => {
                      e.stopPropagation()
                      remove(a.agent_id)
                    }}
                  />
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <div className="flex-1 overflow-auto">
        {error && <p className="mb-3 text-sm text-red-400">{error}</p>}

        {editing ? (
          <AgentForm
            spec={editing}
            onChange={setEditing}
            onSave={save}
            onCancel={() => setEditing(null)}
            saving={saving}
            t={t}
          />
        ) : selectedId ? (
          <div className="space-y-4">
            <div className="flex gap-2">
              <input
                className="flex-1 rounded-md border border-white/10 bg-card px-3 py-2 text-sm"
                placeholder={t('agents.questionPlaceholder')}
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
              />
              <Button onClick={triggerRun} disabled={running || !question.trim()}>
                {running ? <Loader2 className="h-4 w-4 animate-spin" /> : t('agents.run')}
              </Button>
            </div>

            <div>
              <h2 className="mb-2 text-sm font-medium text-muted-foreground">{t('agents.runs')}</h2>
              {runs.length === 0 ? (
                <p className="text-sm text-muted-foreground">{t('agents.noRuns')}</p>
              ) : (
                <ul className="space-y-1">
                  {runs.map((r) => (
                    <li key={r.id}>
                      <button
                        className="flex w-full items-center justify-between rounded-md border border-white/10 px-3 py-2 text-left text-sm hover:bg-white/5"
                        onClick={() => openRun(r)}
                      >
                        <span className="truncate">{r.question}</span>
                        <StatusBadge variant={RUN_STATUS_VARIANT[r.status]}>
                          {t(`agents.status.${r.status}`)}
                        </StatusBadge>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>

            {selectedRun && (
              <div className="rounded-xl border border-white/10 p-4">
                <h2 className="mb-2 text-sm font-medium text-muted-foreground">{t('agents.trace')}</h2>
                <ol className="space-y-2">
                  {selectedRun.steps.map((s) => (
                    <li key={s.id} className="rounded-md bg-white/5 p-3 text-sm">
                      <div className="flex items-center justify-between">
                        <span className="font-medium">
                          {s.kind === 'final_answer'
                            ? '→'
                            : s.tool
                              ? t(`agents.toolKind.${s.tool}`)
                              : s.kind}
                        </span>
                        {s.approval_status === 'pending' && (
                          <div className="flex gap-1">
                            <Button size="sm" variant="outline" onClick={() => decide(s.id, true)}>
                              <Check className="h-3.5 w-3.5" /> {t('agents.approve')}
                            </Button>
                            <Button size="sm" variant="outline" onClick={() => decide(s.id, false)}>
                              <X className="h-3.5 w-3.5" /> {t('agents.reject')}
                            </Button>
                          </div>
                        )}
                      </div>
                      {s.approval_status === 'pending' && (
                        <p className="mt-1 text-xs text-amber-400">{t('agents.pendingApproval')}</p>
                      )}
                      {s.result && (
                        <p className="mt-1 whitespace-pre-wrap text-xs text-muted-foreground">
                          {s.result}
                        </p>
                      )}
                    </li>
                  ))}
                </ol>
              </div>
            )}
          </div>
        ) : (
          <EmptyState icon={<Bot className="h-6 w-6" />} title={t('agents.subtitle')} />
        )}
      </div>
    </div>
  )
}

function AgentForm({
  spec,
  onChange,
  onSave,
  onCancel,
  saving,
  t,
}: {
  spec: AgentSpec
  onChange: (s: AgentSpec) => void
  onSave: () => void
  onCancel: () => void
  saving: boolean
  t: (key: string) => string
}) {
  const update = (patch: Partial<AgentSpec>) => onChange({ ...spec, ...patch })

  const updateTool = (index: number, patch: Partial<AgentToolConfig>) => {
    const tools = [...spec.tools]
    tools[index] = { ...tools[index], ...patch }
    onChange({ ...spec, tools })
  }

  return (
    <div className="max-w-xl space-y-4">
      <div>
        <Label>{t('agents.name')}</Label>
        <Input
          value={spec.name}
          onChange={(e) =>
            update({
              name: e.target.value,
              agent_id: spec.agent_id || e.target.value.toLowerCase().replace(/[^a-z0-9_-]/g, '-'),
            })
          }
          placeholder={t('agents.namePlaceholder')}
        />
      </div>

      <div>
        <Label>{t('agents.prompt')}</Label>
        <Input
          value={spec.prompt.name}
          onChange={(e) => update({ prompt: { ...spec.prompt, name: e.target.value } })}
        />
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <Label>{t('agents.model')}</Label>
          <select
            className="w-full rounded-md border border-white/10 bg-card px-3 py-2 text-sm"
            value={spec.model.backend}
            onChange={(e) =>
              update({
                model:
                  e.target.value === 'anthropic'
                    ? {
                        backend: 'anthropic',
                        base_url: 'https://api.anthropic.com',
                        model: 'claude-sonnet-5',
                        api_key_env: 'ANTHROPIC_API_KEY',
                      }
                    : {
                        backend: 'api',
                        base_url: 'https://api.openai.com/v1',
                        model: 'gpt-4o-mini',
                      },
              })
            }
          >
            <option value="api">{t('agents.backendApi')}</option>
            <option value="anthropic">{t('agents.backendAnthropic')}</option>
          </select>
        </div>
        <div>
          <Label>{t('agents.maxSteps')}</Label>
          <Input
            type="number"
            min={1}
            value={spec.max_steps}
            onChange={(e) => update({ max_steps: Number(e.target.value) || 1 })}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <Label>{t('agents.baseUrl')}</Label>
          <Input
            value={spec.model.base_url}
            onChange={(e) => update({ model: { ...spec.model, base_url: e.target.value } })}
          />
        </div>
        <div>
          <Label>{t('agents.modelName')}</Label>
          <Input
            value={spec.model.model}
            onChange={(e) => update({ model: { ...spec.model, model: e.target.value } })}
          />
        </div>
      </div>

      <div>
        <Label>{t('agents.apiKeyEnv')}</Label>
        <Input
          value={spec.model.api_key_env ?? ''}
          onChange={(e) => update({ model: { ...spec.model, api_key_env: e.target.value } })}
        />
      </div>

      <div>
        <div className="mb-2 flex items-center justify-between">
          <Label>{t('agents.tools')}</Label>
          <Button
            size="sm"
            variant="outline"
            onClick={() =>
              update({
                tools: [...spec.tools, { tool: defaultToolFor('search_vectors'), approval: 'auto' }],
              })
            }
          >
            <Plus className="h-3.5 w-3.5" />
          </Button>
        </div>
        <div className="space-y-2">
          {spec.tools.map((tc, i) => (
            <ToolRow
              key={i}
              tool={tc}
              onChange={(patch) => updateTool(i, patch)}
              onRemove={() => onChange({ ...spec, tools: spec.tools.filter((_, j) => j !== i) })}
              t={t}
            />
          ))}
        </div>
      </div>

      <div className="flex gap-2">
        <Button onClick={onSave} disabled={saving || !spec.name || !spec.prompt.name}>
          {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : t('agents.save')}
        </Button>
        <Button variant="outline" onClick={onCancel}>
          {t('agents.cancel')}
        </Button>
      </div>
    </div>
  )
}

function ToolRow({
  tool,
  onChange,
  onRemove,
  t,
}: {
  tool: AgentToolConfig
  onChange: (patch: Partial<AgentToolConfig>) => void
  onRemove: () => void
  t: (key: string) => string
}) {
  return (
    <div className="rounded-md border border-white/10 p-3">
      <div className="flex items-center gap-2">
        <select
          className="flex-1 rounded-md border border-white/10 bg-card px-2 py-1 text-sm"
          value={tool.tool.kind}
          onChange={(e) => onChange({ tool: defaultToolFor(e.target.value as AgentToolKind['kind']) })}
        >
          {TOOL_KINDS.map((k) => (
            <option key={k} value={k}>
              {t(`agents.toolKind.${k}`)}
            </option>
          ))}
        </select>
        <select
          className="rounded-md border border-white/10 bg-card px-2 py-1 text-sm"
          value={tool.approval}
          onChange={(e) => onChange({ approval: e.target.value as ApprovalMode })}
        >
          <option value="auto">{t('agents.approvalAuto')}</option>
          <option value="require_approval">{t('agents.approvalRequire')}</option>
        </select>
        <button onClick={onRemove} className="text-muted-foreground hover:text-red-400">
          <Trash2 className="h-3.5 w-3.5" />
        </button>
      </div>

      {(tool.tool.kind === 'search_vectors' || tool.tool.kind === 'run_pipeline') && (
        <Input
          className="mt-2"
          placeholder="pipeline_id"
          value={tool.tool.pipeline_id}
          onChange={(e) =>
            onChange({ tool: { ...tool.tool, pipeline_id: e.target.value } as AgentToolKind })
          }
        />
      )}
      {tool.tool.kind === 'call_webhook' && (
        <Input
          className="mt-2"
          placeholder="https://..."
          value={tool.tool.url}
          onChange={(e) => onChange({ tool: { ...tool.tool, url: e.target.value } as AgentToolKind })}
        />
      )}
      {'source' in tool.tool && (
        <Input
          className="mt-2"
          placeholder="connector (e.g. postgres)"
          value={tool.tool.source.connector}
          onChange={(e) => {
            if (!('source' in tool.tool)) return
            onChange({
              tool: {
                ...tool.tool,
                source: { ...tool.tool.source, connector: e.target.value },
              } as AgentToolKind,
            })
          }}
        />
      )}
    </div>
  )
}

export default AgentsPanel
