import { useCallback, useState, type DragEvent } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  addEdge,
  applyNodeChanges,
  applyEdgeChanges,
  useReactFlow,
  type Edge,
  type OnConnect,
  type OnNodesChange,
  type OnEdgesChange,
  type OnSelectionChangeFunc,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Download, Loader2, AlertCircle, Play } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import { useTheme } from '@/lib/theme'
import { useAuth } from '@/lib/auth-context'
import { useInfraModules } from '@/hooks/useInfraModules'
import { InfraModulePalette } from '@/components/InfraModulePalette'
import { infraNodeTypes } from '@/components/infra-node-types'
import { SchemaForm } from '@/components/SchemaForm'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { generateInfra, type GeneratedFiles, type InfraModuleDescriptor } from '@/lib/api'
import { toInfraGraph, type InfraEdgeData, type InfraNode as InfraNodeType } from '@/lib/infra'

function newNodeId(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return `module-${crypto.randomUUID()}`
  return `module-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

function parseConfig(raw: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

/** Shown when this build has no Terraform module catalog linked in
 * (`GET /infra/modules` returns `[]` without the `infra-terraform`
 * feature). */
function InfraUnavailable() {
  const { t } = useI18n()
  return (
    <div className="flex h-full items-center justify-center p-6">
      <div className="max-w-md rounded-xl border border-white/10 bg-card p-6 text-center">
        <AlertCircle className="mx-auto h-8 w-8 text-muted-foreground" />
        <h2 className="mt-3 text-lg font-semibold text-foreground">{t('infra.unavailableTitle')}</h2>
        <p className="mt-1.5 text-xs text-muted-foreground">{t('infra.unavailableDesc')}</p>
      </div>
    </div>
  )
}

function ModuleInspector({
  node,
  modules,
  onConfigChange,
}: {
  node: InfraNodeType
  modules: InfraModuleDescriptor[]
  onConfigChange: (config: string) => void
}) {
  const descriptor = modules.find((m) => m.id === node.data.module)
  const schema = descriptor?.config_schema
  return (
    <div className="flex h-full w-80 shrink-0 flex-col overflow-auto border-l border-white/10 bg-card p-4">
      <h3 className="text-sm font-semibold text-foreground">{descriptor?.name ?? node.data.module}</h3>
      {schema ? (
        <div className="mt-3">
          <SchemaForm
            schema={schema}
            defs={schema.$defs ?? {}}
            idPrefix="infra-node-config-"
            value={parseConfig(node.data.config)}
            onChange={(next) => onConfigChange(JSON.stringify(next, null, 2))}
          />
        </div>
      ) : null}
    </div>
  )
}

function EdgeInspector({
  edge,
  modules,
  nodes,
  onChange,
}: {
  edge: Edge
  modules: InfraModuleDescriptor[]
  nodes: InfraNodeType[]
  onChange: (data: InfraEdgeData) => void
}) {
  const { t } = useI18n()
  const sourceNode = nodes.find((n) => n.id === edge.source)
  const sourceModule = modules.find((m) => m.id === sourceNode?.data.module)
  const data = (edge.data as InfraEdgeData | undefined) ?? { output: '', input: '' }

  return (
    <div className="flex h-full w-80 shrink-0 flex-col overflow-auto border-l border-white/10 bg-card p-4">
      <h3 className="text-sm font-semibold text-foreground">{t('infra.edgeMapping')}</h3>
      <p className="mt-1 text-xs text-muted-foreground">{t('infra.edgeMappingDesc')}</p>
      <div className="mt-3">
        <label className="text-xs font-medium text-foreground">{t('infra.output')}</label>
        <select
          value={data.output}
          onChange={(e) => onChange({ ...data, output: e.target.value })}
          className="mt-1 h-8 w-full rounded-md border border-input bg-transparent px-2 text-xs text-foreground"
        >
          <option value="">—</option>
          {(sourceModule?.outputs ?? []).map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </div>
      <div className="mt-3">
        <label className="text-xs font-medium text-foreground">{t('infra.input')}</label>
        <Input
          value={data.input}
          onChange={(e) => onChange({ ...data, input: e.target.value })}
          placeholder={t('infra.inputPlaceholder')}
          className="mt-1 text-xs"
        />
      </div>
    </div>
  )
}

function GeneratedFilesViewer({ files, onClose }: { files: GeneratedFiles; onClose: () => void }) {
  const names = Object.keys(files.files)
  const [active, setActive] = useState(names[0] ?? '')

  const download = (name: string) => {
    const blob = new Blob([files.files[name]], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = name
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="absolute inset-0 z-20 flex flex-col bg-background/95 backdrop-blur">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-2">
        <div className="flex gap-1 overflow-x-auto">
          {names.map((name) => (
            <button
              key={name}
              type="button"
              onClick={() => setActive(name)}
              className={`shrink-0 rounded-md px-2.5 py-1 text-xs font-medium ${
                active === name ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-white/5'
              }`}
            >
              {name}
            </button>
          ))}
        </div>
        <div className="flex gap-2">
          <Button size="sm" variant="outline" onClick={() => download(active)}>
            <Download className="h-3.5 w-3.5" />
          </Button>
          <Button size="sm" variant="outline" onClick={onClose}>
            ✕
          </Button>
        </div>
      </div>
      <pre className="flex-1 overflow-auto p-4 font-mono text-xs text-foreground">{files.files[active]}</pre>
    </div>
  )
}

function CanvasInner() {
  const { t } = useI18n()
  const { theme } = useTheme()
  const { token } = useAuth()
  const { modules, loading, error } = useInfraModules()
  const { screenToFlowPosition } = useReactFlow()

  const [nodes, setNodes] = useState<InfraNodeType[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null)
  const [generating, setGenerating] = useState(false)
  const [generateError, setGenerateError] = useState<string | null>(null)
  const [generated, setGenerated] = useState<GeneratedFiles | null>(null)
  const [region, setRegion] = useState('us-east-1')

  const onNodesChange: OnNodesChange<InfraNodeType> = useCallback(
    (changes) => setNodes((current) => applyNodeChanges(changes, current)),
    [],
  )
  const onEdgesChange: OnEdgesChange = useCallback(
    (changes) => setEdges((current) => applyEdgeChanges(changes, current)),
    [],
  )
  const onConnect: OnConnect = useCallback(
    (connection) =>
      setEdges((current) => addEdge({ ...connection, data: { output: '', input: '' } }, current)),
    [],
  )
  const onSelectionChange: OnSelectionChangeFunc = useCallback(({ nodes: n, edges: e }) => {
    setSelectedNodeId(n[0]?.id ?? null)
    setSelectedEdgeId(n[0] ? null : (e[0]?.id ?? null))
  }, [])

  const onDragOver = useCallback((event: DragEvent<HTMLDivElement>) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault()
      const moduleId = event.dataTransfer.getData('application/nexusflow-infra-module')
      if (!moduleId) return
      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      const id = newNodeId()
      setNodes((current) => [
        ...current,
        { id, type: 'module', position, data: { kind: 'module', module: moduleId, config: '{}' } },
      ])
    },
    [screenToFlowPosition],
  )

  const updateNodeConfig = useCallback((id: string, config: string) => {
    setNodes((current) => current.map((n) => (n.id === id ? { ...n, data: { ...n.data, config } } : n)))
  }, [])

  const updateEdgeData = useCallback((id: string, data: InfraEdgeData) => {
    setEdges((current) => current.map((e) => (e.id === id ? { ...e, data } : e)))
  }, [])

  const handleGenerate = useCallback(async () => {
    if (!token) return
    setGenerating(true)
    setGenerateError(null)
    try {
      const graph = toInfraGraph(nodes, edges, { aws: { region } })
      const files = await generateInfra(token, graph)
      setGenerated(files)
    } catch (err) {
      setGenerateError(err instanceof Error ? err.message : String(err))
    } finally {
      setGenerating(false)
    }
  }, [token, nodes, edges, region])

  const selectedNode = nodes.find((n) => n.id === selectedNodeId) ?? null
  const selectedEdge = edges.find((e) => e.id === selectedEdgeId) ?? null

  if (!loading && modules.length === 0) {
    return <InfraUnavailable />
  }

  return (
    <div className="flex h-full w-full flex-col">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-2">
        <div>
          <h1 className="text-sm font-semibold text-foreground">{t('infra.title')}</h1>
          <p className="text-xs text-muted-foreground">{t('infra.subtitle')}</p>
        </div>
        <div className="flex items-center gap-2">
          <label className="text-xs text-muted-foreground" htmlFor="infra-region">
            {t('infra.region')}
          </label>
          <Input
            id="infra-region"
            value={region}
            onChange={(e) => setRegion(e.target.value)}
            className="h-7 w-28 text-xs"
          />
          <Button size="sm" onClick={handleGenerate} disabled={generating || nodes.length === 0}>
            {generating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
            {t('infra.generate')}
          </Button>
        </div>
      </div>
      {generateError && (
        <div className="flex items-center gap-2 border-b border-red-500/20 bg-red-500/10 px-4 py-2 text-xs text-red-400">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {generateError}
        </div>
      )}
      <div className="relative flex flex-1 overflow-hidden">
        <InfraModulePalette modules={modules} loading={loading} error={error} />
        <div className="relative flex-1 bg-background" onDragOver={onDragOver} onDrop={onDrop}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={infraNodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onSelectionChange={onSelectionChange}
            colorMode={theme}
            fitView
            fitViewOptions={{ maxZoom: 1 }}
          >
            <Background gap={20} size={1} color="var(--flow-dots)" />
          </ReactFlow>
          {generated && <GeneratedFilesViewer files={generated} onClose={() => setGenerated(null)} />}
        </div>
        {selectedNode && (
          <ModuleInspector
            node={selectedNode}
            modules={modules}
            onConfigChange={(config) => updateNodeConfig(selectedNode.id, config)}
          />
        )}
        {selectedEdge && !selectedNode && (
          <EdgeInspector
            edge={selectedEdge}
            modules={modules}
            nodes={nodes}
            onChange={(data) => updateEdgeData(selectedEdge.id, data)}
          />
        )}
      </div>
    </div>
  )
}

/** `useReactFlow` needs a provider above it — same wrapping `DagCanvas.tsx`
 * uses, kept here so `App.tsx` doesn't need to know that. */
export function InfraCanvas() {
  return (
    <ReactFlowProvider>
      <CanvasInner />
    </ReactFlowProvider>
  )
}

export default InfraCanvas
