import { describe, expect, it } from 'vitest'
import {
  DEFAULT_CLEAN_DATA,
  fromPipelineSpec,
  toPipelineSpec,
  type CleanBlockNodeData,
  type ConnectorNodeData,
  type DagNode,
  type EmbeddingNodeData,
  type PipelineMeta,
} from './dag'

function sourceNode(overrides: Partial<ConnectorNodeData> = {}): DagNode {
  return {
    id: 'n1',
    type: 'connector',
    position: { x: 0, y: 0 },
    data: {
      kind: 'connector',
      connector: 'postgres',
      role: 'source',
      name: '',
      config: '{"table": "src"}',
      ...overrides,
    },
  }
}

function sinkNode(overrides: Partial<ConnectorNodeData> = {}): DagNode {
  return {
    id: 'n2',
    type: 'connector',
    position: { x: 0, y: 0 },
    data: {
      kind: 'connector',
      connector: 'sqlite',
      role: 'sink',
      name: '',
      config: '{"path": "out.db"}',
      ...overrides,
    },
  }
}

function transformNode(sql: string): DagNode {
  return {
    id: 'n3',
    type: 'transform',
    position: { x: 0, y: 0 },
    data: { kind: 'transform', sql },
  }
}

function embeddingNode(overrides: Partial<EmbeddingNodeData> = {}): DagNode {
  return {
    id: 'n4',
    type: 'embedding',
    position: { x: 0, y: 0 },
    data: {
      kind: 'embedding',
      sourceColumn: 'text',
      outputColumn: 'embedding',
      dimension: 384,
      backend: 'onnx',
      repo: 'sentence-transformers/all-MiniLM-L6-v2',
      revision: 'main',
      filename: 'model.onnx',
      tokenizerFilename: 'tokenizer.json',
      maxLength: 128,
      baseUrl: '',
      model: '',
      apiKeyEnv: '',
      strategy: 'fixed_window',
      chunkSize: 256,
      overlap: 0,
      similarityThreshold: 0.8,
      separators: '',
      ...overrides,
    },
  }
}

function cleanNode(
  id: string,
  x: number,
  overrides: Partial<CleanBlockNodeData> = {},
): DagNode {
  return {
    id,
    type: 'clean',
    position: { x, y: 0 },
    data: { ...DEFAULT_CLEAN_DATA, ...overrides },
  }
}

const meta: PipelineMeta = { pipelineId: 'p1' }

describe('toPipelineSpec', () => {
  it('serializes a linear source->sink pipeline', () => {
    const spec = toPipelineSpec([sourceNode(), sinkNode()], meta)
    expect(spec.pipeline_id).toBe('p1')
    expect(spec.sources).toHaveLength(1)
    expect(spec.sinks).toHaveLength(1)
    expect(spec.sources[0].connector).toBe('postgres')
    expect(spec.sinks[0].connector).toBe('sqlite')
    expect(spec.transform).toBeUndefined()
  })

  it('rejects empty pipeline_id', () => {
    expect(() => toPipelineSpec([sourceNode(), sinkNode()], { pipelineId: '  ' })).toThrow(
      'pipeline_id must not be empty',
    )
  })

  it('rejects missing sources or sinks', () => {
    expect(() => toPipelineSpec([sourceNode()], meta)).toThrow('sinks must not be empty')
    expect(() => toPipelineSpec([sinkNode()], meta)).toThrow('sources must not be empty')
  })

  it('rejects multiple sources/sinks without a transform', () => {
    expect(() => toPipelineSpec([sourceNode(), sourceNode(), sinkNode()], meta)).toThrow(
      'without a transform',
    )
  })

  it('allows fan-in/fan-out with a transform', () => {
    const spec = toPipelineSpec(
      [sourceNode(), sourceNode(), transformNode('SELECT * FROM source'), sinkNode(), sinkNode()],
      meta,
    )
    expect(spec.sources).toHaveLength(2)
    expect(spec.sinks).toHaveLength(2)
    expect(spec.transform?.sql).toBe('SELECT * FROM source')
  })

  it('rejects empty transform sql', () => {
    expect(() => toPipelineSpec([sourceNode(), transformNode('  '), sinkNode()], meta)).toThrow(
      'transform.sql must not be empty',
    )
  })

  it('serializes embedding node with onnx backend', () => {
    const spec = toPipelineSpec(
      [sourceNode(), embeddingNode(), transformNode('SELECT * FROM source'), sinkNode()],
      meta,
    )
    expect(spec.embedding).toBeDefined()
    expect(spec.embedding?.source_column).toBe('text')
    expect(spec.embedding?.model.backend).toBe('onnx')
    expect(spec.embedding?.chunking.strategy).toBe('fixed_window')
  })

  it('serializes embedding node with api backend', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        embeddingNode({
          backend: 'api',
          baseUrl: 'http://localhost:8000',
          model: 'text-embedding-3-small',
          apiKeyEnv: 'OPENAI_API_KEY',
        }),
        transformNode('SELECT * FROM source'),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.embedding?.model.backend).toBe('api')
    expect(spec.embedding?.model).toMatchObject({
      base_url: 'http://localhost:8000',
      model: 'text-embedding-3-small',
      api_key_env: 'OPENAI_API_KEY',
    })
  })

  it('rejects invalid connector config JSON', () => {
    expect(() =>
      toPipelineSpec(
        [sourceNode({ config: 'not json' }), sinkNode()],
        meta,
      ),
    ).toThrow('config is not valid JSON')
  })

  it('allows drafts to skip validation', () => {
    const spec = toPipelineSpec([], { pipelineId: 'draft' }, true)
    expect(spec.draft).toBe(true)
    expect(spec.sources).toHaveLength(0)
    expect(spec.sinks).toHaveLength(0)
  })
})

describe('fromPipelineSpec', () => {
  it('round-trips a linear pipeline', () => {
    const original = toPipelineSpec([sourceNode(), sinkNode()], meta)
    const { nodes, edges } = fromPipelineSpec(original)
    expect(nodes).toHaveLength(2)
    expect(edges).toHaveLength(1)
    const roundTrip = toPipelineSpec(nodes, meta)
    expect(roundTrip.sources).toEqual(original.sources)
    expect(roundTrip.sinks).toEqual(original.sinks)
  })

  it('round-trips a transform pipeline', () => {
    const original = toPipelineSpec(
      [sourceNode(), transformNode('SELECT 1'), sinkNode()],
      meta,
    )
    const { nodes } = fromPipelineSpec(original)
    const roundTrip = toPipelineSpec(nodes, meta)
    expect(roundTrip.transform?.sql).toBe('SELECT 1')
  })

  it('round-trips an embedding node', () => {
    const original = toPipelineSpec(
      [sourceNode(), embeddingNode(), transformNode('SELECT * FROM source'), sinkNode()],
      meta,
    )
    const { nodes } = fromPipelineSpec(original)
    const roundTrip = toPipelineSpec(nodes, meta)
    expect(roundTrip.embedding).toEqual(original.embedding)
  })
})

describe('toPipelineSpec — clean blocks (Fase 30)', () => {
  it('serializes a single filter block', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, {
          blockKind: 'filter',
          column: 'valor',
          operator: 'gt',
          value: '10',
        }),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.clean_blocks).toEqual([
      { kind: 'filter', column: 'valor', operator: 'gt', value: '10' },
    ])
    expect(spec.transform).toBeUndefined()
  })

  it('orders blocks by canvas x position, not array/insertion order', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('later', 400, { blockKind: 'sort', column: 'id', direction: 'asc' }),
        cleanNode('earlier', 100, { blockKind: 'trim', columns: 'nome' }),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.clean_blocks?.map((b) => b.kind)).toEqual(['trim', 'sort'])
  })

  it('rejects combining clean blocks with a transform node', () => {
    expect(() =>
      toPipelineSpec(
        [sourceNode(), transformNode('SELECT 1'), cleanNode('c1', 100), sinkNode()],
        meta,
      ),
    ).toThrow('cannot be combined')
  })

  it('rejects combining clean blocks with a python node', () => {
    const pythonNode: DagNode = {
      id: 'py',
      type: 'python',
      position: { x: 0, y: 0 },
      data: { kind: 'python', script: 'def transform(df):\n    return df', timeoutSeconds: 0 },
    }
    expect(() =>
      toPipelineSpec([sourceNode(), pythonNode, cleanNode('c1', 100), sinkNode()], meta),
    ).toThrow('cannot be combined')
  })

  it('rejects clean blocks with more than 1 source', () => {
    expect(() =>
      toPipelineSpec([sourceNode(), sourceNode(), cleanNode('c1', 100), sinkNode()], meta),
    ).toThrow('exactly 1 source')
  })

  it('allows clean blocks to fan out to multiple sinks', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, { blockKind: 'sort', column: 'id', direction: 'asc' }),
        sinkNode(),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.sinks).toHaveLength(2)
  })

  it('rejects a filter block missing its column', () => {
    expect(() =>
      toPipelineSpec(
        [sourceNode(), cleanNode('c1', 100, { blockKind: 'filter' }), sinkNode()],
        meta,
      ),
    ).toThrow('column must not be empty')
  })

  it('rejects a comparison filter with no value', () => {
    expect(() =>
      toPipelineSpec(
        [
          sourceNode(),
          cleanNode('c1', 100, { blockKind: 'filter', column: 'id', operator: 'eq' }),
          sinkNode(),
        ],
        meta,
      ),
    ).toThrow('requires a value')
  })

  it('is_null/is_not_null filters need no value', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, { blockKind: 'filter', column: 'id', operator: 'is_null' }),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.clean_blocks?.[0]).toEqual({ kind: 'filter', column: 'id', operator: 'is_null' })
  })

  it('parses the group_by/aggregations mini-DSL', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, {
          blockKind: 'aggregate',
          groupBy: 'cidade',
          aggregations: 'valor:sum:total_valor\nid:count:total_linhas',
        }),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.clean_blocks?.[0]).toEqual({
      kind: 'aggregate',
      group_by: ['cidade'],
      aggregations: [
        { column: 'valor', function: 'sum', output: 'total_valor' },
        { column: 'id', function: 'count', output: 'total_linhas' },
      ],
    })
  })

  it('rejects a malformed aggregation line', () => {
    expect(() =>
      toPipelineSpec(
        [
          sourceNode(),
          cleanNode('c1', 100, { blockKind: 'aggregate', aggregations: 'not-a-valid-line' }),
          sinkNode(),
        ],
        meta,
      ),
    ).toThrow('column:function:output')
  })

  it('fill_nulls with other_column strategy uses fallback_column, not column', () => {
    const spec = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, {
          blockKind: 'fill_nulls',
          column: 'nome',
          nullStrategy: 'other_column',
          fallbackColumn: 'cidade',
        }),
        sinkNode(),
      ],
      meta,
    )
    expect(spec.clean_blocks?.[0]).toEqual({
      kind: 'fill_nulls',
      column: 'nome',
      strategy: 'other_column',
      fallback_column: 'cidade',
    })
  })
})

describe('fromPipelineSpec — clean blocks (Fase 30)', () => {
  it('round-trips a chain of clean blocks, preserving order', () => {
    const original = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, { blockKind: 'trim', columns: 'nome' }),
        cleanNode('c2', 200, { blockKind: 'sort', column: 'id', direction: 'asc' }),
        sinkNode(),
      ],
      meta,
    )
    const { nodes, edges } = fromPipelineSpec(original)
    const cleanNodes = nodes.filter((n) => n.data.kind === 'clean')
    expect(cleanNodes).toHaveLength(2)
    // Re-serializing must reproduce the exact same order/content.
    const roundTrip = toPipelineSpec(nodes, meta)
    expect(roundTrip.clean_blocks).toEqual(original.clean_blocks)
    // source -> block1 -> block2 -> sink, no edge skips the chain.
    expect(edges).toHaveLength(3)
  })

  it('round-trips an aggregate block', () => {
    const original = toPipelineSpec(
      [
        sourceNode(),
        cleanNode('c1', 100, {
          blockKind: 'aggregate',
          groupBy: 'cidade',
          aggregations: 'valor:sum:total',
        }),
        sinkNode(),
      ],
      meta,
    )
    const { nodes } = fromPipelineSpec(original)
    const roundTrip = toPipelineSpec(nodes, meta)
    expect(roundTrip.clean_blocks).toEqual(original.clean_blocks)
  })
})
