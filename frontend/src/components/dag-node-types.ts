import {
  ConnectorNodeView,
  DbtNodeView,
  EmbeddingNodeView,
  PythonNodeView,
  TransformNodeView,
} from '@/components/dag-nodes'

export const dagNodeTypes = {
  connector: ConnectorNodeView,
  transform: TransformNodeView,
  dbt: DbtNodeView,
  embedding: EmbeddingNodeView,
  python: PythonNodeView,
}
