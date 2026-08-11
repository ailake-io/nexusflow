import {
  ConnectorNodeView,
  DbtNodeView,
  EmbeddingNodeView,
  TransformNodeView,
} from '@/components/dag-nodes'

export const dagNodeTypes = {
  connector: ConnectorNodeView,
  transform: TransformNodeView,
  dbt: DbtNodeView,
  embedding: EmbeddingNodeView,
}
