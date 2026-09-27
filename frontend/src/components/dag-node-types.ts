import {
  CleanBlockNodeView,
  ConnectorNodeView,
  DbtNodeView,
  EmbeddingNodeView,
  PythonNodeView,
  TransformNodeView,
  VisualizationNodeView,
} from '@/components/dag-nodes'

export const dagNodeTypes = {
  connector: ConnectorNodeView,
  transform: TransformNodeView,
  dbt: DbtNodeView,
  embedding: EmbeddingNodeView,
  python: PythonNodeView,
  visualization: VisualizationNodeView,
  clean: CleanBlockNodeView,
}
