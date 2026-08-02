import { ConnectorNodeView, DbtNodeView, TransformNodeView } from '@/components/dag-nodes'

export const dagNodeTypes = {
  connector: ConnectorNodeView,
  transform: TransformNodeView,
  dbt: DbtNodeView,
}
