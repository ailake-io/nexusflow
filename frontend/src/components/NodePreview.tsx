import { useState } from 'react'
import { Eye, Loader2, AlertCircle } from 'lucide-react'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { ApiError, previewConnector, type PreviewResult } from '@/lib/api'
import { Button } from '@/components/ui/button'

interface NodePreviewProps {
  connector: string
  config: Record<string, unknown>
}

/**
 * Inline "Ver amostra" button on a connector node's inspector panel — a
 * source *or* sink can be previewed the moment its config is filled in,
 * no pipeline save required (`POST /connectors/preview`, see
 * `nexus-server/src/lib.rs::preview_adhoc_handler`). Fetches on click
 * rather than on every keystroke — a config still mid-edit (missing host,
 * bad JSON) would just produce a wall of transient errors otherwise.
 */
export function NodePreview({ connector, config }: NodePreviewProps) {
  const { token } = useAuth()
  const { t } = useI18n()
  const [result, setResult] = useState<PreviewResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handlePreview = async () => {
    if (!token) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      setResult(await previewConnector(token, connector, config, 20))
    } catch (err: unknown) {
      setError(err instanceof ApiError ? err.message : t('canvas.previewError'))
    } finally {
      setLoading(false)
    }
  }

  const columns = result && result.rows.length > 0 ? Object.keys(result.rows[0]) : []

  return (
    <div className="border-t border-white/10 pt-4">
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="w-full"
        disabled={loading}
        onClick={handlePreview}
      >
        {loading ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
        ) : (
          <Eye className="h-3.5 w-3.5" />
        )}
        {t('canvas.previewSample')}
      </Button>

      {error && (
        <div className="mt-2 flex items-start gap-1.5 rounded-md border border-red-500/20 bg-red-500/10 p-2 text-[11px] text-red-400">
          <AlertCircle className="mt-0.5 h-3 w-3 shrink-0" />
          <span className="break-words">{error}</span>
        </div>
      )}

      {result && result.rows.length === 0 && !error && (
        <p className="mt-2 text-[11px] text-muted-foreground">{t('canvas.previewEmpty')}</p>
      )}

      {result && result.rows.length > 0 && (
        <div className="mt-2 max-h-64 overflow-auto rounded-md border border-white/10">
          <table className="w-full text-left text-[11px]">
            <thead className="sticky top-0 bg-card">
              <tr>
                {columns.map((c) => (
                  <th key={c} className="whitespace-nowrap border-b border-white/10 px-2 py-1 font-medium text-muted-foreground">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, i) => (
                <tr key={i} className="border-b border-white/5 last:border-0">
                  {columns.map((c) => (
                    <td key={c} className="whitespace-nowrap px-2 py-1 font-mono text-foreground">
                      {row[c] === null || row[c] === undefined ? (
                        <span className="text-muted-foreground">null</span>
                      ) : (
                        String(row[c])
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default NodePreview
