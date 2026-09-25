import { useState } from 'react'
import { Eye, Loader2, AlertCircle } from 'lucide-react'
import { useAuth } from '@/lib/auth-context'
import { useI18n } from '@/lib/i18n'
import { ApiError, previewCleanBlocks, type PreviewResult } from '@/lib/api'
import type { NodeSpec, CleanBlockSpec } from '@/lib/dag'
import { Button } from '@/components/ui/button'

interface CleanBlockPreviewProps {
  /** The connected source node's `{connector, config}` — `undefined` when
   * no source is wired up yet (nothing to sample from). */
  source: NodeSpec | undefined
  /** Every clean block up to and including the one being edited, in
   * canvas left-to-right order — matches what `POST
   * /pipelines/preview-clean-blocks` compiles server-side. `null` means
   * the current chain doesn't convert to valid blocks yet (a required
   * field is still empty) — same class of error `toPipelineSpec` would
   * raise on save, shown here instead of crashing the panel. */
  blocks: CleanBlockSpec[] | null
}

/**
 * Inline "Ver amostra" button on a clean block's inspector panel — same
 * mechanism and look as `NodePreview` (connector nodes), but samples the
 * *connected source* and runs the compiled block chain up to this node
 * over it (`POST /pipelines/preview-clean-blocks`, no pipeline save
 * required). Fetches on click, not on every keystroke, for the same
 * reason `NodePreview` does.
 */
export function CleanBlockPreview({ source, blocks }: CleanBlockPreviewProps) {
  const { token } = useAuth()
  const { t } = useI18n()
  const [result, setResult] = useState<PreviewResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const disabled = !token || !source || !blocks || blocks.length === 0

  const handlePreview = async () => {
    if (!token || !source || !blocks || blocks.length === 0) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      setResult(await previewCleanBlocks(token, source, blocks, 20))
    } catch (err: unknown) {
      setError(err instanceof ApiError ? err.message : t('canvas.previewError'))
    } finally {
      setLoading(false)
    }
  }

  const columns = result && result.rows.length > 0 ? Object.keys(result.rows[0]) : []

  return (
    <div className="border-t border-white/10 pt-4">
      {!source && (
        <p className="mb-2 text-[11px] text-muted-foreground">{t('canvas.clean.previewNoSource')}</p>
      )}
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="w-full"
        disabled={disabled || loading}
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
                  <th
                    key={c}
                    className="whitespace-nowrap border-b border-white/10 px-2 py-1 font-medium text-muted-foreground"
                  >
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

export default CleanBlockPreview
