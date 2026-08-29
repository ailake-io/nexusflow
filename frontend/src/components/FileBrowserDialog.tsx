import { useEffect, useState } from 'react'
import { Folder, File as FileIcon, ChevronRight } from 'lucide-react'
import { useI18n } from '@/lib/i18n'
import { useAuth } from '@/lib/auth-context'
import { browseFilesystem, type BrowseEntry, ApiError } from '@/lib/api'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'

interface FileBrowserDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Starting directory — the field's current value if it looks like a
   *  path, otherwise `/`. */
  initialPath?: string
  onSelect: (path: string) => void
}

/**
 * Modal directory browser backing every file-based connector's "Browse…"
 * button (see `SchemaForm.tsx`) — lists a server-side directory via
 * `GET /system/browse-fs`, lets the user navigate into subfolders or pick a
 * file, and (since a source can now point `path` straight at a directory —
 * `nexus-connector-csv`/`nexus-connector-parquet`'s multi-file read) also
 * offers "Select this folder" for the current directory itself.
 */
export function FileBrowserDialog({ open, onOpenChange, initialPath, onSelect }: FileBrowserDialogProps) {
  const { t } = useI18n()
  const { token } = useAuth()
  const [path, setPath] = useState('/')
  const [entries, setEntries] = useState<BrowseEntry[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!open) return
    const start = initialPath && initialPath.startsWith('/') ? initialPath : '/'
    setPath(start)
  }, [open, initialPath])

  useEffect(() => {
    if (!open || !token) return
    let cancelled = false
    setLoading(true)
    setError(null)

    const load = async () => {
      try {
        return await browseFilesystem(token, path)
      } catch (e) {
        // `path` may still be pointing at a *file* — e.g. reopening the
        // dialog on a field whose current value is the file picked last
        // time. The server correctly rejects listing a non-directory;
        // fall back to its parent once instead of surfacing that as an
        // error the user has to click past.
        const parent = path.replace(/\/[^/]*\/?$/, '') || '/'
        if (parent === path) throw e
        return await browseFilesystem(token, parent)
      }
    }

    load()
      .then((listing) => {
        if (cancelled) return
        setPath(listing.path)
        setEntries(listing.entries)
      })
      .catch((e) => {
        if (cancelled) return
        setError(e instanceof ApiError ? e.message : t('fileBrowser.loadError'))
        setEntries([])
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, token, path])

  const segments = path.split('/').filter(Boolean)
  const goTo = (index: number) => setPath('/' + segments.slice(0, index + 1).join('/'))

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{t('fileBrowser.title')}</DialogTitle>
        </DialogHeader>

        <div className="flex flex-wrap items-center gap-1 rounded-md bg-muted/50 px-2 py-1.5 text-xs text-muted-foreground">
          <button type="button" onClick={() => setPath('/')} className="hover:text-foreground hover:underline">
            /
          </button>
          {segments.map((segment, index) => (
            <span key={index} className="flex items-center gap-1">
              <ChevronRight className="h-3 w-3" />
              <button
                type="button"
                onClick={() => goTo(index)}
                className="hover:text-foreground hover:underline"
              >
                {segment}
              </button>
            </span>
          ))}
        </div>

        <div className="max-h-80 overflow-y-auto rounded-md border border-white/10">
          {loading && (
            <div className="p-4 text-sm text-muted-foreground">{t('common.loading')}</div>
          )}
          {!loading && error && <div className="p-4 text-sm text-red-400">{error}</div>}
          {!loading && !error && entries.length === 0 && (
            <div className="p-4 text-sm text-muted-foreground">{t('fileBrowser.empty')}</div>
          )}
          {!loading &&
            !error &&
            entries.map((entry) => (
              <button
                key={entry.name}
                type="button"
                onClick={() => {
                  if (entry.is_dir) {
                    setPath(path === '/' ? `/${entry.name}` : `${path}/${entry.name}`)
                  } else {
                    onSelect(path === '/' ? `/${entry.name}` : `${path}/${entry.name}`)
                    onOpenChange(false)
                  }
                }}
                className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-sm hover:bg-white/5"
              >
                {entry.is_dir ? (
                  <Folder className="h-4 w-4 shrink-0 text-primary" />
                ) : (
                  <FileIcon className="h-4 w-4 shrink-0 text-muted-foreground" />
                )}
                <span className="flex-1 truncate">{entry.name}</span>
                {!entry.is_dir && entry.size !== null && (
                  <span className="shrink-0 text-xs text-muted-foreground">{formatSize(entry.size)}</span>
                )}
              </button>
            ))}
        </div>

        <DialogFooter>
          <Button
            type="button"
            onClick={() => {
              onSelect(path)
              onOpenChange(false)
            }}
          >
            {t('fileBrowser.selectFolder')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}
