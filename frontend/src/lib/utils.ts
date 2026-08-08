import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** Human-readable duration between two ISO timestamps (e.g. "2m 34s"),
 * or `null` when the run hasn't finished yet (`finishedAt` is `null`) or
 * the timestamps are malformed — callers decide how to label that case
 * (e.g. "still running" vs "—"). */
export function formatDuration(startedAt: string, finishedAt: string | null): string | null {
  if (!finishedAt) return null
  const startMs = new Date(startedAt).getTime()
  const endMs = new Date(finishedAt).getTime()
  const deltaMs = endMs - startMs
  if (!Number.isFinite(deltaMs) || deltaMs < 0) return null

  const totalSeconds = Math.round(deltaMs / 1000)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60

  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  if (totalSeconds > 0) return `${totalSeconds}s`
  return '<1s'
}
