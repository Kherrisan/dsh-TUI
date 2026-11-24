/**
 * `MentionFs` over a session's own file capability (`SessionCapabilities.files`):
 * the `@` completion, the fenced file queries and the read-only preview of a
 * session whose working directory belongs to another process (a remote Host).
 *
 * Display paths are the Host's absolute paths, built from the capability's
 * root; a path outside that root is a miss rather than a fallback to this
 * machine's disk.
 */
import type { SessionFiles } from '../../agent/capabilities.js'
import type { MentionFs } from './types.js'

/** Join a workspace-relative path onto the Host root, POSIX style. */
function join(root: string, path: string): string {
  if (path === '') return root
  return root.endsWith('/') ? `${root}${path}` : `${root}/${path}`
}

/** The relative path a display path stands for, or undefined outside the root. */
function relative(root: string, displayPath: string): string | undefined {
  if (displayPath === root) return ''
  const prefix = root.endsWith('/') ? root : `${root}/`
  if (!displayPath.startsWith(prefix)) return undefined
  return displayPath.slice(prefix.length).split('\\').join('/').replace(/^\.\//, '')
}

/** The channel's file surface for one session-scoped file provider. */
export function sessionFilesFs(files: SessionFiles | undefined): MentionFs | undefined {
  if (files === undefined) return undefined
  const root = files.root.replace(/\/+$/, '')
  const target = (displayPath: string): string => {
    const path = relative(root, displayPath.replace(/\/+$/, ''))
    if (path === undefined) throw new Error(`${displayPath} is outside this session's Host directory (${root})`)
    return path
  }
  return {
    async resolve(path) {
      const absolute = path.replace(/\/+$/, '')
      if (relative(root, absolute) === undefined) {
        throw new Error(`${absolute} is outside this session's Host directory (${root})`)
      }
      return { displayPath: absolute === '' ? root : absolute }
    },
    async stat(entry) {
      const type = await files.stat(target(entry.displayPath))
      return type === undefined ? undefined : { type: type.type }
    },
    async readText(entry) {
      const page = await files.readText(target(entry.displayPath))
      return page.text
    },
    async listDir(entry) {
      const listing = await files.list(target(entry.displayPath))
      return listing.entries.map(child => ({ name: child.name, type: child.type }))
    },
  }
}
