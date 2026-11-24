/** Remote DSH Web backend identity and the process-global attach handoff. */
export const REMOTE_BACKEND_ID = 'remote'
export const REMOTE_BACKEND_LABEL = 'Remote Web'

/**
 * The connect handoff the entry fills after reading the startup link. It lives
 * on the process global (never in argv or an environment variable) because it
 * carries the launch token. The backend is a separate import island, so it
 * reads the same symbol instead of sharing a module with the entry.
 */
export const CONNECT_HANDOFF = Symbol.for('dsh-tui.remote-connect')

export interface ConnectHandoff {
  /** Same-origin base URL, e.g. `http://127.0.0.1:32871`. */
  readonly baseUrl: string
  readonly token: string
  /** Informational: the session the entry picked before mounting the screen. */
  readonly sessionId?: string
  /** A Host-known directory (the picked session's own cwd, or the Host
   *  workspace the entry created in). The TUI's local cwd is never this. */
  readonly cwd?: string
}

/** Read the handoff, or undefined when this is not a `--connect` launch. */
export function readConnectHandoff(): ConnectHandoff | undefined {
  const value: unknown = (globalThis as Record<symbol, unknown>)[CONNECT_HANDOFF]
  if (value === null || typeof value !== 'object') return undefined
  const baseUrl = (value as { baseUrl?: unknown }).baseUrl
  const token = (value as { token?: unknown }).token
  const sessionId = (value as { sessionId?: unknown }).sessionId
  const cwd = (value as { cwd?: unknown }).cwd
  if (typeof baseUrl !== 'string' || typeof token !== 'string' || baseUrl === '' || token === '') return undefined
  return {
    baseUrl,
    token,
    ...(typeof sessionId === 'string' && sessionId !== '' ? { sessionId } : {}),
    ...(typeof cwd === 'string' && cwd !== '' ? { cwd } : {}),
  }
}
