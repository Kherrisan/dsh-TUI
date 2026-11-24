/**
 * The `--connect` backend: detect a Web handoff, resume one existing Host
 * session, list the Host's sessions for the browser, and create a session on
 * the Host when the user asks for a new one. Creation always uses a
 * Host-known directory (the picked session's cwd, or a Host workspace): the
 * TUI's own cwd is a different machine's path and is never sent.
 */
import type { AgentBackend, BackendDetection, BackendHost, OpenTarget } from '../../agent/backend.js'
import type { SessionSummary } from '../../agent/index.js'
import type { AgentSession } from '../../agent/session.js'
import { REMOTE_BACKEND_ID, REMOTE_BACKEND_LABEL, readConnectHandoff } from './contract.js'
import { RemoteEndpoint } from './protocol.js'
import { RemoteDshSession } from './session.js'

interface ListValue {
  readonly items?: readonly Record<string, unknown>[]
}

interface MemoryPrefs {
  last: string | undefined
}

const memoryPrefs = new Map<string, MemoryPrefs>()

const titleOf = (item: Record<string, unknown>): string => {
  const projections = item.projections
  if (projections === null || typeof projections !== 'object' || Array.isArray(projections)) return ''
  const values = (projections as { values?: unknown }).values
  if (values === null || typeof values !== 'object' || Array.isArray(values)) return ''
  const title = (values as { title?: unknown }).title
  return typeof title === 'string' ? title : ''
}

const valueOf = (item: Record<string, unknown>, key: string): unknown => {
  const projections = item.projections
  if (projections === null || typeof projections !== 'object' || Array.isArray(projections)) return undefined
  const values = (projections as { values?: unknown }).values
  if (values === null || typeof values !== 'object' || Array.isArray(values)) return undefined
  return (values as Record<string, unknown>)[key]
}

const modelOf = (item: Record<string, unknown>): string | undefined => {
  const selection = valueOf(item, 'modelSelection')
  if (selection === null || typeof selection !== 'object' || Array.isArray(selection)) return undefined
  const used = (selection as { lastUsed?: unknown }).lastUsed
  if (used === null || typeof used !== 'object' || Array.isArray(used)) return undefined
  const model = (used as { model?: unknown }).model
  return typeof model === 'string' ? model : undefined
}

const summaryOf = (item: Record<string, unknown>): SessionSummary => {
  const id = typeof item.sessionId === 'string' ? item.sessionId : ''
  const cwd = typeof item.cwd === 'string' ? item.cwd : ''
  const title = titleOf(item)
  const parentSessionId = typeof item.parentSessionId === 'string' ? item.parentSessionId : undefined
  const origin = item.origin === 'subagent'
  const agentPreset = valueOf(item, 'agentPreset')
  return {
    id,
    backendId: REMOTE_BACKEND_ID,
    kind: origin
      ? { kind: 'subagent', parent: parentSessionId, depth: 1 }
      : parentSessionId === undefined ? { kind: 'root' } : { kind: 'fork', parent: parentSessionId },
    title: { text: title, source: title === '' ? 'fallback' : 'auto' },
    cwd,
    createdAt: 0,
    updatedAt: typeof item.updatedAt === 'number' ? item.updatedAt : 0,
    bytes: undefined,
    hasPrompt: item.blank !== true,
    agentPreset: typeof agentPreset === 'string' ? agentPreset : undefined,
    model: modelOf(item),
    label: undefined,
    branch: undefined,
    childCount: 0,
  }
}

/** Open one mux generation, read a stream's first frame, and close it. */
async function firstFrame(endpoint: RemoteEndpoint, endpointName: string, payload: unknown): Promise<unknown> {
  const mux = await endpoint.openMux()
  const streamId = 'oneshot-1'
  try {
    const stream = mux.openStream(streamId, endpointName, payload)
    for await (const frame of stream) return frame
    return undefined
  } finally {
    mux.close()
  }
}

/** The Host's Workspace directories, in the Host's own order. */
async function hostWorkspaces(endpoint: RemoteEndpoint): Promise<readonly string[]> {
  try {
    const frame = await firstFrame(endpoint, 'workspace/follow', {}) as { type?: unknown; value?: { items?: unknown } } | undefined
    if (frame?.type !== 'baseline') return []
    const items = Array.isArray(frame.value?.items) ? frame.value.items : []
    return items.flatMap((item): string[] => {
      if (item === null || typeof item !== 'object') return []
      const path = (item as { path?: unknown }).path
      return typeof path === 'string' && path !== '' ? [path] : []
    })
  } catch {
    return []
  }
}

/**
 * The directory a new Host session may be created in: the entry's explicit
 * Host path first, then a directory the Host itself already accounts for
 * (its newest session or a Workspace). Anything else would be this machine's
 * path, so an empty answer refuses the creation instead of guessing.
 */
async function hostCreateCwd(endpoint: RemoteEndpoint, handoff: { readonly cwd?: string }): Promise<string | undefined> {
  if (handoff.cwd !== undefined) return handoff.cwd
  const rows = await listRemote()
  const fromSession = rows.find(row => row.cwd !== '')?.cwd
  if (fromSession !== undefined) return fromSession
  const [workspace] = await hostWorkspaces(endpoint)
  if (workspace !== undefined) return workspace
  // A virgin Host has no session and no Workspace yet. Its own first-use path
  // registers a default Workspace (documents directory, fixed name, no Session
  // and no message); asking for a new session is exactly that first use, and
  // the call is a no-op once such a Workspace exists.
  try {
    const initialized = await endpoint.rpc<{ workspace?: { path?: unknown } } | undefined>(
      'workspace/initializeDefault',
      {},
      { retryAuth: true },
    )
    const path = initialized?.workspace?.path
    return typeof path === 'string' && path !== '' ? path : undefined
  } catch {
    return undefined
  }
}

/** List one Host generation without keeping a socket. */
async function listRemote(): Promise<readonly SessionSummary[]> {
  const handoff = readConnectHandoff()
  if (handoff === undefined) return []
  const endpoint = new RemoteEndpoint(handoff.baseUrl, handoff.token)
  const value = await endpoint.rpc<ListValue>('session/list', { _request: {} }, { retryAuth: true })
  const items = Array.isArray(value.items) ? value.items : []
  return items.flatMap((item): SessionSummary[] => {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) return []
    return [summaryOf(item as Record<string, unknown>)]
  })
}

export const remoteBackend: AgentBackend = {
  id: REMOTE_BACKEND_ID,
  descriptor: { label: REMOTE_BACKEND_LABEL },
  async detect(): Promise<BackendDetection> {
    return readConnectHandoff() === undefined
      ? { installed: false, auth: 'missing', hint: 'run `dsh-tui --connect` and paste the Web URL' }
      : { installed: true, auth: 'ok' }
  },
  async open(target: OpenTarget, host: BackendHost): Promise<AgentSession> {
    const handoff = readConnectHandoff()
    if (handoff === undefined) throw new Error('remote mode was not initialised: start with `dsh-tui --connect`')
    const endpoint = new RemoteEndpoint(handoff.baseUrl, handoff.token)
    if (target.kind === 'resume') {
      try {
        return await RemoteDshSession.open(target.sessionId, endpoint, host)
      } catch (error) {
        throw new Error(`could not attach to remote session "${target.sessionId}": ${error instanceof Error ? error.message : String(error)}`, { cause: error })
      }
    }
    // `target.cwd` is THIS machine's directory (the channel passes its own
    // cwd): it is never sent to the Host.
    const cwd = await hostCreateCwd(endpoint, handoff)
    if (cwd === undefined) {
      throw new Error('the Host names no workspace or existing session directory, so there is nowhere on the Host to create a session; create one in the Web UI first, then attach to it')
    }
    const created = await endpoint.rpc<{ sessionId?: unknown }>('session/create', { request: { cwd } }, { retryAuth: true })
    const sessionId = typeof created.sessionId === 'string' ? created.sessionId : undefined
    if (sessionId === undefined) throw new Error('the Host did not return the created session id')
    try {
      return await RemoteDshSession.open(sessionId, endpoint, host)
    } catch (error) {
      throw new Error(`the Host created session "${sessionId}" but attaching to it failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error })
    }
  },
  catalog: {
    list: async () => listRemote(),
    async info(sessionId: string): Promise<SessionSummary | undefined> {
      const rows = await listRemote()
      return rows.find(row => row.id === sessionId)
    },
    async rename(sessionId: string, title: string): Promise<void> {
      const handoff = readConnectHandoff()
      if (handoff === undefined) throw new Error('remote mode was not initialised')
      const endpoint = new RemoteEndpoint(handoff.baseUrl, handoff.token)
      await endpoint.rpc<unknown>('session/rename', { request: { sessionId, title } }, { retryAuth: true })
    },
    snapshotKey: () => {
      const handoff = readConnectHandoff()
      return handoff === undefined ? undefined : `remote:${handoff.baseUrl}`
    },
  },
  launch: {
    sessionPrefs(host: BackendHost) {
      const key = `${host.cwd}:${readConnectHandoff()?.baseUrl ?? ''}`
      let prefs = memoryPrefs.get(key)
      if (prefs === undefined) {
        prefs = { last: undefined }
        memoryPrefs.set(key, prefs)
      }
      return {
        lastSession: () => prefs.last,
        setLastSession: sessionId => { prefs.last = sessionId },
        touch: sessionId => { prefs.last = sessionId },
        forget: sessionId => { if (prefs.last === sessionId) prefs.last = undefined },
      }
    },
    resumeCommand: sessionId => `dsh-tui --connect --resume ${sessionId}`,
  },
}
