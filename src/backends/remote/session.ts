/**
 * One live session on a remote DSH Web Host. The Host owns the Agent, tools,
 * persistence and policy; this handle only mirrors its durable log and forwards
 * user intent. Disposal therefore closes the socket and never cancels a turn.
 */
import type { AgentEvent, AgentEventMeta, CommandInfo, PermissionOutcome, PermissionRequestView, QuestionItemView, QuestionRequestView } from '../../agent/events.js'
import type { AgentSession, AgentSessionStatus, AgentInput, CancelCause, CancelReceipt, SubmitPlacement } from '../../agent/session.js'
import type { CommandRunOutcome, EffortOption, HostTerminalEnvironment, HostTerminalInfo, ModelOption, ModelRef, ModelSwitchOutcome, PermissionDecision, QuestionAnswers, QueueMutationOutcome, SessionCapabilities, SubagentTranscriptPage, SubagentTranscriptWindow } from '../../agent/capabilities.js'
import type { BackendHost } from '../../agent/backend.js'
import type { AgentSessionRef } from '../../agent/refs.js'
import { RemoteConnectionLost, RemoteEndpoint, RemoteWireError, WebMux } from './protocol.js'
import { RemoteTranslator, type FollowRecord } from './translate.js'
import { REMOTE_BACKEND_ID } from './contract.js'

export interface QueueItem {
  /** Channel-generated client id (`source.rpcId` on the Host message). */
  readonly clientMessageId: string
  /** Host message id (`session/updateQueue` itemId). */
  readonly itemId: string
  readonly placement: 'followup' | 'steer'
  readonly text: string
}

interface WireQueueItem {
  readonly id?: unknown
  readonly placement?: unknown
  readonly rpcId?: unknown
  readonly content?: unknown
}

interface WireCatalogModel {
  readonly id: string
  readonly name: string
  readonly description?: string
  readonly efforts?: readonly { readonly id: string; readonly name: string }[]
  readonly defaultEffort?: string
}

interface WireCatalogGroup {
  readonly id: string
  readonly name: string
  readonly models: readonly WireCatalogModel[]
}

interface WireCatalog {
  readonly groups: readonly WireCatalogGroup[]
}

interface WireState {
  readonly cursor?: unknown
  readonly running?: unknown
  readonly queue?: readonly WireQueueItem[] | null
}

interface FollowSnapshot {
  readonly type?: unknown
  readonly header?: unknown
  readonly projections?: unknown
  readonly cursor?: unknown
  readonly records?: readonly FollowRecord[]
  readonly hasMore?: unknown
}

interface PageValue {
  readonly records?: readonly FollowRecord[]
  readonly hasMore?: unknown
}

interface ParkedPermission {
  readonly kind: 'permission'
  readonly request: PermissionRequestView
}

interface ParkedQuestion {
  readonly kind: 'question'
  readonly request: QuestionRequestView
  readonly questionIds: readonly string[]
}

type Parked = ParkedPermission | ParkedQuestion

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
const str = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined
const num = (value: unknown): number | undefined => typeof value === 'number' && Number.isFinite(value) ? value : undefined

const textOf = (content: unknown): string => {
  if (!Array.isArray(content)) return ''
  return content.flatMap(block => isRecord(block) && block.type === 'text' && typeof block.text === 'string' ? [block.text] : []).join('').trim()
}

/** The Host's own queue-action vocabulary (`session/updateQueue`). */
type WireQueueAction =
  | { readonly kind: 'remove' }
  | { readonly kind: 'steer' }
  | { readonly kind: 'edit'; readonly content: readonly { readonly type: 'text'; readonly text: string }[] }

/** The Host refuses a mutation for a row it no longer holds. */
const isGoneError = (error: unknown): boolean => {
  if (!(error instanceof RemoteWireError)) return false
  const code = (error as { code?: unknown }).code
  const message = error.message.toLowerCase()
  return typeof code === 'string'
    ? code.includes('not-found') || code.includes('gone') || code.includes('unknown-item')
    : message.includes('not found') || message.includes('not-found') || message.includes('no longer')
}

const queueFromState = (value: WireState | undefined): QueueItem[] => {
  const queue = value?.queue
  if (!Array.isArray(queue)) return []
  return queue.flatMap((entry): QueueItem[] => {
    if (!isRecord(entry)) return []
    const itemId = str(entry.id)
    if (itemId === undefined) return []
    const clientMessageId = str(entry.rpcId) ?? itemId
    return [{
      clientMessageId,
      itemId,
      placement: entry.placement === 'steering' ? 'steer' : 'followup',
      text: textOf(entry.content),
    }]
  })
}

const sleep = async (ms: number): Promise<void> => { await new Promise(resolve => setTimeout(resolve, ms)) }

/** A remote session bound to one Host session id. */
export class RemoteDshSession implements AgentSession {
  readonly ref: AgentSessionRef
  readonly capabilities: SessionCapabilities
  cwd: string
  status: AgentSessionStatus = 'starting'

  private readonly endpoint: RemoteEndpoint
  private readonly sessionId: string
  private mux: WebMux | undefined
  private follow: AsyncIterable<unknown> | undefined
  private events: AsyncIterable<unknown> | undefined
  private historyEvents: readonly AgentEvent[] = []
  private readonly translator = new RemoteTranslator(attachment => this.readAttachment(attachment))
  private listener: ((batch: readonly AgentEvent[], meta: AgentEventMeta) => void) | undefined
  private buffered: AgentEvent[] = []
  private recorded: AgentEvent[] = []
  private parked = new Map<string, Parked>()
  private disposed = false
  private connected = false
  private reconnecting = false
  private readonly older: AgentEvent[] = []
  private olderMore = false
  private oldestSeq: number | undefined
  private prefetching = false
  private title: string | undefined
  private model = ''
  /** The Host's resolved reasoning effort for this session (undefined = its
   *  own default), and the catalog it resolves against. */
  private selection: { effort?: string } = {}
  private cachedCatalog: WireCatalog | undefined

  private provider: string | undefined
  private permissionMode: string | undefined
  /** Submissions whose send receipt was lost; reconciled before they may move. */
  private readonly unconfirmed = new Set<string>()
  private reconcileTimer: ReturnType<typeof setTimeout> | undefined

  constructor(endpoint: RemoteEndpoint, sessionId: string, private readonly host: BackendHost) {
    this.endpoint = endpoint
    this.sessionId = sessionId
    this.ref = { backendId: REMOTE_BACKEND_ID, sessionId }
    this.cwd = host.cwd
    this.capabilities = {
      native: {},
      transcript: {
        record: () => this.recorded,
        hasOlder: () => this.olderMore || this.older.length > 0,
        older: () => this.takeOlder(),
      },
      // The queue lives in the Host, so every mutation is answered there:
      // a synchronous boolean would let the preview disappear before the Host
      // accepted (or refused) the retraction.
      pendingQueue: {
        remove: id => this.mutateQueue(id, { kind: 'remove' }),
        edit: (id, text) => this.mutateQueue(id, { kind: 'edit', content: [{ type: 'text', text }] }),
        steer: id => this.mutateQueue(id, { kind: 'steer' }),
      },
      permissions: {
        respond: (requestId, decision) => { void this.respondPermission(requestId, decision) },
        pending: () => [...this.parked.values()].flatMap(value => value.kind === 'permission' ? [value.request] : []),
      },
      questions: {
        respond: (requestId, answers) => { void this.respondQuestion(requestId, answers) },
        cancel: requestId => { void this.cancelQuestion(requestId) },
      },
      // The Host owns the catalog and the selection: /model reads the Host's
      // own catalog (so a model the Host cannot route is never offered) and
      // every switch is a Host RPC whose answer is the resolved selection.
      models: {
        list: () => this.listModels(),
        current: () => this.currentModel(),
        set: ref => this.selectModel(ref),
      },
      effort: {
        levels: () => this.effortLevels(),
        levelsAvailable: true,
        forModel: ref => this.effortForModel(ref),
        current: () => this.currentEffort(),
        set: id => this.setEffort(id),
      },
      // The Host publishes the effective command catalog for this agent and
      // executes a known line in-process (logging command/run + command/done):
      // a slash line is never model text on this backend.
      commands: {
        list: () => this.listCommands(),
        execute: line => this.runCommand(line),
      },
      rename: { rename: title => this.rename(title) },
      fork: { fork: anchor => this.fork(anchor) },
      // Children are Host sessions: the catalog is the parent's durable
      // `subagent/catalog` records, the transcript is their own log, and the
      // only mutation with a public interface is interrupt.
      subagents: {
        interrupt: agentId => this.interruptSubagent(agentId),
        history: (agentId, window) => this.readSubagentHistory(agentId, window),
      },
      // Background jobs: the Host registers/settles them in the durable log
      // (translated into task rows) and owns the kill request.
      tasks: { stop: taskId => this.killJob(taskId) },
      // The Host's terminal INVENTORY only: this client never attaches to a
      // terminal, never starts one and never types into one (stage-5 report).
      terminals: {
        list: () => this.hostTerminals(),
        environment: () => this.hostTerminalEnvironment(),
        shells: () => this.hostShells(),
      },
      // The workspace files of the SESSION's directory, read through the Host:
      // nothing here falls back to this machine's disk. Paths are relative to
      // the Host workspace root with `/` separators.
      files: {
        root: this.cwd,
        list: path => this.hostList(path),
        stat: path => this.hostStat(path),
        readText: path => this.hostReadText(path),
      },
    }
  }

  static async open(sessionId: string, endpoint: RemoteEndpoint, host: BackendHost): Promise<RemoteDshSession> {
    const session = new RemoteDshSession(endpoint, sessionId, host)
    await session.start()
    return session
  }

  history(): Promise<readonly AgentEvent[]> {
    return Promise.resolve(this.historyEvents)
  }

  subscribe(listener: (batch: readonly AgentEvent[], meta: AgentEventMeta) => void): () => void {
    this.listener = listener
    if (this.buffered.length > 0) {
      const batch = this.buffered.splice(0)
      listener(batch, { replay: false })
    }
    return () => {
      if (this.listener === listener) this.listener = undefined
    }
  }

  async submit(input: AgentInput, placement: SubmitPlacement): Promise<{
    readonly accepted: boolean
    readonly reason?: string
    readonly dropped?: boolean
    readonly outcome?: 'accepted' | 'rejected' | 'unknown'
  }> {
    // Nothing was sent in these three cases, so the text may safely go back to
    // the composer as a draft.
    if (this.disposed) return { accepted: false, reason: 'the remote session is closed', dropped: true, outcome: 'rejected' }
    if (!this.connected || this.reconnecting) return { accepted: false, reason: 'the remote connection is reconnecting; the text was kept as a draft and not sent', dropped: true, outcome: 'rejected' }
    if (placement === 'now') return { accepted: false, reason: 'interrupt-and-resubmit is not supported in remote mode yet', dropped: true, outcome: 'rejected' }
    const mode = placement === 'steer' ? 'steer' : 'queue'
    // Images are the Host's inline prompt part (`{type:'image', mediaType,
    // data}`): the bytes come from the client file the user attached, never
    // from a Host path. An unreadable attachment refuses the submit BEFORE
    // anything leaves this process, so the draft keeps every image.
    const images: { type: 'image'; mediaType: string; data: string; name?: string }[] = []
    for (const image of input.images ?? []) {
      try {
        const bytes = await image.read()
        images.push({
          type: 'image',
          mediaType: image.mediaType ?? 'image/png',
          data: Buffer.from(bytes).toString('base64'),
          ...(image.name === undefined ? {} : { name: image.name }),
        })
      } catch (error) {
        return {
          accepted: false,
          reason: `could not read the attached image${image.name === undefined ? '' : ` ${image.name}`}: ${error instanceof Error ? error.message : String(error)}`,
          dropped: true,
          outcome: 'rejected',
        }
      }
    }
    try {
      const value = await this.endpoint.rpc<{ accepted?: unknown; reason?: unknown }>('session/prompt', {
        request: {
          requestId: input.clientMessageId,
          sessionId: this.sessionId,
          mode,
          content: [
            ...(input.text === '' ? [] : [{ type: 'text' as const, text: input.text }]),
            ...images,
          ],
        },
      }, { retryAuth: false })
      if (value.accepted === false) {
        return { accepted: false, reason: str(value.reason) ?? 'the Host refused the prompt', dropped: true, outcome: 'rejected' }
      }
      return { accepted: true, outcome: 'accepted' }
    } catch (error) {
      if (error instanceof RemoteConnectionLost) {
        this.lose(error)
        // The prompt left this process, so the Host may have it. The row stays
        // where it is (not a re-sendable draft) until `reconcileUnconfirmed`
        // checks the queue and the durable log.
        this.unconfirmed.add(input.clientMessageId)
        this.scheduleReconcile()
        return {
          accepted: false,
          reason: 'the connection was lost before the Host confirmed the prompt; it will be reconciled, not re-sent',
          outcome: 'unknown',
        }
      }
      if (error instanceof RemoteWireError) {
        return { accepted: false, reason: error.message, dropped: true, outcome: 'rejected' }
      }
      this.unconfirmed.add(input.clientMessageId)
      this.scheduleReconcile()
      return { accepted: false, reason: error instanceof Error ? error.message : String(error), outcome: 'unknown' }
    }
  }

  async cancel(cause: CancelCause): Promise<CancelReceipt> {
    const before = this.translator.queue().map(item => item.clientMessageId)
    if (this.disposed || !this.connected) return { stillQueued: before, outcome: 'unknown' }
    if (cause === 'switch' || cause === 'dispose') return { stillQueued: before, outcome: 'unknown' }
    try {
      await this.endpoint.rpc<unknown>('session/cancel', { request: { sessionId: this.sessionId } }, { retryAuth: false })
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      return { stillQueued: before, outcome: 'failed' }
    }
    // `session/cancel` is documented as "cancel one active Agent turn without
    // dropping its pending inbox": the Host keeps every queued submission and
    // delivers it as the next turn. Deleting rows here would also delete what
    // another client (the Web UI) queued in the same session, so the receipt
    // reports the Host's queue instead of mutating it.
    const state = await this.readState()
    if (state === undefined) return { stillQueued: before, outcome: 'unknown' }
    // The Host's queue is the receipt: mirror it and repaint the previews, so
    // an interrupt never looks like it emptied a queue the Host still holds.
    const queue = queueFromState(state)
    this.translator.setQueue(queue)
    this.emit([this.pendingEvent(undefined, undefined)])
    return { stillQueued: queue.map(item => item.clientMessageId), outcome: 'confirmed' }
  }

  async dispose(): Promise<void> {
    if (this.disposed) return
    this.disposed = true
    if (this.reconcileTimer !== undefined) {
      clearTimeout(this.reconcileTimer)
      this.reconcileTimer = undefined
    }
    this.connected = false
    this.reconnecting = false
    this.status = 'disposed'
    this.parked.clear()
    this.mux?.close()
    this.mux = undefined
  }

  private async start(): Promise<void> {
    await this.connect()
    const snapshot = this.snapshot
    const header = isRecord(snapshot?.header) ? snapshot.header : {}
    const projections = isRecord(snapshot?.projections) ? snapshot.projections : {}
    const values = isRecord(projections.values) ? projections.values : {}
    this.cwd = str(header.cwd) ?? this.host.cwd
    this.title = str(values.title) ?? undefined
    const selection = isRecord(values.modelSelection) ? values.modelSelection : {}
    const lastUsed = isRecord(selection.lastUsed) ? selection.lastUsed : {}
    this.model = str(lastUsed.model) ?? ''
    this.provider = str(lastUsed.provider)
    const permissions = isRecord(values.permissions) ? values.permissions : {}
    this.permissionMode = str(permissions.currentValue)
    const state = await this.readState()
    if (state !== undefined) this.translator.setQueue(queueFromState(state))
    this.status = state?.running === true ? 'running' : 'idle'
    const ready: AgentEvent = {
      type: 'session.ready',
      sessionId: this.sessionId,
      cwd: this.cwd,
      model: this.model,
      ...(this.provider === undefined ? {} : { provider: this.provider }),
      ...(this.title === undefined || this.title === '' ? {} : { title: this.title }),
      ...(this.permissionMode === undefined ? {} : { permissionMode: this.permissionMode }),
    }
    const events = this.translateRecords(snapshot?.records ?? [], true)
    this.historyEvents = [ready, ...events]
    this.recorded = [...this.historyEvents]
    this.oldestSeq = minSeq(snapshot?.records ?? [])
    this.olderMore = snapshot?.hasMore === true
    this.emit([{ type: 'session.status', status: this.status }])
    void this.pump()
    void this.prefetchOlder()
  }

  private snapshot: FollowSnapshot | undefined
  private pumpGeneration = 0

  /** First physical generation: read the opening snapshot, then keep both
   *  streams moving. Reconnection reopens both streams on a fresh cookie. */
  private async connect(): Promise<void> {
    const mux = await this.endpoint.openMux()
    this.mux = mux
    this.follow = mux.openStream('follow', 'session/follow', {
      args: { request: { address: { kind: 'session', sessionId: this.sessionId }, assistantStream: true } },
    })
    this.events = mux.openStream('events', '$events', { args: {} })
    const iterator = this.follow[Symbol.asyncIterator]()
    for (;;) {
      const next = await iterator.next()
      if (next.done === true) throw new RemoteConnectionLost('the session stream ended before its opening snapshot')
      if (isRecord(next.value) && next.value.type === 'snapshot') {
        this.snapshot = next.value as FollowSnapshot
        // Keep reading from the same iterator: wrap the consumed first frame
        // with an iterator whose next() continues after it.
        this.follow = { [Symbol.asyncIterator]: () => ({ next: () => iterator.next(), return: () => iterator.return?.() ?? Promise.resolve({ done: true, value: undefined }) }) }
        break
      }
    }
    this.connected = true
    this.reconnecting = false
  }

  private async pump(): Promise<void> {
    const generation = ++this.pumpGeneration
    while (!this.disposed) {
      const follow = this.follow
      const events = this.events
      if (follow === undefined || events === undefined) break
      try {
        await Promise.allSettled([this.readFollow(follow, generation), this.readEvents(events)])
      } catch { /* read helpers surface loss through lose() */ }
      if (this.disposed || generation !== this.pumpGeneration) return
      this.lose(new RemoteConnectionLost())
      this.reconnecting = true
      let delay = 1000
      while (!this.disposed) {
        this.emit([{ type: 'notice', level: 'warning', text: 'Remote Web connection lost; reconnecting…', key: 'remote-reconnect' }])
        try {
          this.mux?.close()
          this.mux = undefined
          await this.connect()
          const snapshot = this.snapshot
          const nextFollow = this.follow
          const nextEvents = this.events
          if (snapshot === undefined || nextFollow === undefined || nextEvents === undefined) throw new RemoteConnectionLost()
          await this.applyReconnect(snapshot)
          this.emit([{ type: 'notice', level: 'info', text: 'Remote Web connection restored.', key: 'remote-reconnect' }])
          break
        } catch {
          await sleep(delay)
          delay = Math.min(delay * 2, 15000)
        }
      }
      if (this.disposed) return
    }
  }

  private async readFollow(stream: AsyncIterable<unknown>, generation: number): Promise<void> {
    for await (const value of stream) {
      if (this.disposed || generation !== this.pumpGeneration) return
      if (!isRecord(value)) continue
      if (value.type === 'event' && isRecord(value.event)) {
        const events = this.translator.translateRecord({ type: 'event', event: value.event as unknown as NonNullable<FollowRecord['event']> }, false)
        if (events.length > 0) this.emit(this.withStatus(events))
        continue
      }
      if (value.type === 'assistant-stream' && isRecord(value.frame)) {
        const events = this.translator.translateAssistantFrame(value.frame as unknown as Parameters<RemoteTranslator['translateAssistantFrame']>[0])
        if (events.length > 0) this.emit(events, 'frame')
        continue
      }
      if (value.type === 'snapshot') {
        // A follow stream may rebase after a Host-side generation change while
        // this physical socket stays up. Treat it as a reconnect baseline.
        this.snapshot = value as FollowSnapshot
        await this.applyReconnect(this.snapshot)
      }
    }
    throw new RemoteConnectionLost('the session event stream ended')
  }

  private async readEvents(stream: AsyncIterable<unknown>): Promise<void> {
    for await (const value of stream) {
      if (this.disposed) return
      if (!isRecord(value)) continue
      if (value.type === 'emit') {
        if (value.event === 'api-session/status' && Array.isArray(value.args)) {
          const [sessionId, running] = value.args
          if (sessionId !== this.sessionId) continue
          const next: AgentSessionStatus = running === true ? 'running' : 'idle'
          if (this.status !== next) {
            this.status = next
            this.emit([{ type: 'session.status', status: next }])
          }
        }
        continue
      }
      if (value.type === 'waterfall' && typeof value.eventId === 'string' && isRecord(value.request)) {
        this.handleWaterfall(value.event, value.eventId, str(value.agentId), value.request)
        continue
      }
      if (value.type === 'cancel' && typeof value.eventId === 'string') {
        this.settleParked(value.eventId, 'cancelled')
      }
    }
    throw new RemoteConnectionLost('the forwarded-event stream ended')
  }

  private handleWaterfall(event: unknown, eventId: string, agentId: string | undefined, request: Record<string, unknown>): void {
    if (agentId !== undefined && agentId !== this.sessionId) {
      void this.mux?.answer(this.endpoint, eventId, { kind: 'next' }).catch(() => undefined)
      return
    }
    if (event === 'approval/request') {
      const toolName = str(request.toolName) ?? 'tool'
      const display = isRecord(request.displayReason) ? request.displayReason : undefined
      const title = display === undefined ? undefined : str(display[this.host.locale()]) ?? str(display.en)
      const view: PermissionRequestView = {
        requestId: eventId,
        toolName,
        ...(str(request.callId) === undefined ? {} : { callId: str(request.callId) }),
        ...(str(request.reason) === undefined ? {} : { reason: str(request.reason) }),
        ...(title === undefined ? {} : { title }),
        options: [
          { id: 'allow-once', kind: 'allow-once' },
          { id: 'reject', kind: 'reject' },
        ],
      }
      this.parked.set(eventId, { kind: 'permission', request: view })
      this.emit([{ type: 'permission.request', request: view }])
      return
    }
    if (event === 'user-questions/request') {
      const questions = Array.isArray(request.questions) ? request.questions : []
      const pairs = questions.flatMap((question): { id: string; view: QuestionItemView }[] => {
        if (!isRecord(question) || typeof question.question !== 'string' || typeof question.id !== 'string') return []
        const options = Array.isArray(question.options)
          ? question.options.flatMap(option => isRecord(option) && typeof option.label === 'string'
            ? [{ label: option.label, ...(typeof option.description === 'string' ? { description: option.description } : {}) }]
            : [])
          : []
        return [{
          id: question.id,
          view: {
            question: question.question,
            options,
            ...(typeof question.header === 'string' ? { header: question.header } : {}),
            ...(typeof question.detail === 'string' ? { detail: question.detail } : {}),
            ...(question.multiSelect === true ? { multiSelect: true } : {}),
          },
        }]
      })
      if (pairs.length === 0) {
        void this.mux?.answer(this.endpoint, eventId, { kind: 'next' }).catch(() => undefined)
        return
      }
      const items = pairs.map(pair => pair.view)
      const questionIds = pairs.map(pair => pair.id)
      const wait = isRecord(request.wait) ? request.wait : undefined
      const callId = str(wait?.callId)
      const view: QuestionRequestView = {
        requestId: eventId,
        questions: items,
        ...(callId === undefined ? {} : { callId }),
      }
      this.parked.set(eventId, { kind: 'question', request: view, questionIds })
      this.emit([{ type: 'question.request', request: view }])
    }
  }

  private settlePermission(eventId: string, outcome: PermissionOutcome): void {
    const parked = this.parked.get(eventId)
    if (parked === undefined || parked.kind !== 'permission') return
    this.parked.delete(eventId)
    this.emit([{ type: 'permission.settled', requestId: eventId, outcome }])
  }

  private settleQuestion(eventId: string): void {
    const parked = this.parked.get(eventId)
    if (parked === undefined || parked.kind !== 'question') return
    this.parked.delete(eventId)
    this.emit([{ type: 'question.settled', requestId: eventId }])
  }

  private settleParked(eventId: string, outcome: 'cancelled' | 'settled'): void {
    const parked = this.parked.get(eventId)
    if (parked === undefined) return
    if (parked.kind === 'permission') this.settlePermission(eventId, outcome === 'cancelled' ? 'cancelled' : 'rejected')
    else this.settleQuestion(eventId)
  }

  private async respondPermission(requestId: string, decision: PermissionDecision): Promise<void> {
    const parked = this.parked.get(requestId)
    if (parked === undefined || parked.kind !== 'permission') return
    // The gateway only accepts a tagged outcome: `{kind:'result', value}` for
    // a decision, `{kind:'rejected', error}` to refuse the request itself.
    // The Host's own vocabulary is the approval outcome string.
    const outcome = decision.kind === 'reject' ? 'rejected' : 'allowed-once'
    try {
      if (this.mux === undefined) throw new RemoteConnectionLost()
      await this.mux.answer(this.endpoint, requestId, { kind: 'result', value: outcome })
      this.settlePermission(requestId, decision.kind === 'reject' ? 'rejected' : 'allow-once')
    } catch (error) {
      this.emit([{ type: 'notice', level: 'warning', text: `Could not answer the permission request: ${error instanceof Error ? error.message : String(error)}` }])
      if (error instanceof RemoteConnectionLost) this.lose(error)
    }
  }

  private async respondQuestion(requestId: string, answers: QuestionAnswers): Promise<void> {
    const parked = this.parked.get(requestId)
    if (parked === undefined || parked.kind !== 'question') return
    const batch = answers.answers.map((answer, index) => ({
      id: parked.questionIds[index] ?? String(index),
      selected: answer.selected,
      ...(answer.custom === undefined ? {} : { custom: answer.custom }),
    }))
    try {
      if (this.mux === undefined) throw new RemoteConnectionLost()
      await this.mux.answer(this.endpoint, requestId, { kind: 'result', value: { answers: batch } })
      this.settleQuestion(requestId)
    } catch (error) {
      this.emit([{ type: 'notice', level: 'warning', text: `Could not answer the question: ${error instanceof Error ? error.message : String(error)}` }])
      if (error instanceof RemoteConnectionLost) this.lose(error)
    }
  }

  private async cancelQuestion(requestId: string): Promise<void> {
    const parked = this.parked.get(requestId)
    if (parked === undefined || parked.kind !== 'question') return
    try {
      if (this.mux === undefined) throw new RemoteConnectionLost()
      await this.mux.answer(this.endpoint, requestId, {
        kind: 'rejected',
        error: { name: 'UserQuestionError', message: 'the user closed the question', code: 'ASK_ABORTED' },
      })
    } catch { /* closing the panel is still the right local outcome */ }
    this.settleQuestion(requestId)
  }

  /** Deliver one batch, or hold it for the subscriber that is about to
   *  attach: the channel subscribes after `open()` returns, so frames that
   *  arrive during the opening handshake must survive too. */
  private emit(events: readonly AgentEvent[], _kind?: 'frame'): void {
    if (events.length === 0) return
    if (this.listener !== undefined) {
      this.listener(events, { replay: false })
      return
    }
    this.buffered.push(...events)
  }

  /** Mirror a durable batch's turn boundaries into the session status. */
  private withStatus(events: readonly AgentEvent[]): readonly AgentEvent[] {
    const started = events.some(event => event.type === 'turn.start')
    const ended = events.some(event => event.type === 'turn.end')
    const next = started ? 'running' : ended ? 'idle' : undefined
    if (next === undefined || next === this.status) return events
    this.status = next
    return [...events, { type: 'session.status', status: next }]
  }

  private pendingEvent(claimed?: readonly string[], dropped?: readonly string[]): AgentEvent {
    return this.translator.pendingSnapshot(claimed, dropped)
  }

  /**
   * Ask the Host to mutate one still-pending submission and report what it
   * actually did. Only an explicit refusal is a refusal: a lost receipt is
   * `unknown`, because the Host may have applied the mutation before the
   * connection dropped.
   */
  private async mutateQueue(
    clientMessageId: string,
    action: WireQueueAction,
  ): Promise<QueueMutationOutcome> {
    const item = this.translator.queue().find(candidate => candidate.clientMessageId === clientMessageId)
    if (item === undefined) return this.goneOutcome(clientMessageId)
    if (this.disposed) return { kind: 'rejected', reason: 'the remote session is closed' }
    if (!this.connected) {
      return { kind: 'unknown', reason: 'the remote connection is down, so the Host could not confirm the queue change' }
    }
    try {
      await this.endpoint.rpc<unknown>('session/updateQueue', {
        request: { sessionId: this.sessionId, itemId: item.itemId, action },
      }, { retryAuth: false })
    } catch (error) {
      if (error instanceof RemoteConnectionLost) {
        this.lose(error)
        return { kind: 'unknown', reason: 'the connection was lost before the Host confirmed the queue change' }
      }
      const message = error instanceof Error ? error.message : String(error)
      // A row the Host no longer holds cannot be mutated any more: that is a
      // state fact, not a failure the user should retry.
      if (isGoneError(error)) {
        this.translator.dropQueued(clientMessageId)
        return this.goneOutcome(clientMessageId)
      }
      return { kind: 'rejected', reason: message }
    }
    // The receipt means the Host committed the mutation; the authoritative
    // queue arrives with the next state or inbox frame, so the mirror drops the
    // row here rather than waiting for it.
    if (action.kind !== 'edit') this.translator.dropQueued(clientMessageId)
    return { kind: 'accepted' }
  }

  /**
   * Why a row is no longer pending. A durable user message carrying its
   * request id is proof it ran; a bounded replay window that does not contain
   * it is not proof of the opposite, so absence stays a plain `gone`.
   */
  private goneOutcome(clientMessageId: string): QueueMutationOutcome {
    return this.translator.hasSeenRequestId(clientMessageId)
      ? { kind: 'gone', claimed: true }
      : { kind: 'gone' }
  }

  /**
   * Settle submissions whose receipt was lost. The Host is asked again: a
   * request that shows up in its queue or in the durable log landed, and one
   * that is in neither never arrived — that row is handed back to the channel
   * as a re-sendable draft (`dropped`) instead of staying invisible.
   */
  /** One delayed pass: an unknown receipt usually resolves as soon as the
   *  queue projection or the durable log catches up. */
  private scheduleReconcile(): void {
    if (this.reconcileTimer !== undefined) return
    this.reconcileTimer = setTimeout(() => {
      this.reconcileTimer = undefined
      void this.reconcileUnconfirmed()
    }, 4000)
    this.reconcileTimer.unref?.()
  }

  private async reconcileUnconfirmed(): Promise<void> {
    if (this.unconfirmed.size === 0 || this.disposed || !this.connected) return
    const state = await this.readState()
    if (state === undefined) return
    const queued = new Set(queueFromState(state).map(item => item.clientMessageId))
    const landed: string[] = []
    const released: string[] = []
    for (const id of [...this.unconfirmed]) {
      this.unconfirmed.delete(id)
      if (this.translator.hasSeenRequestId(id)) landed.push(id)
      else if (!queued.has(id)) released.push(id)
    }
    if (landed.length > 0) {
      // A durable message already carries this request: retire its preview.
      this.emit([this.pendingEvent(landed, undefined)])
    }
    if (released.length > 0) {
      // Neither queued nor durable: the channel turns these into drafts.
      this.emit([this.pendingEvent(undefined, released)])
      this.emit([{
        type: 'notice',
        level: 'warning',
        text: 'The Host did not receive a message whose send was interrupted; it is kept as a draft.',
        key: 'remote-unconfirmed',
      }])
    }
  }

  /** Degrade to "connection lost": the pump reconnects and the UI keeps its
   *  screen until a fresh baseline replaces it. Never resends anything. */
  private lose(error: Error): void {
    if (this.disposed) return
    this.connected = false
    this.mux?.close()
    if (this.mux !== undefined) this.mux = undefined
    // The pump's outer loop will reconnect; a submission sees the gap.
    void error
  }

  // ── model catalog, selection and effort (Host-owned) ─────────────────────

  /** The Host's catalog, fetched once per session and refreshed on demand. */
  private async catalog(): Promise<WireCatalog | undefined> {
    if (this.cachedCatalog !== undefined) return this.cachedCatalog
    if (this.disposed || !this.connected) return undefined
    try {
      const value = await this.endpoint.rpc<WireCatalog>('session/modelCatalog', {}, { retryAuth: false })
      const groups = Array.isArray(value?.groups) ? value.groups : []
      const parsed: WireCatalog = {
        groups: groups.flatMap(group => {
          if (!isRecord(group)) return []
          const id = str(group.id)
          const name = str(group.name) ?? id
          if (id === undefined || name === undefined) return []
          const models = Array.isArray(group.models) ? group.models : []
          return [{
            id,
            name,
            models: models.flatMap(model => {
              if (!isRecord(model)) return []
              const modelId = str(model.id)
              if (modelId === undefined) return []
              const reasoning = isRecord(model.reasoning) ? model.reasoning : undefined
              const efforts = Array.isArray(reasoning?.efforts) ? reasoning.efforts : []
              return [{
                id: modelId,
                name: str(model.name) ?? modelId,
                ...(str(model.description) === undefined ? {} : { description: str(model.description) }),
                ...(reasoning === undefined ? {} : {
                  defaultEffort: str(reasoning.defaultEffort),
                  efforts: efforts.flatMap(effort => {
                    if (!isRecord(effort)) return []
                    const effortId = str(effort.id)
                    return effortId === undefined ? [] : [{ id: effortId, name: str(effort.name) ?? effortId }]
                  }),
                }),
              }]
            }),
          }]
        }),
      }
      this.cachedCatalog = parsed
      return parsed
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      throw error
    }
  }

  private catalogModel(catalog: WireCatalog | undefined, ref: ModelRef): WireCatalogModel | undefined {
    if (catalog === undefined) return undefined
    const group = catalog.groups.find(candidate => candidate.id === ref.provider)
    const matches = group === undefined ? catalog.groups.flatMap(candidate => candidate.models) : group.models
    return matches.find(model => model.id === ref.model)
  }

  private async listModels(): Promise<readonly ModelOption[]> {
    const catalog = await this.catalog()
    if (catalog === undefined) return []
    return catalog.groups.flatMap(group => group.models.map(model => ({
      provider: group.id,
      id: model.id,
      label: model.name,
      ...(model.description === undefined ? {} : { description: model.description }),
    })))
  }

  private currentModel(): ModelRef {
    return { ...(this.provider === undefined ? {} : { provider: this.provider }), model: this.model }
  }

  /**
   * Switch through the Host's own selector: the answer carries the resolved
   * selection, and only that answer updates this client. A refusal (or a lost
   * connection) leaves the old binding exactly as it was.
   */
  private async selectModel(ref: ModelRef): Promise<ModelSwitchOutcome> {
    if (this.disposed) return { kind: 'refused', reason: 'the remote session is closed' }
    const provider = ref.provider ?? this.provider
    if (provider === undefined) return { kind: 'refused', reason: 'the Host catalog did not name a provider for this session' }
    try {
      const value = await this.endpoint.rpc<{ selected?: unknown }>('session/selectModel', {
        request: {
          sessionId: this.sessionId,
          provider,
          model: ref.model,
          ...(this.selection.effort === undefined ? {} : { reasoningEffort: this.selection.effort }),
        },
      }, { retryAuth: false })
      const selected = isRecord(value?.selected) ? value.selected : {}
      const model = str(selected.model) ?? ref.model
      const selectedProvider = str(selected.provider) ?? provider
      this.provider = selectedProvider
      this.model = model
      this.selection = { effort: str(selected.reasoningEffort) }
      this.emit([{ type: 'model.changed', model, provider: selectedProvider, source: 'user' }])
      return { kind: 'switched' }
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      const reason = error instanceof Error ? error.message : String(error)
      return { kind: 'refused', reason: `the Host did not accept the model switch: ${reason}` }
    }
  }

  private modelReasoning(): { efforts: readonly EffortOption[]; defaultEffort?: string } | undefined {
    const model = this.catalogModel(this.cachedCatalog, this.currentModel())
    if (model?.efforts === undefined) return undefined
    return {
      efforts: model.efforts.map(effort => ({ id: effort.id, label: effort.name })),
      ...(model.defaultEffort === undefined ? {} : { defaultEffort: model.defaultEffort }),
    }
  }

  /** The catalog is read at session start (the status line needs the model);
   *  a ladder that arrives later is fetched once and cached. */
  private effortLevels(): readonly EffortOption[] {
    return this.modelReasoning()?.efforts ?? []
  }

  private effortForModel(ref: ModelRef): { readonly levels: readonly EffortOption[]; readonly defaultEffort?: string } {
    const model = this.catalogModel(this.cachedCatalog, ref)
    return {
      levels: (model?.efforts ?? []).map(effort => ({ id: effort.id, label: effort.name })),
      ...(model?.defaultEffort === undefined ? {} : { defaultEffort: model.defaultEffort }),
    }
  }

  private currentEffort(): string | undefined {
    return this.selection.effort
  }

  /** Effort rides the same Host selector: provider and model stay put. */
  private async setEffort(id: string | null): Promise<void> {
    const ref = this.currentModel()
    if (ref.model === '') return
    if (ref.provider === undefined) return
    this.selection = { ...this.selection, effort: id ?? undefined }
    const outcome = await this.selectModel(ref)
    if (outcome.kind === 'refused') {
      this.selection = { ...this.selection, effort: undefined }
      throw new Error(outcome.reason)
    }
    this.emit([{ type: 'effort.changed', effort: id }])
  }

  private async rename(title: string): Promise<void> {
    if (this.disposed) throw new Error('the remote session is closed')
    const value = await this.endpoint.rpc<{ title?: unknown }>('session/rename', {
      request: { sessionId: this.sessionId, title },
    }, { retryAuth: false })
    const applied = str(value?.title) ?? title
    this.title = applied
    // The durable `session/title` record repaints the UI; emitting it here too
    // keeps the status line right even before that record lands.
    this.emit([{ type: 'session.title', title: applied, source: 'user' }])
  }

  /**
   * A Host fork is a new session on the Host; the caller opens it as a resume.
   * The anchor is a record seq (remote user messages carry `String(seq)`), so
   * a non-numeric anchor falls back to the Host's latest completed turn.
   */
  private async fork(anchor?: string): Promise<AgentSessionRef> {
    if (this.disposed) throw new Error('the remote session is closed')
    const atSeq = anchor === undefined ? undefined : Number(anchor)
    const value = await this.endpoint.rpc<{ sessionId?: unknown }>('session/fork', {
      request: {
        sessionId: this.sessionId,
        ...(atSeq === undefined || !Number.isSafeInteger(atSeq) || atSeq < 0 ? {} : { atSeq }),
      },
    }, { retryAuth: false })
    const sessionId = str(value?.sessionId)
    if (sessionId === undefined) throw new Error('the Host did not return the forked session id')
    return { backendId: REMOTE_BACKEND_ID, sessionId }
  }

  // ── the Host's command registry ───────────────────────────────────────────

  private async listCommands(): Promise<readonly CommandInfo[]> {
    // Not gated on `connected`: the endpoint authenticates on demand, and the
    // channel asks as soon as a session is bound — usually before the opening
    // snapshot lands. An unreadable catalog answers empty, never an exception.
    if (this.disposed) return []
    try {
      const value = await this.endpoint.rpc<readonly unknown[]>('commands/list', { agentId: this.sessionId }, { retryAuth: false })
      const descriptors = Array.isArray(value) ? value : []
      return descriptors.flatMap((entry): CommandInfo[] => {
        if (!isRecord(entry)) return []
        const name = str(entry.name)
        if (name === undefined) return []
        const input = isRecord(entry.input) ? entry.input : undefined
        const hint = str(input?.hint)
        return [{
          name,
          ...(str(entry.description) === undefined ? {} : { description: str(entry.description) }),
          ...(hint === undefined ? {} : { argumentHint: hint }),
        }]
      })
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      throw error
    }
  }

  /**
   * Execute one slash line on the Host. `undefined` from the Host means the
   * name is no command at all; the caller must report that instead of sending
   * the line to the model.
   */
  private async runCommand(line: string): Promise<CommandRunOutcome> {
    if (this.disposed) return { kind: 'failed', reason: 'the remote session is closed' }
    if (!this.connected) return { kind: 'failed', reason: 'the remote connection is down, so the Host could not run the command' }
    try {
      // Attachments are empty until the Host's file receipts are wired; the
      // Host rejects an invocation that carries none it expected.
      const value = await this.endpoint.rpc<unknown>('commands/execute', {
        agentId: this.sessionId,
        line,
        submittedAttachments: [],
      }, { retryAuth: false })
      if (value === undefined || value === null) return { kind: 'unknown' }
      const execution = isRecord(value) ? value : {}
      const result = isRecord(execution.result) ? execution.result : {}
      const kind = str(result.kind)
      const text = str(result.text) ?? ''
      if (kind === 'error') return { kind: 'settled', text, error: true }
      if (kind === 'success') return { kind: 'settled', text }
      return { kind: 'failed', reason: `the Host returned an unknown command result: ${kind ?? 'none'}` }
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      return { kind: 'failed', reason: error instanceof Error ? error.message : String(error) }
    }
  }

  // ── subagents and background jobs (Host-owned) ────────────────────────────

  private async interruptSubagent(agentId: string): Promise<boolean> {
    if (this.disposed || !this.connected) return false
    try {
      const value = await this.endpoint.rpc<{ accepted?: unknown }>('subagents/interruptByUser', { childSessionId: agentId }, { retryAuth: false })
      return value?.accepted === true
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      return false
    }
  }

  /** One direct child's own durable transcript, addressed by parent and child. */
  private async readSubagentHistory(agentId: string, window?: SubagentTranscriptWindow): Promise<SubagentTranscriptPage> {
    const entry = this.subagentCatalog().find(candidate => candidate.id === agentId)
    const address = {
      kind: 'subagent',
      parentSessionId: this.sessionId,
      childSessionId: agentId,
      mode: entry?.mode ?? 'unknown',
    }
    const value = await this.endpoint.rpc<{ records?: readonly FollowRecord[] }>('session/page', {
      request: { address, ...(window === undefined ? {} : { maxMessages: Math.max(1, window.count) }) },
    }, { retryAuth: false })
    const records = Array.isArray(value?.records) ? value.records : []
    const translator = new RemoteTranslator(attachment => this.readAttachment(attachment))
    const events = records.flatMap(record => translator.translateRecord(record, true))
    return {
      events,
      parentAgentId: this.sessionId,
      uuids: events.flatMap(event => 'id' in event && typeof event.id === 'string' ? [event.id] : []),
      hasOlder: records.length > 0,
      skippedFromStart: 0,
    }
  }

  /** One durable image's bytes, read back from the Host by its reference. */
  private async readAttachment(attachment: unknown): Promise<Uint8Array> {
    const value = await this.endpoint.rpc<{ data?: unknown }>('session/attachment', {
      request: { sessionId: this.sessionId, attachment },
    }, { retryAuth: false })
    const data = str(value?.data)
    if (data === undefined) throw new Error('the Host returned no image bytes')
    return Buffer.from(data, 'base64')
  }

  // ── the Host's terminal inventory (read-only) ─────────────────────────────

  private async hostTerminals(): Promise<readonly HostTerminalInfo[]> {
    const value = await this.endpoint.rpc<unknown>('terminal/list', {
      sessionId: this.sessionId,
    }, { retryAuth: false })
    const items = Array.isArray(value)
      ? value
      : isRecord(value) && Array.isArray(value.terminals) ? value.terminals : []
    return items.flatMap(item => {
      if (!isRecord(item)) return []
      const id = str(item.id)
      if (id === undefined) return []
      const state = item.state === 'running' ? 'running' as const : item.state === 'exited' ? 'exited' as const : 'failed' as const
      return [{
        id,
        title: str(item.title) ?? id,
        state,
        cwd: str(item.cwd) ?? '',
        cols: num(item.cols) ?? 0,
        rows: num(item.rows) ?? 0,
        origin: item.origin === 'agent' ? 'agent' as const : 'user' as const,
        ...(item.exitCode === null || num(item.exitCode) === undefined ? {} : { exitCode: num(item.exitCode) }),
      }]
    })
  }

  private async hostTerminalEnvironment(): Promise<HostTerminalEnvironment | undefined> {
    try {
      const value = await this.endpoint.rpc<{ cwd?: unknown; maxCols?: unknown; maxRows?: unknown; scrollback?: unknown }>('terminal/environment', {
        agentId: this.sessionId,
      }, { retryAuth: false })
      if (!isRecord(value)) return undefined
      return {
        cwd: str(value.cwd) ?? '',
        maxCols: num(value.maxCols) ?? 0,
        maxRows: num(value.maxRows) ?? 0,
        scrollback: num(value.scrollback) ?? 0,
      }
    } catch {
      return undefined
    }
  }

  private async hostShells(): Promise<readonly { name: string; path: string }[]> {
    try {
      const value = await this.endpoint.rpc<unknown>('terminal/shells', {
        agentId: this.sessionId,
      }, { retryAuth: false })
      const items = Array.isArray(value)
        ? value
        : isRecord(value) && Array.isArray(value.shells) ? value.shells : []
      return items.flatMap(item => {
        if (!isRecord(item)) return []
        const path = str(item.path)
        if (path === undefined) return []
        return [{ name: str(item.name) ?? path, path }]
      })
    } catch {
      return []
    }
  }

  // ── the session's own workspace files (Host-owned) ────────────────────────

  private async hostList(path: string): Promise<{ path: string; entries: readonly { name: string; type: 'file' | 'directory' | 'other' }[] }> {
    const value = await this.endpoint.rpc<{ path?: unknown; entries?: readonly unknown[] }>('workspaceFiles/list', {
      workspaceFileScopeId: this.sessionId,
      path,
    }, { retryAuth: false })
    const entries = (Array.isArray(value?.entries) ? value.entries : []).flatMap(entry => {
      if (!isRecord(entry)) return []
      const name = str(entry.name)
      if (name === undefined) return []
      const type = entry.type === 'directory' ? 'directory' as const : entry.type === 'file' ? 'file' as const : 'other' as const
      return [{ name, type }]
    })
    return { path: str(value?.path) ?? path, entries }
  }

  private async hostStat(path: string): Promise<{ type: 'file' | 'directory' | 'other'; absolutePath?: string } | undefined> {
    try {
      const value = await this.endpoint.rpc<{ type?: unknown; absolutePath?: unknown }>('workspaceFiles/stat', {
        workspaceFileScopeId: this.sessionId,
        path,
      }, { retryAuth: false })
      const type = value?.type === 'directory' ? 'directory' as const : value?.type === 'file' ? 'file' as const : 'other' as const
      const absolutePath = str(value?.absolutePath)
      return { type, ...(absolutePath === undefined ? {} : { absolutePath }) }
    } catch {
      // A stat miss is a miss, not a broken channel: the caller reports it.
      return undefined
    }
  }

  private async hostReadText(path: string): Promise<{ text: string; eof: boolean }> {
    // No line range: the Host owns its page size, so this asks for the opening
    // page and reports its own end-of-file flag.
    const value = await this.endpoint.rpc<{ text?: unknown; eof?: unknown }>('workspaceFiles/read', {
      workspaceFileScopeId: this.sessionId,
      path,
      range: {},
    }, { retryAuth: false })
    return { text: str(value?.text) ?? '', eof: value?.eof !== false }
  }

  private async killJob(taskId: string): Promise<boolean> {
    if (this.disposed || !this.connected) return false
    try {
      const value = await this.endpoint.rpc<{ outcome?: unknown }>('job/kill', {
        request: { sessionId: this.sessionId, jobId: taskId },
      }, { retryAuth: false })
      // `already-finished` is not a failure of the request: the job is over.
      return value?.outcome === 'requested' || value?.outcome === 'already-finished'
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      return false
    }
  }

  /** The parent's durable subagent catalog, as far as this mirror has seen it. */
  private subagentCatalog(): readonly { readonly id: string; readonly mode: 'one-shot' | 'continuable' | 'unknown' }[] {
    return [...this.translator.catalog].map(([id, mode]) => ({ id, mode }))
  }

  private async readState(): Promise<WireState | undefined> {
    try {
      return await this.endpoint.rpc<WireState>('session/state', { request: { sessionId: this.sessionId } }, { retryAuth: false })
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      return undefined
    }
  }

  private translateRecords(records: readonly FollowRecord[], historical: boolean): AgentEvent[] {
    const events: AgentEvent[] = []
    for (const record of records) {
      events.push(...this.translator.translateRecord(record, historical))
    }
    return events
  }

  private async applyReconnect(snapshot: FollowSnapshot): Promise<void> {
    const state = await this.readState()
    if (state !== undefined) {
      this.translator.setQueue(queueFromState(state))
      const nextStatus = state.running === true ? 'running' : 'idle'
      if (nextStatus !== this.status) {
        this.status = nextStatus
        this.emit([{ type: 'session.status', status: this.status }])
      }
    }
    const records = snapshot.records ?? []
    const oldest = minSeq(records)
    // Continuous when the reopened window still reaches back to the last
    // durable seq this handle saw; otherwise every retained record is new.
    const continuity = this.translator.lastSeq === -1 || (oldest !== undefined && oldest <= this.translator.lastSeq)
    if (continuity) {
      this.translator.newGeneration()
      const fresh = this.translateRecords(records, true)
      if (fresh.length > 0) this.emit(fresh)
      if (oldest !== undefined) this.oldestSeq = Math.min(this.oldestSeq ?? oldest, oldest)
      if (snapshot.hasMore === true) this.olderMore = true
      return
    }
    // Beyond the retained window: repaint the Host's new baseline.
    const replacement = new RemoteTranslator(attachment => this.readAttachment(attachment))
    const events = records.flatMap(record => replacement.translateRecord(record, true))
    this.translator.resetTo(replacement.lastSeq)
    this.translator.setQueue(queueFromState(state))
    this.emit([{ type: 'session.reset', trigger: 'reconnect' }])
    this.listener?.(events, { replay: true })
    if (this.listener === undefined) this.buffered.push(...events)
    this.older.splice(0)
    this.recorded = [...this.historyEvents, ...events]
    this.oldestSeq = oldest
    this.olderMore = snapshot.hasMore === true
    // A fresh baseline is the moment to settle sends whose receipt the old
    // connection swallowed: the queue and the log are both readable again.
    await this.reconcileUnconfirmed()
  }

  private takeOlder(): readonly AgentEvent[] {
    if (this.older.length === 0) {
      void this.prefetchOlder()
      return []
    }
    const slice = this.older.splice(0)
    void this.prefetchOlder()
    return slice
  }

  private async prefetchOlder(): Promise<void> {
    if (this.prefetching || !this.olderMore || this.disposed || !this.connected) return
    if (this.oldestSeq === undefined) return
    this.prefetching = true
    try {
      const value = await this.endpoint.rpc<PageValue>('session/page', {
        request: {
          address: { kind: 'session', sessionId: this.sessionId },
          beforeSeq: this.oldestSeq,
          maxMessages: 30,
        },
      }, { retryAuth: false })
      const records = value.records ?? []
      const translator = new RemoteTranslator(attachment => this.readAttachment(attachment))
      const events = records.flatMap(record => translator.translateRecord(record, true))
      const oldest = minSeq(records)
      if (oldest !== undefined) this.oldestSeq = oldest
      this.olderMore = value.hasMore === true
      this.older.unshift(...events)
      this.emit([{ type: 'session.status', status: this.status === 'running' ? 'running' : 'idle' }])
    } catch (error) {
      if (error instanceof RemoteConnectionLost) this.lose(error)
      this.emit([{ type: 'notice', level: 'warning', text: `Could not load earlier history: ${error instanceof Error ? error.message : String(error)}` }])
    } finally {
      this.prefetching = false
    }
  }
}

const minSeq = (records: readonly FollowRecord[]): number | undefined => {
  let result: number | undefined
  for (const record of records) {
    const seq = record.event?.seq
    if (typeof seq === 'number' && (result === undefined || seq < result)) result = seq
  }
  return result
}
