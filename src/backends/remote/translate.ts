/**
 * DSH Web wire records -> neutral AgentEvents. The durable `session/follow`
 * records are the authority; transient assistant frames only sketch the live
 * attempt that the durable `assistant/message` settles.
 */
import type { AgentEvent, AgentEventOf, AgentEventType, AssistantBlock, ContentBlockView, ImageRef, PendingItem, TurnEndReason, UsageDelta } from '../../agent/events.js'
import type { QueueItem } from './session.js'

type JsonRecord = Record<string, unknown>

interface WireEvent {
  readonly type: string
  readonly seq: number
  readonly time: number
  readonly data: unknown
}

export interface FollowRecord {
  readonly type?: string
  readonly event?: WireEvent
}

interface AssistantFrame {
  readonly type: string
  readonly attemptId: string
  readonly revision: number
  readonly turn?: number
  readonly step?: number
  readonly index?: number
  readonly time?: number
  readonly chunk?: unknown
  readonly startedAfterSeq?: number
  readonly outcome?: { readonly kind?: unknown; readonly eventType?: unknown }
}

const isRecord = (value: unknown): value is JsonRecord =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const str = (value: unknown): string | undefined => typeof value === 'string' ? value : undefined
const num = (value: unknown): number | undefined => typeof value === 'number' && Number.isFinite(value) ? value : undefined

const textOf = (content: unknown, firstOnly = false): string => {
  if (!Array.isArray(content)) return ''
  const blocks = content.flatMap(block => isRecord(block) && block.type === 'text' && typeof block.text === 'string' ? [block.text] : [])
  return (firstOnly ? (blocks[0] ?? '') : blocks.join('')).trim()
}

/**
 * The image parts of one Host message, as rows that read their bytes back
 * through `session/attachment` (the durable reference the Host stored). Absent
 * reader = the handle cannot display images; the blocks still count.
 */
const imagesOf = (
  content: unknown,
  read: ((attachment: unknown) => Promise<Uint8Array>) | undefined,
  time: number,
): readonly ImageRef[] => {
  if (!Array.isArray(content)) return []
  return content.flatMap((block, index): ImageRef[] => {
    if (!isRecord(block) || block.type !== 'image') return []
    const attachment = isRecord(block.attachment) ? block.attachment : block
    const id = str(attachment.id)
    if (id === undefined || read === undefined) return []
    const mediaType = str(attachment.mediaType) ?? str(block.mediaType)
    const name = str(attachment.name) ?? str(block.name)
    const width = num(attachment.width) ?? 0
    const height = num(attachment.height) ?? 0
    return [{
      id,
      width,
      height,
      ...(name === undefined ? {} : { name }),
      ...(mediaType === undefined ? {} : { mediaType }),
      ...(num(attachment.bytes) === undefined ? {} : { bytes: num(attachment.bytes) }),
      read: async () => read(attachment),
      ...(time === 0 ? {} : {}),
    }]
  })
}

const blocksOf = (content: unknown): readonly ContentBlockView[] =>
  Array.isArray(content)
    ? content.flatMap((block): ContentBlockView[] => {
      if (!isRecord(block) || typeof block.type !== 'string') return []
      return [{ type: block.type, ...(typeof block.text === 'string' ? { text: block.text } : {}) }]
    })
    : []

const usageDelta = (usage: unknown): UsageDelta | undefined => {
  if (!isRecord(usage)) return undefined
  const fields: UsageDelta = {
    ...(num(usage.inputTokens) === undefined ? {} : { input: num(usage.inputTokens) }),
    ...(num(usage.outputTokens) === undefined ? {} : { output: num(usage.outputTokens) }),
    ...(num(usage.cacheReadTokens) === undefined ? {} : { cacheRead: num(usage.cacheReadTokens) }),
    ...(num(usage.cacheWriteTokens) === undefined ? {} : { cacheWrite: num(usage.cacheWriteTokens) }),
  }
  return Object.keys(fields).length === 0 ? undefined : fields
}

const turnReason = (reason: unknown): TurnEndReason => {
  const kind = isRecord(reason) ? str(reason.kind) : undefined
  switch (kind) {
    case 'completed': return { kind: 'completed' }
    case 'interrupted': return { kind: 'interrupted' }
    case 'blocked': return { kind: 'blocked' }
    case 'aborted': return { kind: 'aborted' }
    case 'error': {
      const error = isRecord(reason) && isRecord(reason.error) ? reason.error : undefined
      return { kind: 'error', message: str(error?.message) ?? 'the model call failed' }
    }
    default: return { kind: 'other', label: kind ?? 'unknown' }
  }
}

const deltaOf = (chunk: unknown): AgentEventOf<'assistant.delta'>['delta'] => {
  if (!isRecord(chunk)) return { kind: 'other' }
  switch (chunk.type) {
    case 'text-delta': return { kind: 'text', text: str(chunk.text) ?? '' }
    case 'reasoning-delta': return { kind: 'reasoning', text: str(chunk.text) ?? '' }
    case 'tool-call-delta': return {
      kind: 'tool-args',
      callId: str(chunk.id) ?? '',
      partialJson: str(chunk.argumentsDelta) ?? '',
      ...(str(chunk.name) === undefined ? {} : { name: str(chunk.name) }),
    }
    default: return { kind: 'other' }
  }
}

const pendingText = (item: unknown): string => {
  if (!isRecord(item)) return ''
  return textOf(item.content)
}

/** One session's incremental translator. */
/**
 * The Remote Web backend's decision for every Agent Domain event type (checked
 * by `verify:agent-domain`): a new `AgentEvent` variant fails `tsc` here until
 * this backend decides whether the Host can make it happen. The Host owns the
 * turn, so only durable Host facts, the interactions it asks this client to
 * settle, and the notices this client raises itself are emitted; everything
 * local (plan mode, goals, tasks, models, commands) stays unemitted.
 */
export function remoteEmits(type: AgentEventType): boolean {
  switch (type) {
    case 'session.ready':
    case 'session.title':
    case 'session.cwd':
    case 'session.reset':
    case 'session.status':
    case 'turn.start':
    case 'turn.end':
    case 'step.start':
    case 'step.end':
    case 'user.message':
    case 'pending.changed':
    case 'assistant.attempt.start':
    case 'assistant.attempt.end':
    case 'assistant.delta':
    case 'assistant.message':
    case 'tool.call':
    case 'tool.result':
    case 'request.header':
    case 'context.capacity':
    case 'system.prompt':
    case 'permission.request':
    case 'permission.settled':
    case 'question.request':
    case 'question.settled':
    case 'notice':
      return true
    case 'session.color':
    case 'usage':
    case 'tool.progress':
    case 'tool.output':
    case 'subagent.start':
    case 'subagent.progress':
    case 'subagent.end':
    case 'task.start':
    case 'task.update':
    case 'task.output':
    case 'task.end':
    case 'tasks.snapshot':
    case 'compaction.start':
    case 'compaction.progress':
    case 'compaction.end':
    case 'context.usage':
    case 'model.changed':
    case 'effort.changed':
    case 'mode.changed':
    case 'commands.changed':
    case 'goal.change':
    case 'todo.write':
    case 'preset.selected':
    case 'agent.message':
    case 'rate-limit':
    case 'custom':
      return false
    default: {
      const unhandled: never = type
      return unhandled
    }
  }
}

export class RemoteTranslator {
  /** Reads a durable image's bytes from the Host; absent = no image rows. */
  constructor(private readonly readAttachment?: (attachment: unknown) => Promise<Uint8Array>) {}
  private readonly pending: QueueItem[] = []
  /** Durable child catalog seen so far (`subagent/catalog`), for addressing. */
  readonly catalog = new Map<string, 'one-shot' | 'continuable' | 'unknown'>()
  /** Durable request ids: proof that a submission reached the Host log. */
  private readonly seenRequestIds = new Set<string>()
  private lastRevision = -1
  private readonly attemptPosition = new Map<string, { readonly turn: number; readonly step: number }>()
  /** Highest durable seq this translator has seen. */
  lastSeq = -1

  queue(): readonly QueueItem[] { return this.pending }

  /** Replace the pending mirror with `session/state`'s authoritative queue. */
  setQueue(items: readonly QueueItem[]): void {
    this.pending.splice(0, this.pending.length, ...items)
  }

  /** Forget one row the Host confirmed it dropped, so a second retraction of
   *  the same row reports "already gone" instead of firing another update. */
  dropQueued(clientMessageId: string): void {
    const index = this.pending.findIndex(item => item.clientMessageId === clientMessageId)
    if (index !== -1) this.pending.splice(index, 1)
  }

  /** Translate one durable record. Historical records never mutate the live
   *  pending mirror: an old claim/drop is not a current state change. */
  translateRecord(record: FollowRecord, historical: boolean): readonly AgentEvent[] {
    if (record.type !== 'event' || record.event === undefined) return []
    const event = record.event
    if (event.seq <= this.lastSeq && this.lastSeq !== -1) return []
    this.lastSeq = Math.max(this.lastSeq, event.seq)
    return this.translateEvent(event, historical)
  }

  /** Translate one `session/follow` live frame. */
  translateAssistantFrame(frame: AssistantFrame): readonly AgentEvent[] {
    if (frame.revision <= this.lastRevision) return []
    this.lastRevision = frame.revision
    if (frame.type === 'start') {
      const position = { turn: num(frame.turn) ?? 0, step: num(frame.step) ?? 0 }
      this.attemptPosition.set(frame.attemptId, position)
      return [{ type: 'assistant.attempt.start', attemptId: frame.attemptId, turn: position.turn, step: position.step }]
    }
    if (frame.type === 'end') {
      const outcome = frame.outcome
      const discarded = isRecord(outcome) && (outcome.kind === 'abandoned' || outcome.eventType === 'assistant/attempt')
      return [{ type: 'assistant.attempt.end', attemptId: frame.attemptId, outcome: discarded ? 'abandoned' : 'committed' }]
    }
    const position = this.attemptPosition.get(frame.attemptId)
    return [{
      type: 'assistant.delta',
      attemptId: frame.attemptId,
      index: num(frame.index) ?? 0,
      time: num(frame.time) ?? Date.now(),
      delta: deltaOf(frame.chunk),
      ...(position === undefined ? {} : position),
    }]
  }

  /** Reset stream revision tracking on a new physical generation. */
  newGeneration(): void {
    this.lastRevision = -1
    this.attemptPosition.clear()
  }

  /** Replace this mirror's durable position after a full baseline repaint. */
  resetTo(seq: number): void {
    this.lastSeq = seq
    this.newGeneration()
  }

  private translateEvent(event: WireEvent, historical: boolean): readonly AgentEvent[] {
    const data = event.data
    switch (event.type) {
      case 'turn/start':
        return [{ type: 'turn.start', turn: num(isRecord(data) ? data.turn : undefined) ?? 0, origin: 'user', time: event.time }]
      case 'turn/end': {
        const record = isRecord(data) ? data : {}
        return [{ type: 'turn.end', turn: num(record.turn) ?? 0, reason: turnReason(record.reason), time: event.time }]
      }
      case 'step/start': {
        const record = isRecord(data) ? data : {}
        return [{ type: 'step.start', turn: num(record.turn) ?? 0, step: num(record.step) ?? 0 }]
      }
      case 'step/end': {
        const record = isRecord(data) ? data : {}
        return [{ type: 'step.end', turn: num(record.turn) ?? 0, step: num(record.step) ?? 0 }]
      }
      case 'user/message': {
        const message = isRecord(data) ? data : {}
        const source = isRecord(message.source) ? message.source : {}
        const id = str(message.id) ?? String(event.seq)
        const rpcId = str(source.rpcId)
        const sourceKind = str(source.kind) ?? 'injected'
        const mapSource = (): 'user' | 'injected' | 'goal' | 'compaction' | 'command-output' | 'notification' | 'team-message' => {
          if (sourceKind === 'user') return 'user'
          if (sourceKind === 'goal') return 'goal'
          if (sourceKind === 'compaction') return 'compaction'
          if (sourceKind === 'command-output') return 'command-output'
          if (sourceKind === 'notification') return 'notification'
          if (sourceKind === 'team-message') return 'team-message'
          return 'injected'
        }
        const images = imagesOf(message.content, this.readAttachment, event.time)
        const events: AgentEvent[] = [{
          type: 'user.message',
          id,
          anchor: String(event.seq),
          seq: event.seq,
          time: event.time,
          source: mapSource(),
          text: textOf(message.content, true),
          blocks: blocksOf(message.content),
          ...(images.length === 0 ? {} : { images }),
        }]
        if (rpcId !== undefined) this.seenRequestIds.add(rpcId)
        if (!historical && rpcId !== undefined) {
          const index = this.pending.findIndex(item => item.clientMessageId === rpcId)
          if (index !== -1) {
            this.pending.splice(index, 1)
            events.push(this.pendingEvent([rpcId], undefined))
          }
        }
        return events
      }
      case 'assistant/message': {
        const record = isRecord(data) ? data : {}
        const message = isRecord(record.message) ? record.message : {}
        return [{
          type: 'assistant.message',
          seq: event.seq,
          anchor: String(event.seq),
          turn: num(record.turn),
          step: num(record.step),
          attemptId: `seq:${String(event.seq)}`,
          time: event.time,
          blocks: blocksOf(message.content) as readonly AssistantBlock[],
          usage: usageDelta(record.usage),
          ...(record.interrupted === true ? { interrupted: true as const } : {}),
          canonical: Array.isArray(record.stream),
        }]
      }
      case 'assistant/attempt': {
        const record = isRecord(data) ? data : {}
        return [{ type: 'assistant.attempt.end', attemptId: `seq:${String(event.seq)}`, outcome: 'abandoned', turn: num(record.turn), step: num(record.step) }]
      }
      case 'tool/call': {
        const record = isRecord(data) ? data : {}
        const name = str(record.name) ?? 'tool'
        const callId = str(record.callId) ?? String(event.seq)
        return [{
          type: 'tool.call',
          seq: event.seq,
          anchor: String(event.seq),
          turn: num(record.turn) ?? 0,
          step: num(record.step) ?? 0,
          callId,
          name,
          argsJson: str(record.arguments) ?? '{}',
          time: event.time,
          ...(name === 'ask_user_question' ? { presentation: { card: 'question' as const } } : {}),
        }]
      }
      case 'tool/result': {
        const record = isRecord(data) ? data : {}
        const message = isRecord(record.message) ? record.message : {}
        const callId = str(message.toolCallId) ?? ''
        const isError = message.isError === true
        const error = isRecord(record.error) ? record.error : undefined
        const reason = str(error?.reason)
        const result: AgentEvent = {
          type: 'tool.result',
          seq: event.seq,
          turn: num(record.turn) ?? 0,
          step: num(record.step) ?? 0,
          callId,
          time: event.time,
          isError,
          content: blocksOf(message.content),
          text: textOf(message.content),
          ...(reason === undefined ? {} : { errorText: reason }),
          ...(record.meta === undefined ? {} : { meta: record.meta }),
        }
        return [result]
      }
      case 'request/header': {
        const record = isRecord(data) ? data : {}
        const header = isRecord(record.header) ? record.header : {}
        const config = isRecord(header.config) ? header.config : {}
        return [{
          type: 'request.header',
          ...(str(config.model) === undefined ? {} : { model: str(config.model) }),
          ...(str(config.reasoningEffort) === undefined ? {} : { effort: str(config.reasoningEffort) }),
        }]
      }
      case 'request/context': {
        const record = isRecord(data) ? data : {}
        const contextWindow = num(record.contextWindow)
        return contextWindow === undefined ? [] : [{ type: 'context.capacity', contextWindow }]
      }
      case 'system/message': {
        const record = isRecord(data) ? data : {}
        const message = isRecord(record.message) ? record.message : {}
        return [{ type: 'system.prompt', text: textOf(message.content) }]
      }
      case 'session/title': {
        const record = isRecord(data) ? data : {}
        const title = str(record.title)
        if (title === undefined || title === '') return []
        const source = isRecord(record.source) ? str(record.source.kind) : undefined
        return [{ type: 'session.title', title, source: source === 'user' ? 'user' : 'auto' }]
      }
      case 'working-directory/change': {
        const record = isRecord(data) ? data : {}
        const cwd = str(record.cwd)
        return cwd === undefined || cwd === '' ? [] : [{ type: 'session.cwd', cwd }]
      }
      case 'agent/inbox/spliced': {
        if (historical) return []
        const record = isRecord(data) ? data : {}
        return this.translateInbox(record)
      }
      case 'job/registered': {
        const record = isRecord(data) ? data : {}
        const taskId = str(record.jobId)
        if (taskId === undefined) return []
        return [{
          type: 'task.start',
          taskId,
          kind: str(record.kind) ?? 'job',
          description: str(record.label) ?? taskId,
          background: true,
          time: num(record.startedAt) ?? event.time,
        }]
      }
      case 'job/settled': {
        const record = isRecord(data) ? data : {}
        const taskId = str(record.jobId)
        if (taskId === undefined) return []
        const status = record.status === 'completed' ? 'completed' : record.status === 'killed' ? 'stopped' : 'failed'
        return [{
          type: 'task.end',
          taskId,
          status,
          ...(str(record.detail) === undefined ? {} : { summary: str(record.detail) }),
          time: num(record.finishedAt) ?? event.time,
        }]
      }
      case 'subagent/catalog': {
        // The parent's own durable child catalog: identity plus mode. A child
        // row is a background agent of this session; its own transcript is
        // read on demand by `subagents.history`.
        const record = isRecord(data) ? data : {}
        const childId = str(record.childId)
        if (childId === undefined) return []
        const mode = record.mode === 'continuable' ? 'continuable' : record.mode === 'one-shot' ? 'one-shot' : 'unknown'
        this.catalog.set(childId, mode)
        const label = str(record.label) ?? str(record.name) ?? childId
        return [{
          type: 'subagent.start',
          agentId: childId,
          description: label,
          kind: mode,
          background: true,
          depth: 1,
          time: num(record.childCreatedAt) ?? event.time,
        }]
      }
      default:
        return []
    }
  }

  private translateInbox(record: JsonRecord): readonly AgentEvent[] {
    const target = str(record.target)
    const placement: QueueItem['placement'] = target === 'next-step' ? 'steer' : 'followup'
    const start = num(record.start) ?? 0
    const removed = num(record.removedCount) ?? 0
    const inserted = Array.isArray(record.inserted) ? record.inserted : []
    const removedItems: QueueItem[] = []
    if (removed > 0) {
      // The Host serves one queue per target; find its slice.
      const positions: number[] = []
      this.pending.forEach((item, index) => { if (item.placement === placement) positions.push(index) })
      for (let offset = 0; offset < removed; offset += 1) {
        const absolute = positions[start + offset]
        if (absolute === undefined) continue
        const [item] = this.pending.splice(absolute, 1)
        if (item !== undefined) removedItems.push(item)
      }
    }
    const added: QueueItem[] = []
    inserted.forEach((message, offset) => {
      if (!isRecord(message)) return
      const clientMessageId = str(isRecord(message.source) ? message.source.rpcId : undefined) ?? str(message.id) ?? crypto.randomUUID()
      const item: QueueItem = {
        clientMessageId,
        itemId: str(message.id) ?? clientMessageId,
        placement,
        text: pendingText(message),
      }
      const positions = this.pending.flatMap((candidate, index) => candidate.placement === placement ? [index] : [])
      this.pending.splice(positions[start + offset] ?? this.pending.length, 0, item)
    })
    const ids = removedItems.map(item => item.clientMessageId)
    if (ids.length === 0) return []
    if (record.outcome === 'canceled') return [this.pendingEvent(undefined, ids)]
    return [this.pendingEvent(ids, undefined)]
  }

  /** Request ids this handle has seen as durable user messages. */
  hasSeenRequestId(clientMessageId: string): boolean {
    return this.seenRequestIds.has(clientMessageId)
  }

  /** A pending snapshot after a Host-confirmed claim or release. */
  pendingSnapshot(claimed?: readonly string[], dropped?: readonly string[]): AgentEvent {
    return this.pendingEvent(claimed, dropped)
  }

  private pendingEvent(claimed: readonly string[] | undefined, dropped: readonly string[] | undefined): AgentEvent {
    const items: PendingItem[] = this.pending.map(item => ({
      id: item.clientMessageId,
      text: item.text,
      placement: item.placement === 'steer' ? 'steer' : 'followup',
    }))
    return {
      type: 'pending.changed',
      items,
      ...(claimed === undefined ? {} : { claimed }),
      ...(dropped === undefined ? {} : { dropped }),
    }
  }
}
