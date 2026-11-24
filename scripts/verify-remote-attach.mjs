/**
 * Focused protocol regression for `--connect` (remote Web attach).
 *
 * A fake DSH Web Host stands in for a real `dsh web`: it serves `GET /connect`
 * (302 + auth cookie), the unary RPC endpoints and `/api/remote.mux`, and can
 * replay any of the situations the adapter has to survive — an opening
 * snapshot followed immediately by live records (the first-frame race), a
 * reconnect that reuses the last durable seq, a reconnect whose retained
 * window no longer reaches it (generation replacement), Host-side queue rows
 * this client never submitted, approvals and questions answered on the other
 * end, and receipts lost in flight.
 *
 * What it pins, one assertion group per case:
 *
 *   - auth: every physical generation re-authenticates, because the Host
 *     deletes the Peer when that socket closes;
 *   - opening: history carries `session.ready` + the snapshot, and live
 *     records that arrived before the subscriber attached are not lost;
 *   - streaming: `session/follow` assistant frames become attempt/delta
 *     events; revision fencing drops a replayed frame;
 *   - paging: `session/page` prepends older records without re-emitting the
 *     live window;
 *   - reconnect (continuous): resumes after the last durable seq, never
 *     re-emitting history and never skipping the gap;
 *   - reconnect (gap): emits `session.reset` and replays the new baseline;
 *   - queue: the Host is authoritative — `items` materialise rows this client
 *     did not submit, and retraction addresses the Host `itemId`;
 *   - approvals/questions: answers carry the Host event id and the original
 *     question ids, never display positions;
 *   - lost receipt: an unknown outcome keeps the input (no silent drop) and
 *     never re-sends;
 *   - dispose: releasing the TUI never cancels the Host turn;
 *   - launcher shape: `--connect` refuses a value, because a bare positional
 *     would reach the entry as the initial prompt and submit the launch token
 *     into the session log.
 *
 * Run after build: `npm run verify:remote-attach` (in `verify:build`).
 */
import { spawnSync } from 'node:child_process'
import { createServer } from 'node:http'
import { WebSocketServer } from 'ws'
import { RemoteEndpoint } from '../lib/types/backends/remote/protocol.js'
import { RemoteDshSession } from '../lib/types/backends/remote/session.js'
import { remoteBackend } from '../lib/types/backends/remote/backend.js'
import { remoteEmits } from '../lib/types/backends/remote/translate.js'

const SESSION_ID = 'session-verify'
const HOST_WORKSPACE = '/host/work'
const HOST_CWD = '/tmp/remote-verify'

/** Everything the fake Host remembers across sockets. */
const host = {
  /** Monotonic generation: each `/connect` mints a fresh cookie. */
  generations: 0,
  /** RPC calls in arrival order, for wire-shape assertions. */
  calls: [],
  /** Forwarded-event answers the adapter sent back. */
  answers: [],
  /** Sessions the client subscribed to. */
  follows: 0,
  /** Queue as the Host would report it. */
  queue: [
    { id: 'host-item-1', rpcId: 'client-from-browser', placement: 'queued', content: [{ type: 'text', text: 'queued in the browser' }] },
  ],
  /** Whether `session/cancel` was ever asked for. */
  cancelled: false,
  /** Whether a prompt RPC ever arrived more than once for one request id. */
  prompts: [],
  /** `session/page` windows, newest first (built after the helpers exist). */
  pages: [],
  /** How the next `session/follow` snapshot should look. */
  followMode: 'initial',
  /** RPC result substitutions by method, consumed once each. */
  failures: new Map(),
  /** Mutations the Host accepted, in order (`session/updateQueue`). */
  updateQueue: [],
  /** Refuse the next `session/updateQueue` instead of applying it. */
  failUpdateQueue: undefined,
  /** Accept the next prompt durably, then drop the socket without a reply. */
  hangPrompt: false,
  /** Drop the next prompt's socket before the Host takes it, with no reply. */
  hangPromptUnqueued: false,
  /** Sessions the Host created through `session/create`. */
  creates: [],
  /** Titles the Host applied through `session/rename`. */
  renames: [],
  /** Forks the Host performed. */
  forks: [],
  /** Model selections the Host resolved. */
  selections: [],
  /** Refuse the next `session/selectModel` (a model this Host cannot route). */
  failSelectModel: false,
  /** Workspace directories the Host reports on `workspace/follow`. */
  workspaces: [],
  /** Refuse the first-use default Workspace (no system Documents directory). */
  failInitializeDefault: false,
  /** Slash lines the Host registry executed (`commands/execute`). */
  commands: [],
  /** Child interrupts the Host accepted (`subagents/interruptByUser`). */
  interrupts: [],
  /** Job kill requests the Host accepted (`job/kill`). */
  kills: [],
  /** `session/page` requests (subagent transcripts address parent + child). */
  pageRequests: [],
  /** The child transcript the Host serves for a subagent address. */
  childRecords: [],
  /** Directory listings the Host served (`workspaceFiles/list`). */
  listings: [],
  /** Text reads the Host served (`workspaceFiles/read`). */
  reads: [],
  /** Durable image reads the Host served (`session/attachment`). */
  attachmentReads: [],
  /** Prompts the Host accepted, with their content parts. */
  promptContents: [],
  /** Host terminal inventory the fake Host reports. */
  terminals: [],
  terminalEnvironment: { cwd: HOST_WORKSPACE, maxCols: 500, maxRows: 200, scrollback: 1000 },
  terminalShells: [{ name: 'bash', path: '/bin/bash', args: ['-i'] }],
  /** Calls the fake Host saw for the terminal inventory. */
  terminalListCalls: [],
  terminalEnvironmentCalls: [],
  terminalShellCalls: [],
  /** Terminal operations this client must never issue. */
  terminalCreates: 0,
  terminalAttaches: 0,
  /** Directory listings by workspace-relative path. */
  directories: {},
  /** Stat-able entries by workspace-relative path. */
  files: {},
  /** Text files by workspace-relative path. */
  texts: {},
}

const user = text => ({ id: `u-${text}`, source: { kind: 'user' }, content: [{ type: 'text', text }] })

host.pages = [
  { beforeSeq: 6, records: [record(6, user('older-6'))], hasMore: true },
  { beforeSeq: 4, records: [record(4, user('older-4')), record(5, user('older-5'))], hasMore: true },
  { beforeSeq: 1, records: [record(1, user('older-1'))], hasMore: false },
]

function record(seq, data, time = 1_700_000_000_000 + seq) {
  return { type: 'event', event: { type: 'user/message', seq, time, data } }
}

function snapshot(records, { hasMore = false } = {}) {
  return {
    type: 'snapshot',
    header: { cwd: HOST_CWD },
    projections: { values: { title: 'Verify', modelSelection: { lastUsed: { model: 'verify-model' } } } },
    cursor: { seq: records.at(-1)?.event.seq ?? 0 },
    hasMore,
    assistantStream: true,
    records,
  }
}

const reply = (res, message, value) => {
  res.writeHead(200, { 'content-type': 'application/json' })
  res.end(JSON.stringify({ type: 'server-response', rpcId: message.rpcId, result: { ok: true, value } }))
}

/** The gateway's failure envelope: `ok:false` with a coded error. */
const replyError = (res, message, code, text) => {
  res.writeHead(200, { 'content-type': 'application/json' })
  res.end(JSON.stringify({ type: 'server-response', rpcId: message.rpcId, result: { ok: false, error: { code, message: text } } }))
}

const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1')
  if (url.pathname === '/connect') {
    if (url.searchParams.get('token') !== 'valid-token') {
      res.writeHead(403)
      res.end()
      return
    }
    host.generations += 1
    res.writeHead(302, {
      location: './api/remote.mux',
      'set-cookie': `dsh-auth-local=v2.generation-${String(host.generations)}; Path=/; HttpOnly`,
    })
    res.end()
    return
  }
  let body = ''
  req.on('data', chunk => { body += chunk })
  req.on('end', () => {
    let message = {}
    try { message = JSON.parse(body) } catch { /* reported as a failed assertion below */ }
    const args = message.payload?.args
    host.calls.push({ method: message.method, args })
    if (message.method === 'session/prompt') host.prompts.push(args?.request)
    const failure = host.failures.get(message.method)
    if (failure !== undefined) {
      host.failures.delete(message.method)
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ type: 'server-response', rpcId: message.rpcId, result: { ok: false, error: { code: failure.code ?? 'remote/failed', message: failure.message } } }))
      return
    }
    switch (message.method) {
      case 'session/state':
        reply(res, message, { cursor: 0, running: false, queue: host.queue })
        return
      case 'session/prompt': {
        const request = args?.request
        if (host.hangPromptUnqueued) {
          host.hangPromptUnqueued = false
          res.socket.destroy()
          return
        }
        if (host.hangPrompt) {
          host.hangPrompt = false
          // The Host took the submission (it is in its queue and its log) but
          // the receipt never reaches this client.
          host.queue = [...host.queue, { id: 'host-hung', rpcId: request?.requestId, placement: request?.mode ?? 'queued', content: request?.content }]
          res.socket.destroy()
          return
        }
        host.promptContents.push({ requestId: request?.requestId, content: request?.content })
        reply(res, message, { accepted: true })
        return
      }
      case 'session/cancel':
        host.cancelled = true
        reply(res, message, {})
        return
      case 'session/updateQueue':
        if (host.failUpdateQueue === 'refused') {
          replyError(res, message, 'session/item-not-pending', 'the item is no longer pending')
          return
        }
        if (host.failUpdateQueue === 'gone') {
          replyError(res, message, 'session/item-not-found', 'the Host has no such pending item')
          return
        }
        host.updateQueue = [...(host.updateQueue ?? []), args?.request]
        host.queue = host.queue.filter(item => item.id !== args?.request?.itemId)
        reply(res, message, { ok: true, value: { accepted: true } })
        return
      case 'session/create':
        host.creates.push(args?.request)
        if (host.failCreate === true) {
          host.failCreate = undefined
          replyError(res, message, 'session/create-cwd-invalid', 'the Host cannot create a Session in that directory')
          return
        }
        reply(res, message, { sessionId: 'session-created', agentPreset: 'default' })
        return
      case 'session/rename':
        host.renames.push(args?.request)
        reply(res, message, { title: args?.request?.title, seq: 42 })
        return
      case 'session/fork':
        host.forks.push(args?.request)
        reply(res, message, { sessionId: 'session-forked' })
        return
      case 'workspace/initializeDefault':
        if (host.failInitializeDefault === true) {
          replyError(res, message, 'gateway/internal', 'system Documents directory is unavailable')
          return
        }
        reply(res, message, { workspace: { workspaceId: 'ws-default', path: '/host/default-workspace', title: 'dsh', sessionIds: [] } })
        return
      case 'commands/list':
        host.calls.push({ method: 'commands/list', args })
        reply(res, message, [
          { name: 'plan', description: 'Toggle plan mode', input: { hint: '[on|off]' } },
          { name: 'compact', description: 'Compact the Host context' },
          { name: 'ship', description: 'Ship it' },
        ])
        return
      case 'commands/execute': {
        host.commands.push(args)
        const line = String(args?.line ?? '')
        if (line.startsWith('/ship')) {
          reply(res, message, { commandId: 'cmd-1', result: { kind: 'success', text: 'shipped on the Host' } })
          return
        }
        if (line.startsWith('/plan')) {
          reply(res, message, { commandId: 'cmd-2', result: { kind: 'success', text: '' } })
          return
        }
        if (line.startsWith('/refuse')) {
          replyError(res, message, 'command/denied', 'this Host forbids that command')
          return
        }
        // The registry has no such command: the Host answers with no execution.
        reply(res, message, undefined)
        return
      }
      case 'subagents/interruptByUser':
        host.interrupts.push(args)
        reply(res, message, { accepted: true })
        return
      case 'job/kill':
        host.kills.push(args)
        reply(res, message, { outcome: 'requested' })
        return
      case 'terminal/list': {
        host.terminalListCalls.push(args)
        reply(res, message, host.terminals)
        return
      }
      case 'terminal/environment': {
        host.terminalEnvironmentCalls.push(args)
        reply(res, message, host.terminalEnvironment)
        return
      }
      case 'terminal/shells': {
        host.terminalShellCalls.push(args)
        reply(res, message, host.terminalShells)
        return
      }
      case 'workspaceFiles/list': {
        host.listings.push(args)
        const path = String(args?.path ?? '')
        const listing = host.directories[path]
        if (listing === undefined) {
          replyError(res, message, 'workspace-files/not-found', `no such directory: ${path}`)
          return
        }
        reply(res, message, { path, entries: listing, truncated: false })
        return
      }
      case 'workspaceFiles/stat': {
        const path = String(args?.path ?? '')
        const entry = host.files[path]
        if (entry === undefined) {
          replyError(res, message, 'workspace-files/not-found', `no such file: ${path}`)
          return
        }
        reply(res, message, { type: entry.type, absolutePath: `${HOST_WORKSPACE}/${path}`.replace(/\/$/, ''), version: 'v1', ...(entry.bytes === undefined ? {} : { bytes: entry.bytes }) })
        return
      }
      case 'workspaceFiles/read': {
        host.reads.push(args)
        const path = String(args?.path ?? '')
        const file = host.texts[path]
        if (file === undefined) {
          replyError(res, message, 'workspace-files/not-found', `no such file: ${path}`)
          return
        }
        reply(res, message, { text: file, offset: 1, lines: file.split('\n').length, eof: true, absolutePath: `${HOST_WORKSPACE}/${path}`, version: 'v1' })
        return
      }
      case 'session/attachment': {
        host.attachmentReads.push(args)
        reply(res, message, { attachment: args?.request?.attachment, data: 'aGVsbG8taW1hZ2U=' })
        return
      }
      case 'session/modelCatalog':
        reply(res, message, {
          default: { provider: 'deepseek-official', model: 'deepseek-chat' },
          routableProviders: ['deepseek-official', 'relay'],
          groups: [
            {
              id: 'deepseek-official',
              name: 'DeepSeek',
              models: [
                { id: 'deepseek-chat', name: 'DeepSeek Chat' },
                {
                  id: 'deepseek-reasoner',
                  name: 'DeepSeek Reasoner',
                  description: 'thinks first',
                  reasoning: { efforts: [{ id: 'low', name: 'Low' }, { id: 'high', name: 'High' }], defaultEffort: 'high' },
                },
              ],
            },
            { id: 'relay', name: 'Relay', models: [{ id: 'relay-large', name: 'Relay Large' }] },
          ],
          failures: [],
        })
        return
      case 'session/selectModel': {
        const request = args?.request
        host.selections.push(request)
        if (host.failSelectModel) {
          host.failSelectModel = false
          replyError(res, message, 'session/model-unavailable', 'the Host cannot route that model')
          return
        }
        reply(res, message, {
          selected: {
            provider: request?.provider,
            model: request?.model,
            ...(request?.reasoningEffort === undefined ? {} : { reasoningEffort: request.reasoningEffort }),
          },
        })
        return
      }
      case '$events/result':
        host.answers.push(args)
        reply(res, message, {})
        return
      case 'session/page': {
        host.pageRequests.push(args)
        if (args?.request?.address?.kind === 'subagent') {
          reply(res, message, { records: host.childRecords, hasMore: false })
          return
        }
        const page = host.pages.find(entry => entry.beforeSeq === args?.request?.beforeSeq)
        reply(res, message, page === undefined ? { records: [], hasMore: false } : { records: page.records, hasMore: page.hasMore })
        return
      }
      default:
        reply(res, message, {})
    }
  })
})

const wss = new WebSocketServer({ noServer: true })

/** Stream ids are per socket; the waterfall stream is the `$events` one. */
const socketOf = { events: '', follow: '' }
/** Push one live frame onto the current `session/follow` stream. */
let feedFollow = () => {}

server.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, ws => {
    const send = (streamId, value) => { ws.send(JSON.stringify({ type: 'item', streamId, value })) }
    ws.on('message', raw => {
      let frame = {}
      try { frame = JSON.parse(String(raw)) } catch { return }
      if (frame.type !== 'open') return
      if (frame.endpoint === '$events') {
        socketOf.events = frame.streamId
        send(frame.streamId, { type: 'ready', clientId: `client-${String(host.generations)}`, host: { home: '/tmp' } })
        return
      }
      if (frame.endpoint === 'workspace/follow') {
        send(frame.streamId, { type: 'baseline', value: { items: host.workspaces.map(path => ({ workspaceId: `ws-${path}`, path, title: path, sessionIds: [] })), archivedSessionIds: [], pinnedSessionIds: [] } })
        return
      }
      if (frame.endpoint !== 'session/follow') return
      socketOf.follow = frame.streamId
      feedFollow = value => send(frame.streamId, value)
      host.follows += 1
      if (host.followMode === 'initial') {
        send(frame.streamId, snapshot([record(1, user('history-1')), record(2, user('history-2'))], { hasMore: true }))
        // Live records land immediately: the adapter must buffer them until a
        // subscriber attaches, or the first paint loses them.
        send(frame.streamId, record(3, user('live-3')))
        send(frame.streamId, { type: 'assistant-stream', frame: { type: 'start', attemptId: 'attempt-1', revision: 1, turn: 1, step: 1 } })
        send(frame.streamId, { type: 'assistant-stream', frame: { type: 'chunk', attemptId: 'attempt-1', revision: 2, index: 0, time: 1_700_000_000_100, chunk: { kind: 'text', text: 'streamed' } } })
        // A replayed revision must be dropped by the revision fence.
        send(frame.streamId, { type: 'assistant-stream', frame: { type: 'chunk', attemptId: 'attempt-1', revision: 2, index: 0, time: 1_700_000_000_100, chunk: { kind: 'text', text: 'streamed' } } })
        setTimeout(() => {
          send(socketOf.events, { type: 'waterfall', eventId: 'approval-1', event: 'approval/request', agentId: SESSION_ID, request: { toolName: 'bash', callId: 'call-1', reason: 'run a command' } })
          send(socketOf.events, { type: 'waterfall', eventId: 'question-1', event: 'user-questions/request', agentId: SESSION_ID, request: { questions: [{ id: 'q-1', question: 'Pick one', options: [{ label: 'A' }, { label: 'B' }] }, { id: 'q-2', question: 'Free text', options: [] }], wait: { callId: 'qcall-1' } } })
          // A waterfall for someone else's session must be handed back.
          send(socketOf.events, { type: 'waterfall', eventId: 'foreign-1', event: 'approval/request', agentId: 'session-other', request: { toolName: 'bash' } })
        }, 20)
        return
      }
      if (host.followMode === 'continuous') {
        // The retained window still reaches the last durable seq (4 here).
        send(frame.streamId, snapshot([record(3, user('live-3')), record(4, user('gap-4'))]))
        return
      }
      if (host.followMode === 'gap') {
        // Beyond the retained window: nothing this client saw survives.
        send(frame.streamId, snapshot([record(20, user('replacement-20')), record(21, user('replacement-21'))]))
      }
    })
  })
})

await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
const baseUrl = `http://127.0.0.1:${String(server.address().port)}`
const hostPort = { cwd: HOST_CWD, locale: () => 'en', debug: () => {}, warn: () => {} }

let failures = 0
const check = (name, ok, detail) => {
  if (ok) {
    console.log(`PASS: ${name}`)
    return
  }
  failures += 1
  console.log(`FAIL: ${name}${detail === undefined ? '' : `  (${detail})`}`)
}
const wait = ms => new Promise(resolve => { setTimeout(resolve, ms) })
const textsOf = batch => batch.flatMap(event => event.type === 'user.message' ? [event.text] : [])

// ── authentication ─────────────────────────────────────────────────────────

const badToken = new RemoteEndpoint(baseUrl, 'expired-token')
let refused = false
try {
  await badToken.authenticate()
} catch {
  refused = true
}
check('an expired launch token is refused, not silently used', refused)

const endpoint = new RemoteEndpoint(baseUrl, 'valid-token')
await endpoint.authenticate()
const firstGeneration = host.generations
await endpoint.authenticate()
check('each authentication mints a fresh Peer cookie', host.generations === firstGeneration + 1)

// ── opening snapshot + first-frame race ────────────────────────────────────

const session = await RemoteDshSession.open(SESSION_ID, new RemoteEndpoint(baseUrl, 'valid-token'), hostPort)
const live = []
session.subscribe((batch, meta) => { live.push({ batch, meta }) })
await wait(120)

const history = await session.history()
check('history opens with session.ready', history[0]?.type === 'session.ready', JSON.stringify(history.slice(0, 2).map(event => event.type)))
check('history carries the snapshot records', history.flatMap(event => event.type === 'user.message' ? [event.text] : []).join() === 'history-1,history-2')
check('the snapshot header supplies cwd, not the local one', session.cwd === HOST_CWD, session.cwd)
check('records that landed before the subscriber are not lost', live.flatMap(entry => textsOf(entry.batch)).includes('live-3'))
check('the opening snapshot is not replayed twice', live.flatMap(entry => textsOf(entry.batch)).filter(text => text === 'history-1').length === 0)

const streamed = live.flatMap(entry => entry.batch).filter(event => event.type === 'assistant.delta')
check('assistant frames become streaming deltas', streamed.length === 1, JSON.stringify(streamed))
check('the revision fence drops a replayed frame', live.flatMap(entry => entry.batch).filter(event => event.type === 'assistant.attempt.start').length === 1)

// ── interactions: Host ids, two-end settlement ─────────────────────────────

const approval = session.capabilities.permissions?.pending() ?? []
check('the bound session exposes the Host approval', approval.some(request => request.requestId === 'approval-1'), JSON.stringify(approval))
check('a foreign session waterfall is handed back', host.answers.some(answer => answer.eventId === 'foreign-1' && answer.outcome?.kind === 'next'), JSON.stringify(host.answers))

session.capabilities.permissions?.respond('approval-1', { kind: 'allow-once' })
await wait(60)
check('an approval answer is the tagged Host outcome vocabulary',
  host.answers.some(answer => answer.eventId === 'approval-1'
    && answer.outcome?.kind === 'result' && answer.outcome.value === 'allowed-once'),
  JSON.stringify(host.answers))
check('a settled approval leaves no pending row', session.capabilities.permissions?.pending().length === 0)

session.capabilities.questions?.respond('question-1', { answers: [{ selected: ['A'] }, { selected: [], custom: 'typed' }] })
await wait(60)
const answered = host.answers.find(answer => answer.eventId === 'question-1')
check('a question answer keeps the Host question ids', answered?.outcome?.kind === 'result' && answered.outcome.value.answers.map(entry => entry.id).join() === 'q-1,q-2', JSON.stringify(answered))

// ── queue authority and retraction ─────────────────────────────────────────

const pendingQueue = session.capabilities.pendingQueue
check('the Host queue capability is offered', pendingQueue !== undefined)

// A Host refusal is a refusal: the preview must not look withdrawn.
host.failUpdateQueue = 'refused'
const refusedMutation = await pendingQueue?.remove('client-from-browser')
check('a refused queue mutation is not a retraction', refusedMutation?.kind === 'rejected', JSON.stringify(refusedMutation))
host.failUpdateQueue = undefined

const firstRemoval = await pendingQueue?.remove('client-from-browser')
check('a queue row this client did not submit is retractable', firstRemoval?.kind === 'accepted', JSON.stringify(firstRemoval))
check('retraction addresses the Host item id, not the client id', host.updateQueue?.some(request => request?.itemId === 'host-item-1') === true, JSON.stringify(host.updateQueue))

const secondRemoval = await pendingQueue?.remove('client-from-browser')
check('a row the Host already accepted removal for is not reported again', secondRemoval?.kind === 'gone', JSON.stringify(secondRemoval))

// ── cancel keeps the Host queue ────────────────────────────────────────────

host.queue = [
  { id: 'host-item-2', rpcId: 'client-queued-2', placement: 'queued', content: [{ type: 'text', text: 'also queued' }] },
]
const beforeCancelMutations = host.updateQueue.length
const receipt = await session.cancel('interrupt')
check('an interrupt cancels the turn on the Host', host.cancelled === true)
check('an interrupt does not delete the Host queue', host.updateQueue.length === beforeCancelMutations)
check('the cancel receipt reports the Host queue as still live',
  receipt.stillQueued.includes('client-queued-2'), JSON.stringify(receipt))
check('the cancel receipt repaints the Host queue as previews',
  live.flatMap(entry => entry.batch).some(event => event.type === 'pending.changed'
    && (event.items ?? []).some(item => item.id === 'client-queued-2')),
  JSON.stringify(live.flatMap(entry => entry.batch).filter(event => event.type === 'pending.changed')))
host.cancelled = false

// ── queue edit and steer use the official actions ──────────────────────────

host.queue = [
  { id: 'host-item-2', rpcId: 'client-queued-2', placement: 'queued', content: [{ type: 'text', text: 'also queued' }] },
]
// A cancel receipt reads the Host queue back into this client's mirror.
await session.cancel('interrupt')
host.cancelled = false

const editOutcome = await pendingQueue?.edit?.('client-queued-2', 'edited text')
const editCall = host.updateQueue.at(-1)
check('a queue edit is accepted by the Host', editOutcome?.kind === 'accepted', JSON.stringify(editOutcome))
check('a queue edit replaces the text in place',
  editCall?.action?.kind === 'edit' && editCall.action.content?.[0]?.text === 'edited text', JSON.stringify(editCall))
check('a queue edit keeps the Host item id', editCall?.itemId === 'host-item-2', JSON.stringify(editCall))

const steerOutcome = await pendingQueue?.steer?.('client-queued-2')
const steerCall = host.updateQueue.at(-1)
check('a queue promotion is accepted by the Host', steerOutcome?.kind === 'accepted', JSON.stringify(steerOutcome))
check('a queue promotion uses the official steer action', steerCall?.action?.kind === 'steer', JSON.stringify(steerCall))

// ── a lost send receipt is reconciled, never re-sent ───────────────────────

host.queue = []
host.hangPrompt = true
const promptsBefore = host.prompts.length
const lost = await session.submit({ clientMessageId: 'client-lost', text: 'lost receipt', images: [] }, 'queue')
check('a lost prompt receipt is reported as unknown, not refused',
  lost.accepted === false && lost.outcome === 'unknown', JSON.stringify(lost))
check('a lost receipt is not handed back as a draft', lost.dropped !== true)
check('a lost prompt was sent exactly once', host.prompts.length === promptsBefore + 1)

// The Host did take it: the reconcile pass must find it in the queue and keep
// the preview, without a second `session/prompt`.
await wait(4600)
check('reconciliation finds the queued submission and does not re-send',
  host.prompts.length === promptsBefore + 1)
const droppedByReconcile = live.flatMap(entry => entry.batch)
  .filter(event => event.type === 'pending.changed')
  .flatMap(event => event.dropped ?? [])
check('a submission the Host queued is never released as a draft', !droppedByReconcile.includes('client-lost'))

// ── a send the Host never took is released as a draft ──────────────────────

host.queue = []
host.hangPromptUnqueued = true
live.length = 0
const lostNever = await session.submit({ clientMessageId: 'client-never', text: 'never arrived', images: [] }, 'queue')
check('a prompt the Host never answered is unknown', lostNever.outcome === 'unknown', JSON.stringify(lostNever))
await wait(4600)
const afterReconcile = live.flatMap(entry => entry.batch)
check('a submission in neither the queue nor the log is released as a draft',
  afterReconcile.some(event => event.type === 'pending.changed' && (event.dropped ?? []).includes('client-never')),
  JSON.stringify(afterReconcile.filter(event => event.type === 'pending.changed')))
check('the release says the Host did not receive the message',
  afterReconcile.some(event => event.type === 'notice' && event.key === 'remote-unconfirmed'),
  JSON.stringify(afterReconcile.filter(event => event.type === 'notice')))

// ── paging ─────────────────────────────────────────────────────────────────

let older = session.capabilities.transcript?.older() ?? []
if (older.length === 0) {
  await wait(150)
  older = session.capabilities.transcript?.older() ?? []
}
check('paging prepends the older window', older.flatMap(event => event.type === 'user.message' ? [event.text] : []).includes('older-1'), JSON.stringify(older.map(event => event.type)))
check('paging asks for records before the oldest held seq', host.calls.some(call => call.method === 'session/page' && call.args?.request?.beforeSeq === 1), JSON.stringify(host.calls.filter(call => call.method === 'session/page').map(call => call.args?.request)))

// ── prompt submission and a lost receipt ───────────────────────────────────

const accepted = await session.submit({ text: 'from the tui', clientMessageId: 'client-tui-1' }, 'followup')
check('a prompt is accepted once', accepted.accepted === true && host.prompts.filter(prompt => prompt?.requestId === 'client-tui-1').length === 1, JSON.stringify(host.prompts))
check('the prompt uses the Host queue vocabulary', host.prompts.at(-1)?.mode === 'queue', JSON.stringify(host.prompts.at(-1)))

host.failures.set('session/prompt', { code: 'remote/unknown', message: 'the Host closed the request' })
const refusedPrompt = await session.submit({ text: 'lost receipt', clientMessageId: 'client-tui-2' }, 'steer')
check('a refused prompt is reported, not treated as sent', refusedPrompt.accepted === false)
check('the refusal keeps the text in the composer', refusedPrompt.dropped === true)
check('a refused prompt is not re-sent', host.prompts.filter(prompt => prompt?.requestId === 'client-tui-2').length === 1)

// ── reconnect: continuous ──────────────────────────────────────────────────

live.length = 0
host.followMode = 'continuous'
wss.clients.forEach(client => { client.close() })
await wait(1600)

check('a lost connection says so', live.flatMap(entry => entry.batch).some(event => event.type === 'notice' && event.key === 'remote-reconnect'))
check('a reconnect recovers the records it missed', live.flatMap(entry => textsOf(entry.batch)).includes('gap-4'))
check('a reconnect never replays settled history', !live.flatMap(entry => textsOf(entry.batch)).includes('history-1'))
check('a reconnect re-authenticates', host.generations >= 3, String(host.generations))

// ── reconnect: generation replacement ──────────────────────────────────────

live.length = 0
host.followMode = 'gap'
wss.clients.forEach(client => { client.close() })
await wait(1600)

check('an unreachable window resets the transcript', live.flatMap(entry => entry.batch).some(event => event.type === 'session.reset'), JSON.stringify(live.map(entry => entry.batch.map(event => event.type))))
check('the replacement baseline is replayed', live.some(entry => entry.meta.replay && textsOf(entry.batch).includes('replacement-21')))

// ── release semantics ──────────────────────────────────────────────────────

const promptsAtRelease = host.prompts.length
await session.dispose()
check('releasing the TUI never cancels the Host turn', host.cancelled === false)
await wait(100)
check('releasing the TUI does not send anything further', host.prompts.length === promptsAtRelease, JSON.stringify(host.prompts.map(prompt => prompt?.requestId).slice(promptsAtRelease)))

// Mirrors claude/codex: the exhaustive table may never under-claim, or a
// projection would receive an event the backend said it cannot produce.
const emitted = new Set([...history, ...live.flatMap(entry => entry.batch)].map(event => event.type))
const undeclared = [...emitted].filter(type => !remoteEmits(type))
check('every event type this backend emits is declared', undeclared.length === 0, undeclared.join(', '))

// ── "gone" distinguishes ran-and-gone from merely withdrawn ────────────────

// A second handle on the same fake Host: these checks advance this handle's
// durable position, which the first handle's reconnect assertions depend on.
const queueSession = await RemoteDshSession.open(SESSION_ID, new RemoteEndpoint(baseUrl, 'valid-token'), hostPort)
await wait(80)
const secondQueue = queueSession.capabilities.pendingQueue

// A row the Host refuses with a not-found style code left its queue; without
// durable proof of a message the retraction is a plain `gone`.
host.queue = [
  { id: 'host-item-3', rpcId: 'client-vanished', placement: 'queued', content: [{ type: 'text', text: 'vanished' }] },
]
await queueSession.cancel('interrupt')
host.failUpdateQueue = 'gone'
const vanished = await secondQueue?.remove('client-vanished')
check('a row the Host no longer holds is gone, not refused',
  vanished?.kind === 'gone' && vanished.claimed !== true, JSON.stringify(vanished))
host.failUpdateQueue = undefined

// The same request id arriving as a durable user message is proof it ran: the
// row must report `claimed`, so its text never returns as a sendable draft.
host.queue = [
  { id: 'host-item-4', rpcId: 'client-claimed', placement: 'queued', content: [{ type: 'text', text: 'claimed' }] },
]
await queueSession.cancel('interrupt')
const claimedSeq = 900
feedFollow({
  type: 'event',
  event: {
    type: 'user/message',
    seq: claimedSeq,
    time: 1_700_000_900_000,
    data: { id: 'u-claimed', source: { kind: 'user', rpcId: 'client-claimed' }, content: [{ type: 'text', text: 'claimed' }] },
  },
})
await wait(80)
const claimedGone = await secondQueue?.remove('client-claimed')
check('a row that became a durable message reports claimed',
  claimedGone?.kind === 'gone' && claimedGone.claimed === true, JSON.stringify(claimedGone))
await queueSession.dispose()

// ── stage 2: create, rename, fork, model catalog, effort ───────────────────

// These run on their own handles: they advance durable positions and touch
// Host state the earlier cases pin.
host.followMode = 'continuous'
const stage2 = await RemoteDshSession.open(SESSION_ID, new RemoteEndpoint(baseUrl, 'valid-token'), hostPort)
const stage2Live = []
stage2.subscribe((batch) => { stage2Live.push(batch) })
await wait(120)

// The catalog is the Host's own: providers, labels and the reasoning ladder.
const catalogList = await stage2.capabilities.models.list()
check('the model list is the Host catalog with provider ids',
  catalogList.length === 3
  && catalogList[0].provider === 'deepseek-official' && catalogList[0].id === 'deepseek-chat'
  && catalogList[2].provider === 'relay'
  && catalogList[1].label === 'DeepSeek Reasoner', JSON.stringify(catalogList))

// The opening projection is the session's own selection.
check('the session model comes from the Host projection',
  stage2.capabilities.models.current().model === 'verify-model', JSON.stringify(stage2.capabilities.models.current()))

// A refused switch keeps the old binding: no local optimism.
host.failSelectModel = true
const refusedSwitch = await stage2.capabilities.models.set({ provider: 'relay', model: 'relay-large' })
check('a refused switch is refused, not applied',
  refusedSwitch.kind === 'refused' && stage2.capabilities.models.current().model === 'verify-model', JSON.stringify(refusedSwitch))

// A switch is a Host RPC; the answer is the resolved selection.
const switched = await stage2.capabilities.models.set({ provider: 'deepseek-official', model: 'deepseek-reasoner' })
check('a switch goes through the Host selector and reads back',
  switched.kind === 'switched' && stage2.capabilities.models.current().model === 'deepseek-reasoner'
  && host.selections.at(-1)?.provider === 'deepseek-official', JSON.stringify({ switched, sent: host.selections.at(-1) }))

// The ladder belongs to the selected model; effort rides the same selector.
check('the effort ladder follows the selected model',
  stage2.capabilities.effort.levels().map(level => level.id).join(',') === 'low,high'
  && stage2.capabilities.effort.forModel({ provider: 'deepseek-official', model: 'deepseek-reasoner' }).defaultEffort === 'high', JSON.stringify(stage2.capabilities.effort.levels()))
await stage2.capabilities.effort.set('low')
check('setting an effort is a Host selection, not a local guess',
  stage2.capabilities.effort.current() === 'low' && host.selections.at(-1)?.reasoningEffort === 'low'
  && host.selections.at(-1)?.model === 'deepseek-reasoner', JSON.stringify(host.selections.at(-1)))

// Rename and fork are the Host's own operations.
await stage2.capabilities.rename.rename('renamed on both ends')
check('a rename reaches the Host', host.renames.at(-1)?.title === 'renamed on both ends'
  && host.renames.at(-1)?.sessionId === SESSION_ID, JSON.stringify(host.renames.at(-1)))
const forked = await stage2.capabilities.fork.fork('7')
check('a fork names the Host session it created',
  forked.sessionId === 'session-forked' && forked.backendId === 'remote' && host.forks.at(-1)?.atSeq === 7,
  JSON.stringify({ forked, sent: host.forks.at(-1) }))
await stage2.capabilities.fork.fork()
check('a fork without an anchor lets the Host pick the cut',
  host.forks.at(-1)?.atSeq === undefined, JSON.stringify(host.forks.at(-1)))

// ── stage 3: the Host's own command registry ───────────────────────────────
//
// A slash line is the Host's decision, not model text: the catalog is the
// Host's `commands/list`, execution is `commands/execute`, and a name the Host
// does not know comes back distinguishable from a settled one.

const hostCommands = await stage2.capabilities.commands.list()
check('the command catalog is the Host registry, with its argument hints',
  hostCommands.length === 3 && hostCommands[0].name === 'plan'
  && hostCommands[0].argumentHint === '[on|off]' && hostCommands[2].name === 'ship',
  JSON.stringify(hostCommands))
check('the catalog request names the Host agent',
  host.calls.some(call => call.method === 'commands/list' && call.args?.agentId === SESSION_ID),
  JSON.stringify(host.calls.filter(call => call.method === 'commands/list').at(-1)))

const shipped = await stage2.capabilities.commands.execute('/ship now')
check('a command runs in the Host process and returns its report',
  shipped.kind === 'settled' && shipped.text === 'shipped on the Host'
  && host.commands.at(-1)?.line === '/ship now' && host.commands.at(-1)?.agentId === SESSION_ID,
  JSON.stringify({ shipped, sent: host.commands.at(-1) }))

const silent = await stage2.capabilities.commands.execute('/plan')
check('a silent success is still a settlement',
  silent.kind === 'settled' && silent.text === '' && silent.error !== true, JSON.stringify(silent))

const unknownCommand = await stage2.capabilities.commands.execute('/nosuchthing')
check('an unknown slash line is unknown, never text for the model',
  unknownCommand.kind === 'unknown', JSON.stringify(unknownCommand))

const failedCommand = await stage2.capabilities.commands.execute('/refuse')
check('a Host refusal is reported with the Host reason',
  failedCommand.kind === 'failed' && failedCommand.reason.includes('forbids that command'), JSON.stringify(failedCommand))


// ── stage 3: child catalog, child transcript, child interrupt ──────────────

feedFollow({
  type: 'event',
  event: {
    seq: 9001,
    type: 'subagent/catalog',
    time: 1_700_000_009_001,
    data: { version: 3, childId: 'child-1', childCreatedAt: 1_700_000_000_000, mode: 'continuable', label: 'research child', name: 'researcher' },
  },
})
await wait(120)
const childRows = stage2Live.flatMap(batch => batch).filter(event => event.type === 'subagent.start')
check('a Host catalog record becomes a child row with its Host id',
  childRows.some(row => row.agentId === 'child-1' && row.description === 'research child' && row.kind === 'continuable'),
  JSON.stringify(childRows.at(-1)))

host.childRecords = [record(90, user('child said this'))]
const childPage = await stage2.capabilities.subagents.history('child-1', { count: 20, skipFromStart: 0 })
check('a child transcript addresses parent and child, not the local cwd',
  host.pageRequests.at(-1)?.request?.address?.kind === 'subagent'
  && host.pageRequests.at(-1)?.request?.address?.parentSessionId === SESSION_ID
  && host.pageRequests.at(-1)?.request?.address?.childSessionId === 'child-1'
  && host.pageRequests.at(-1)?.request?.address?.mode === 'continuable',
  JSON.stringify(host.pageRequests.at(-1)?.request?.address))
check('the child transcript replays as this session reads history',
  childPage.events.some(event => event.type === 'user.message' && event.text === 'child said this')
  && childPage.parentAgentId === SESSION_ID, JSON.stringify(childPage.events.map(event => event.type)))

const interrupted = await stage2.capabilities.subagents.interrupt('child-1')
check('an interrupt is the Host request for that child',
  interrupted === true && host.interrupts.at(-1)?.childSessionId === 'child-1',
  JSON.stringify(host.interrupts.at(-1)))

// ── stage 3: background jobs are Host-owned too ────────────────────────────

feedFollow({ type: 'event', event: { seq: 9002, type: 'job/registered', time: 1_700_000_009_002, data: { jobId: 'job-1', owner: SESSION_ID, kind: 'bash', label: 'npm run dev', startedAt: 1_700_000_009_002 } } })
feedFollow({ type: 'event', event: { seq: 9003, type: 'job/settled', time: 1_700_000_009_003, data: { jobId: 'job-1', owner: SESSION_ID, kind: 'bash', label: 'npm run dev', startedAt: 1_700_000_009_002, finishedAt: 1_700_000_009_003, status: 'killed', cause: 'user', producerReported: true, detail: 'SIGTERM' } } })
await wait(120)
const jobEvents = stage2Live.flatMap(batch => batch).filter(event => event.type === 'task.start' || event.type === 'task.end')
check('a Host job registration and settlement become task rows',
  jobEvents.some(event => event.type === 'task.start' && event.taskId === 'job-1' && event.description === 'npm run dev')
  && jobEvents.some(event => event.type === 'task.end' && event.taskId === 'job-1' && event.status === 'stopped'),
  JSON.stringify(jobEvents))

const killed = await stage2.capabilities.tasks.stop('job-1')
check('stopping a job is the Host kill request',
  killed === true && host.kills.at(-1)?.request?.jobId === 'job-1' && host.kills.at(-1)?.request?.sessionId === SESSION_ID,
  JSON.stringify(host.kills.at(-1)))

// ── stage 4: the session's own files, and images both ways ─────────────────

// The @ completion and the read-only preview read the HOST directory: a path
// outside it is a miss, never this machine's disk.
host.directories = {
  '': [{ name: 'README.md', type: 'file', size: 12 }, { name: 'src', type: 'directory' }],
  src: [{ name: 'main.ts', type: 'file', size: 30 }],
}
host.files = {
  'README.md': { type: 'file', bytes: 12 },
  src: { type: 'directory' },
  'src/main.ts': { type: 'file', bytes: 30 },
}
host.texts = { 'README.md': '# host readme', 'src/main.ts': 'export const host = true' }

const files = stage2.capabilities.files
// The root is the Host's own cwd for this session, whatever the snapshot said.
const hostRoot = stage2.cwd
check('the session advertises its Host directory, not this machine',
  files !== undefined && files.root === hostRoot, String(files?.root))

const rootListing = await files.list('')
check('listing the Host workspace root returns Host entries',
  rootListing.entries.map(entry => entry.name).join(',') === 'README.md,src'
  && host.listings.at(-1)?.workspaceFileScopeId === SESSION_ID
  && host.listings.at(-1)?.path === '',
  JSON.stringify({ listing: rootListing, sent: host.listings.at(-1) }))

const readme = await files.readText('README.md')
check('reading a Host file returns the Host bytes',
  readme.text === '# host readme' && readme.eof === true
  && host.reads.at(-1)?.workspaceFileScopeId === SESSION_ID, JSON.stringify(readme))

check('a Host path that does not exist is a miss, not an exception',
  (await files.stat('nope/missing.ts')) === undefined, 'stat returned a value')

// The channel-level surface maps those Host paths onto display paths under
// the Host root, and refuses anything outside it.
const { sessionFilesFs } = await import('../lib/types/dsh-adapter/channel/session-files.js')
const mentionFs = sessionFilesFs(files)
const resolved = await mentionFs.resolve(`${hostRoot}/README.md`)
check('a mention inside the Host directory resolves to its display path',
  resolved.displayPath === `${hostRoot}/README.md`, resolved.displayPath)
check('an @ mention of a Host file reads through the Host',
  (await mentionFs.readText(resolved)) === '# host readme', 'readText mismatch')
check('@ completion lists the Host directory',
  (await mentionFs.listDir({ displayPath: hostRoot })).map(entry => entry.name).join(',') === 'README.md,src',
  'listing mismatch')
let outside = ''
try {
  await mentionFs.resolve('/local/machine/path/secret.txt')
} catch (error) {
  outside = error instanceof Error ? error.message : String(error)
}
check('a path outside the Host directory is refused, never read locally',
  outside.includes('outside this session'), outside)

// Outgoing: an attached image is the official inline prompt part, and the
// Host's answer reads back through `session/attachment`.
const imageBytes = Uint8Array.from([1, 2, 3, 4])
const staged = { id: 'img-1', width: 2, height: 2, name: 'shot.png', mediaType: 'image/png', bytes: 4, read: async () => imageBytes }
const imagesAtSubmit = host.promptContents.length
const imageSubmit = await stage2.submit({ text: 'look at this', clientMessageId: 'client-image', images: [staged] }, 'queue')
await wait(80)
const sent = host.promptContents.at(-1)
check('an attached image is sent as an inline Host image part',
  imageSubmit.accepted === true && host.promptContents.length === imagesAtSubmit + 1
  && sent?.content?.[0]?.type === 'text' && sent?.content?.[1]?.type === 'image'
  && sent?.content?.[1]?.mediaType === 'image/png'
  && sent?.content?.[1]?.data === Buffer.from(imageBytes).toString('base64')
  && sent?.content?.[1]?.name === 'shot.png',
  JSON.stringify(sent?.content))

// An unreadable attachment refuses the submit before anything leaves.
const before = host.promptContents.length
const unreadable = await stage2.submit({
  text: 'broken',
  clientMessageId: 'client-image-broken',
  images: [{ id: 'img-2', width: 1, height: 1, mediaType: 'image/png', read: async () => { throw new Error('gone from disk') } }],
}, 'queue')
check('an unreadable image refuses before sending, keeping the draft',
  unreadable.accepted === false && unreadable.dropped === true
  && host.promptContents.length === before && String(unreadable.reason).includes('gone from disk'),
  JSON.stringify(unreadable))

// Incoming: the Host's durable image reference reads back through the Host.
feedFollow({ type: 'event', event: {
  seq: 9101,
  type: 'user/message',
  time: 1_700_000_010_101,
  data: {
    id: 'u-image',
    source: { kind: 'user' },
    content: [
      { type: 'text', text: 'here is a picture' },
      { type: 'image', attachment: { id: 'img-host-1', mediaType: 'image/png', width: 3, height: 4, bytes: 11, name: 'host.png' } },
    ],
  },
} })
await wait(120)
const imageRow = stage2Live.flatMap(batch => batch).find(event => event.type === 'user.message' && event.id === 'u-image')
check('a Host message with an image exposes a row that reads the Host bytes',
  imageRow?.images?.length === 1 && imageRow.images[0].id === 'img-host-1'
  && imageRow.images[0].width === 3 && imageRow.images[0].mediaType === 'image/png',
  JSON.stringify(imageRow?.images))
const imageData = imageRow === undefined ? new Uint8Array() : await imageRow.images[0].read()
check('the image row reads back through session/attachment for this session',
  Buffer.from(imageData).toString('base64') === 'aGVsbG8taW1hZ2U='
  && host.attachmentReads.at(-1)?.request?.sessionId === SESSION_ID,
  JSON.stringify(host.attachmentReads.at(-1)?.request))



// The Host terminal inventory is READ-ONLY: this client lists what the Host
// owns and never attaches, starts or types into a terminal.
host.terminals = [
  { id: 'term-1', title: 'bash', shell: { path: '/bin/bash', name: 'bash', args: ['-i'] }, cwd: HOST_WORKSPACE, cols: 80, rows: 24, state: 'running', exitCode: null, origin: 'user' },
  { id: 'term-2', title: 'agent shell', shell: { path: '/bin/zsh', name: 'zsh', args: ['-i'] }, cwd: HOST_WORKSPACE, cols: 120, rows: 40, state: 'exited', exitCode: 0, origin: 'agent' },
]
const inventory = await stage2.capabilities.terminals.list()
check('the Host terminal inventory maps id/state/origin/size',
  inventory.length === 2 && inventory[0].id === 'term-1' && inventory[0].state === 'running'
  && inventory[0].origin === 'user' && inventory[0].cols === 80
  && inventory[1].state === 'exited' && inventory[1].exitCode === 0 && inventory[1].origin === 'agent'
  && host.terminalListCalls.at(-1)?.sessionId === SESSION_ID,
  JSON.stringify({ inventory, sent: host.terminalListCalls.at(-1) }))
const terminalEnvironment = await stage2.capabilities.terminals.environment()
check('the Host terminal environment comes from the Host',
  terminalEnvironment?.cwd === HOST_WORKSPACE && terminalEnvironment?.maxCols === 500
  && host.terminalEnvironmentCalls.at(-1)?.agentId === SESSION_ID,
  JSON.stringify({ terminalEnvironment, sent: host.terminalEnvironmentCalls.at(-1) }))
check('the Host shells are discovered on the Host',
  (await stage2.capabilities.terminals.shells()).map(shell => shell.name).join(',') === 'bash',
  'shells mismatch')
const { channelCapabilities } = await import('../lib/types/channel/capabilities.js')
const terminalSnapshot = channelCapabilities({
  backendId: 'remote',
  backendLabel: 'Remote Web',
  capabilities: { terminals: stage2.capabilities.terminals, files: stage2.capabilities.files },
})
check('/terminals is published only when the Host owns the inventory',
  terminalSnapshot.commands.includes('terminals')
  && channelCapabilities({ backendId: 'x', backendLabel: 'X', capabilities: {} }).commands.includes('terminals') === false,
  JSON.stringify({ published: terminalSnapshot.commands.includes('terminals') }))

check('listing terminals never creates, attaches or types into one',
  host.terminalListCalls.length === 1 && host.terminalCreates === 0 && host.terminalAttaches === 0,
  JSON.stringify({ lists: host.terminalListCalls.length, creates: host.terminalCreates, attaches: host.terminalAttaches }))

// Leaving the TUI is not job control: the Host keeps running what it owns.
const killsAtRelease = host.kills.length
const commandsAtRelease = host.commands.length
await stage2.dispose()
await wait(120)
check('releasing the TUI leaves the Host jobs alone',
  host.kills.length === killsAtRelease && host.commands.length === commandsAtRelease,
  JSON.stringify({ kills: host.kills.length - killsAtRelease, commands: host.commands.length - commandsAtRelease }))

// Creating a session uses a HOST directory, never this machine's cwd.
const avatar = Symbol.for('dsh-tui.remote-connect')
const previousHandoff = globalThis[avatar]
globalThis[avatar] = { baseUrl, token: 'valid-token', cwd: '/host/work' }
const created = await remoteBackend.open({ kind: 'create', cwd: '/local/machine/path' }, hostPort)
check('a new session is created on the Host in the Host directory',
  created.ref.sessionId === 'session-created' && host.creates.at(-1)?.cwd === '/host/work', JSON.stringify(host.creates.at(-1)))
check('the local cwd never reaches the Host',
  !host.calls.some(call => JSON.stringify(call.args ?? {}).includes('/local/machine/path')), 'a local path was sent')
await created.dispose()

// Without a Host directory the creation is refused, not guessed.
globalThis[avatar] = { baseUrl, token: 'valid-token' }
host.workspaces = []
// Even the Host's first-use default is unavailable here.
host.failInitializeDefault = true
const beforeRefuse = host.creates.length
let createRefusal = ''
try {
  await remoteBackend.open({ kind: 'create', cwd: '/local/machine/path' }, hostPort)
} catch (error) {
  createRefusal = error instanceof Error ? error.message : String(error)
}
check('creating without a Host directory is refused, not guessed',
  createRefusal.includes('names no workspace') && host.creates.length === beforeRefuse, createRefusal)

// The Host's own workspace is the fallback when it has no sessions yet.
host.failInitializeDefault = false
host.workspaces = ['/host/workspace']
const firstSession = await remoteBackend.open({ kind: 'create', cwd: '/local/machine/path' }, hostPort)
check('the Host workspace backs a first session',
  host.creates.at(-1)?.cwd === '/host/workspace' && firstSession.ref.sessionId === 'session-created', JSON.stringify(host.creates.at(-1)))
await firstSession.dispose()

// A virgin Host (no sessions, no Workspace) uses its own first-use default.
host.workspaces = []
host.failInitializeDefault = false
const virgin = await remoteBackend.open({ kind: 'create', cwd: '/local/machine/path' }, hostPort)
check('a virgin Host creates its first session in the Host default workspace',
  host.creates.at(-1)?.cwd === '/host/default-workspace' && virgin.ref.sessionId === 'session-created', JSON.stringify(host.creates.at(-1)))
await virgin.dispose()

// When even that is unavailable the creation is refused, not guessed.
host.failInitializeDefault = true
const beforeIneligible = host.creates.length
let ineligible = ''
try {
  await remoteBackend.open({ kind: 'create', cwd: '/local/machine/path' }, hostPort)
} catch (error) {
  ineligible = error instanceof Error ? error.message : String(error)
}
check('an ineligible Host refuses creation instead of inventing a directory',
  ineligible.includes('names no workspace') && host.creates.length === beforeIneligible, ineligible)
globalThis[avatar] = previousHandoff ?? undefined
if (previousHandoff === undefined) delete globalThis[avatar]

// ── launcher shape: the link is never an argument ──────────────────────────

const launcher = spawnSync(process.execPath, ['bin/dsh-tui.js', '--connect', 'http://127.0.0.1:9/?token=leak'], {
  cwd: new URL('..', import.meta.url).pathname,
  encoding: 'utf8',
  env: { ...process.env, DSH_TUI_CONNECT: '' },
})
check('a link passed as a --connect value is refused',
  launcher.status === 2 && /takes no value|不接受值/u.test(launcher.stderr), `${String(launcher.status)}: ${launcher.stderr.trim()}`)

server.close()
wss.close()
console.log(failures === 0 ? '\nverify-remote-attach OK' : `\nverify-remote-attach FAILED (${String(failures)})`)
process.exit(failures === 0 ? 0 : 1)
