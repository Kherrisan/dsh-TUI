/** Minimal DSH Web wire client: launch-token auth, same-origin RPC, and the
 * forwarded-event/session-stream mux. It uses the public HTTP/WS protocol only
 * and no DSH package, which keeps this backend import-island clean. */
import WebSocket from 'ws'

export class RemoteWireError extends Error {
  constructor(message: string, readonly code?: string, readonly details?: unknown) {
    super(message)
    this.name = 'RemoteWireError'
  }
}

export class RemoteConnectionLost extends Error {
  constructor(message = 'the DSH Web connection closed') {
    super(message)
    this.name = 'RemoteConnectionLost'
  }
}

interface RpcReply {
  readonly type?: unknown
  readonly result?: {
    readonly ok?: unknown
    readonly value?: unknown
    readonly error?: { readonly code?: unknown; readonly message?: unknown; readonly details?: unknown }
  }
}

interface MuxFrame {
  readonly type?: unknown
  readonly streamId?: unknown
  readonly value?: unknown
  readonly error?: unknown
}

class StreamQueue implements AsyncIterable<unknown> {
  private readonly values: unknown[] = []
  private readonly waiters: Array<{ resolve: (value: IteratorResult<unknown>) => void; reject: (error: unknown) => void }> = []
  private ended: Error | undefined
  private done = false

  push(value: unknown): void {
    if (this.done) return
    const waiter = this.waiters.shift()
    if (waiter !== undefined) waiter.resolve({ done: false, value })
    else this.values.push(value)
  }

  close(error?: Error): void {
    if (this.done) return
    this.done = true
    this.ended = error
    for (const waiter of this.waiters.splice(0)) {
      if (error === undefined) waiter.resolve({ done: true, value: undefined })
      else waiter.reject(error)
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<unknown> {
    return {
      next: () => {
        const value = this.values.shift()
        if (value !== undefined) return Promise.resolve({ done: false, value })
        if (this.done) return this.ended === undefined ? Promise.resolve({ done: true, value: undefined }) : Promise.reject(this.ended)
        return new Promise((resolve, reject) => { this.waiters.push({ resolve, reject }) })
      },
      return: () => {
        this.close()
        return Promise.resolve({ done: true, value: undefined })
      },
    }
  }
}

/** One authenticated HTTP connection to a DSH Web Host. */
export class RemoteEndpoint {
  private cookie: string | undefined

  constructor(readonly baseUrl: string, private readonly token: string) {}

  /** Claim a fresh owner Peer. Each physical stream needs its own cookie:
   *  the Host deletes the Peer when that stream's socket closes. */
  async authenticate(): Promise<void> {
    const url = new URL('/connect', this.baseUrl)
    url.searchParams.set('token', this.token)
    url.searchParams.set('interactions', 'answer')
    const response = await fetch(url, { redirect: 'manual' })
    if (response.status !== 302 && response.status !== 200) {
      throw new RemoteWireError(`the Web Host refused the launch token (HTTP ${String(response.status)})`)
    }
    const raw = response.headers.get('set-cookie')
    if (raw === null || raw === '') throw new RemoteWireError('the Web Host returned no authentication cookie')
    this.cookie = raw.split(';')[0] ?? ''
    if (this.cookie === '') throw new RemoteWireError('the Web Host returned an empty authentication cookie')
  }

  /** Same-origin unary RPC. `retryAuth` is for callers with no live socket. */
  async rpc<T>(method: string, args: unknown, options: { readonly retryAuth?: boolean } = {}): Promise<T> {
    for (let attempt = 0; ; attempt += 1) {
      if (this.cookie === undefined) await this.authenticate()
      const response = await fetch(new URL(`/api/${method}`, this.baseUrl), {
        method: 'POST',
        headers: { cookie: this.cookie ?? '', 'content-type': 'application/json' },
        body: JSON.stringify({ type: 'client-request', rpcId: crypto.randomUUID(), method, payload: { args } }),
      })
      if (response.status === 401) {
        if (options.retryAuth === true && attempt === 0) {
          this.cookie = undefined
          continue
        }
        throw new RemoteConnectionLost('the Web Host no longer accepts this connection')
      }
      if (response.status !== 200) throw new RemoteWireError(`the Web Host answered ${method} with HTTP ${String(response.status)}`)
      const reply = await response.json() as RpcReply
      const result = reply.result
      if (result === undefined || result.ok !== true) {
        const code = typeof result?.error?.code === 'string' ? result.error.code : 'remote/rpc-failed'
        const message = typeof result?.error?.message === 'string' ? result.error.message : `${method} failed`
        throw new RemoteWireError(message, code, result?.error?.details)
      }
      return result.value as T
    }
  }

  /** Open the shared mux with a cookie bound to this socket. */
  async openMux(): Promise<WebMux> {
    await this.authenticate()
    const wsUrl = new URL('/api/remote.mux', this.baseUrl)
    wsUrl.protocol = this.baseUrl.startsWith('https:') ? 'wss:' : 'ws:'
    const ws = new WebSocket(wsUrl, {
      headers: { cookie: this.cookie ?? '', origin: this.baseUrl },
    })
    try {
      await new Promise<void>((resolve, reject) => {
        const onOpen = (): void => { ws.off('error', onError); resolve() }
        const onError = (error: Error): void => { ws.off('open', onOpen); reject(new RemoteConnectionLost(`the Web Host refused the event socket: ${error.message}`)) }
        ws.once('open', onOpen)
        ws.once('error', onError)
      })
    } catch (error) {
      ws.terminate()
      throw error
    }
    const mux = new WebMux(ws)
    return mux
  }
}

/** One WebSocket carrying every logical stream of one Peer generation. */
export class WebMux {
  private readonly queues = new Map<string, StreamQueue>()
  private client: string | undefined
  private closed: Error | undefined

  constructor(private readonly ws: WebSocket) {
    ws.on('message', data => { this.receive(data) })
    ws.on('close', () => { this.fail(new RemoteConnectionLost()) })
    ws.on('error', error => { this.fail(new RemoteConnectionLost(error.message)) })
  }

  get clientId(): string | undefined { return this.client }
  get lost(): Error | undefined { return this.closed }

  openStream(streamId: string, endpoint: string, payload: unknown): AsyncIterable<unknown> {
    if (this.closed !== undefined) throw this.closed
    const queue = new StreamQueue()
    this.queues.set(streamId, queue)
    this.ws.send(JSON.stringify({ type: 'open', streamId, endpoint, payload }))
    return queue
  }

  closeStream(streamId: string): void {
    this.queues.delete(streamId)
    if (this.closed === undefined) this.ws.send(JSON.stringify({ type: 'close', streamId }))
  }

  async answer(endpoint: RemoteEndpoint, eventId: string, outcome: unknown): Promise<void> {
    const clientId = this.client
    if (clientId === undefined) throw new RemoteConnectionLost('the forwarded-event stream is not ready')
    await endpoint.rpc('$events/result', { clientId, eventId, outcome }, { retryAuth: false })
  }

  close(): void {
    this.fail(undefined)
    try { this.ws.close() } catch { /* already closing */ }
  }

  private receive(data: unknown): void {
    let frame: MuxFrame
    try {
      frame = JSON.parse(String(data)) as MuxFrame
    } catch {
      return
    }
    if (frame.type === 'item' && typeof frame.streamId === 'string') {
      this.observe(frame.value)
      this.queues.get(frame.streamId)?.push(frame.value)
      return
    }
    if (frame.type === 'error' && typeof frame.streamId === 'string') {
      const error = frame.error
      const message = error !== null && typeof error === 'object'
        ? String((error as { message?: unknown }).message ?? 'stream failed')
        : 'stream failed'
      this.queues.get(frame.streamId)?.close(new RemoteWireError(message))
    }
  }

  private observe(value: unknown): void {
    if (value === null || typeof value !== 'object') return
    const frame = value as { type?: unknown; clientId?: unknown }
    if (frame.type === 'ready' && typeof frame.clientId === 'string') this.client = frame.clientId
  }

  private fail(error: Error | undefined): void {
    if (this.closed !== undefined) return
    this.closed = error ?? new RemoteConnectionLost('the DSH Web connection was closed')
    for (const queue of this.queues.values()) queue.close(this.closed)
    this.queues.clear()
  }
}
