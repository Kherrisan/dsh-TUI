/**
 * The `--connect` startup seam: read a Web launch link without echoing it,
 * authenticate once, list the Host's sessions and choose one **before** the
 * screen mounts. The token lives only on the process global; it never enters
 * argv, the environment, a log or a recovery file.
 */

const HANDOFF = Symbol.for('dsh-tui.remote-connect')
const LOOPBACK = new Set(['127.0.0.1', '::1', '[::1]', 'localhost'])

interface HostSession {
  readonly id: string
  readonly title: string
  readonly cwd: string
  readonly updatedAt: number
  readonly running: boolean
}

interface LaunchLink {
  readonly baseUrl: string
  readonly token: string
}

const zh = (): boolean => process.env.DSH_TUI_LANG !== 'en'
const text = (en: string, cn: string): string => zh() ? cn : en

class ConnectCancelled extends Error {
  constructor() { super('connect cancelled') }
}

const trimLink = (raw: string): string => raw.trim().replace(/^[<"']+/u, '').replace(/[>"']+$/u, '').trim()

function parseLink(raw: string): LaunchLink {
  let url: URL
  try {
    url = new URL(trimLink(raw))
  } catch {
    throw new Error(text('that is not a URL; paste the complete launch link', '这不是一个 URL；请粘贴完整的启动链接'))
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(text('the launch link must use http or https', '启动链接必须使用 http 或 https'))
  }
  if (!LOOPBACK.has(url.hostname)) {
    throw new Error(text(`the Host must be loopback, not ${url.hostname}`, `Host 必须是回环地址，不能是 ${url.hostname}`))
  }
  if (url.username !== '' || url.password !== '') {
    throw new Error(text('do not put credentials in the launch link', '不要在启动链接里携带凭据'))
  }
  if (url.pathname !== '/' && url.pathname !== '') {
    throw new Error(text('paste the launch link exactly as the Web Host printed it', '请按 Web Host 输出的原样粘贴启动链接'))
  }
  const token = url.searchParams.get('token') ?? ''
  if (token === '' || token.length > 512 || /\s/u.test(token)) {
    throw new Error(text('the launch link has no usable token', '启动链接里没有可用的 token'))
  }
  return { baseUrl: `${url.protocol}//${url.host}`, token }
}

async function readHiddenLine(prompt: string): Promise<string> {
  process.stderr.write(prompt)
  const stdin = process.stdin
  if (stdin.isTTY !== true) return await nextPipedLine()
  const wasRaw = stdin.isRaw === true
  stdin.setRawMode(true)
  stdin.resume()
  return await new Promise<string>((resolve, reject) => {
    let value = ''
    const cleanup = (): void => {
      stdin.off('data', onData)
      stdin.pause()
      stdin.setRawMode(wasRaw)
      process.stderr.write('\n')
    }
    const onData = (chunk: Buffer | string): void => {
      const text = String(chunk).replace(/\x1b\[[0-9;?]*[A-Za-z~]/gu, '')
      for (const char of text) {
        if (char === '\u0003') {
          cleanup()
          reject(new ConnectCancelled())
          return
        }
        if (char === '\r' || char === '\n') {
          cleanup()
          resolve(value)
          return
        }
        if (char === '\u007f' || char === '\b') {
          value = [...value].slice(0, -1).join('')
          continue
        }
        if (char === '\u0015') {
          value = ''
          continue
        }
        if (char >= ' ') value += char
      }
    }
    stdin.on('data', onData)
  })
}

/**
 * One answer to the session picker. `r` and `q` are advertised as single keys,
 * so a TTY acts on the first keystroke; a pipe keeps the line-based path so
 * `printf '1\n' | dsh-tui --connect` still works.
 */
async function readChoice(prompt: string): Promise<string> {
  const stdin = process.stdin
  if (stdin.isTTY !== true) {
    process.stderr.write(prompt)
    return (await nextPipedLine()).trim().toLowerCase()
  }
  process.stderr.write(prompt)
  const wasRaw = stdin.isRaw === true
  stdin.setRawMode(true)
  stdin.resume()
  return await new Promise<string>((resolve, reject) => {
    const cleanup = (): void => {
      stdin.off('data', onData)
      stdin.pause()
      stdin.setRawMode(wasRaw)
    }
    const onData = (chunk: Buffer | string): void => {
      const typed = String(chunk).replace(/\x1b\[[0-9;?]*[A-Za-z~]/gu, '')
      const char = [...typed].find(candidate => candidate !== '\r' && candidate !== '\n')
      if (char === undefined) return
      cleanup()
      if (char === '\u0003') {
        reject(new ConnectCancelled())
        return
      }
      process.stderr.write(`${char}\n`)
      resolve(char.toLowerCase())
    }
    stdin.on('data', onData)
  })
}

/**
 * Answers for a non-TTY stdin, drained once. `readline.question()` never
 * settles on a pipe that already ended, which surfaces as an unsettled
 * top-level await; reading the whole stream leaves an empty answer for EOF and
 * keeps `printf 'r\n1\n' | dsh-tui --connect` deterministic.
 */
let pipedLines: string[] | undefined
let pipedCursor = 0

async function nextPipedLine(): Promise<string> {
  if (pipedLines === undefined) {
    const chunks: Buffer[] = []
    for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk as Buffer))
    pipedLines = Buffer.concat(chunks).toString('utf8').split(/\r?\n/u)
  }
  const line = pipedLines[pipedCursor] ?? ''
  pipedCursor += 1
  return line
}

const titleOf = (item: Record<string, unknown>): string => {
  const projections = item.projections
  if (projections === null || typeof projections !== 'object' || Array.isArray(projections)) return ''
  const values = (projections as { values?: unknown }).values
  if (values === null || typeof values !== 'object' || Array.isArray(values)) return ''
  const title = (values as { title?: unknown }).title
  return typeof title === 'string' ? title : ''
}

async function fetchSessions(baseUrl: string, token: string): Promise<readonly HostSession[]> {
  const connect = new URL('/connect', baseUrl)
  connect.searchParams.set('token', token)
  connect.searchParams.set('interactions', 'answer')
  const auth = await fetch(connect, { redirect: 'manual' })
  if (auth.status !== 302 && auth.status !== 200) {
    throw new Error(text(
      `the Host refused the launch token (HTTP ${String(auth.status)}); it may be expired`,
      `Host 拒绝了启动 token（HTTP ${String(auth.status)}）；它可能已过期`,
    ))
  }
  const cookie = auth.headers.get('set-cookie')?.split(';')[0] ?? ''
  if (cookie === '') throw new Error(text('the Host returned no authentication cookie', 'Host 没有返回认证 cookie'))
  const response = await fetch(new URL('/api/session/list', baseUrl), {
    method: 'POST',
    headers: { cookie, 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'client-request', rpcId: crypto.randomUUID(), method: 'session/list', payload: { args: { _request: {} } } }),
  })
  if (response.status !== 200) throw new Error(text(`the Host cannot list sessions (HTTP ${String(response.status)})`, `Host 无法列出会话（HTTP ${String(response.status)}）`))
  const reply = await response.json() as { result?: { ok?: unknown; value?: { items?: unknown }; error?: { message?: unknown } } }
  if (reply.result?.ok !== true) throw new Error(typeof reply.result?.error?.message === 'string' ? reply.result.error.message : text('the Host cannot list sessions', 'Host 无法列出会话'))
  const items = Array.isArray(reply.result.value?.items) ? reply.result.value.items : []
  return items.flatMap((value): HostSession[] => {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) return []
    const item = value as Record<string, unknown>
    if (typeof item.sessionId !== 'string') return []
    return [{
      id: item.sessionId,
      title: titleOf(item),
      cwd: typeof item.cwd === 'string' ? item.cwd : '',
      updatedAt: typeof item.updatedAt === 'number' ? item.updatedAt : 0,
      running: item.running === true,
    }]
  }).sort((a, b) => b.updatedAt - a.updatedAt)
}

function describe(session: HostSession): string {
  const title = session.title === '' ? text('(untitled)', '（无标题）') : session.title
  const where = session.cwd === '' ? '' : `  ${session.cwd}`
  const running = session.running ? text('  (running)', '  （运行中）') : ''
  return `${title}${where}${running}`
}

function printSessions(sessions: readonly HostSession[]): void {
  sessions.forEach((session, index) => {
    process.stderr.write(`  ${String(index + 1).padStart(2)}. ${describe(session)}\n`)
  })
}

/**
 * Choose the session to attach to. `undefined` means "create a new one on the
 * Host": only an empty list offers it, because the Host then has no session to
 * attach to and it is the only way in. The created session's directory is the
 * Host's own workspace, resolved by the backend at open time — this machine's
 * cwd is a different path and is never used.
 */
async function chooseSession(sessions: readonly HostSession[], baseUrl: string, token: string): Promise<HostSession | undefined> {
  for (;;) {
    if (sessions.length === 0) {
      const answer = await readChoice(text(
        'The Host has no sessions to attach to. n creates one in a Host workspace, r refreshes, q quits: ',
        'Host 没有可附加的会话。n 在 Host 的工作区新建一个，r 刷新，q 退出：',
      ))
      if (answer === 'q' || answer === 'quit') throw new ConnectCancelled()
      if (answer === 'n' || answer === 'new') return undefined
      if (answer === 'r' || answer === 'refresh') {
        sessions = await fetchSessions(baseUrl, token)
        continue
      }
      process.stderr.write(text('Enter n, r, or q.\n', '请输入 n、r 或 q。\n'))
      continue
    }
    printSessions(sessions)
    const answer = await readChoice(text('Select a session [1-N], r to refresh, q to quit: ', '选择会话 [1-N]，r 刷新，q 退出：'))
    if (answer === 'q' || answer === 'quit') throw new ConnectCancelled()
    if (answer === 'r' || answer === 'refresh') {
      sessions = await fetchSessions(baseUrl, token)
      continue
    }
    const index = Number(answer)
    if (Number.isInteger(index) && index >= 1 && index <= sessions.length) {
      const picked = sessions[index - 1]
      if (picked !== undefined) return picked
    }
    process.stderr.write(text('Enter a listed number, r, or q.\n', '请输入列表中的编号、r 或 q。\n'))
  }
}

/** Fill the process-global handoff and the resume target for one connect launch. */
export async function prepareConnectLaunch(): Promise<void> {
  const requestedId = process.env.DSH_TUI_RESUME_SESSION?.trim()
  for (;;) {
    let link: LaunchLink
    try {
      const pasted = await readHiddenLine(text(
        'Paste the DSH Web launch URL (input is hidden): ',
        '粘贴 DSH Web 启动链接（输入不回显）：',
      ))
      // EOF on a closed stdin: nothing more can be pasted, so stop quietly
      // instead of looping on an empty link.
      if (pasted.trim() === '') throw new ConnectCancelled()
      link = parseLink(pasted)
    } catch (error) {
      // A cancelled link (Ctrl+C, or EOF on a closed stdin) ends the launch;
      // only a malformed link earns another paste.
      if (error instanceof ConnectCancelled) process.exit(130)
      process.stderr.write(`dsh-tui: ${error instanceof Error ? error.message : String(error)}\n`)
      continue
    }
    try {
      const sessions = await fetchSessions(link.baseUrl, link.token)
      let selected: HostSession | undefined
      if (requestedId !== undefined && requestedId !== '') {
        const found = sessions.find(session => session.id === requestedId)
        if (found === undefined) throw new Error(text(
          `the Host has no session "${requestedId}"; run without --resume to choose one`,
          `Host 上没有会话「${requestedId}」；去掉 --resume 可选择现有会话`,
        ))
        selected = found
      } else {
        selected = await chooseSession(sessions, link.baseUrl, link.token)
      }
      process.env.DSH_TUI_BACKEND = 'remote'
      process.env.DSH_TUI_CONNECT = '1'
      // A new session has no id yet: the backend creates it on the Host and
      // the ordinary startup path adopts the created session.
      if (selected === undefined) delete process.env.DSH_TUI_RESUME_SESSION
      else process.env.DSH_TUI_RESUME_SESSION = selected.id
      ;(globalThis as Record<symbol, unknown>)[HANDOFF] = {
        baseUrl: link.baseUrl,
        token: link.token,
        ...(selected === undefined ? {} : { sessionId: selected.id }),
        ...(selected === undefined || selected.cwd === '' ? {} : { cwd: selected.cwd }),
      }
      return
    } catch (error) {
      if (error instanceof ConnectCancelled) process.exit(130)
      const message = error instanceof Error ? error.message : String(error)
      process.stderr.write(`dsh-tui: ${message}\n`)
      // A bad token/URL deserves another paste; a closed Host stays retryable.
      if (requestedId !== undefined && requestedId !== '') process.exit(1)
    }
  }
}
