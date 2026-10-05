import { Context } from '@deepseek-ai/cordis'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { Readable } from 'node:stream'
import { afterEach, describe, expect, it } from 'vitest'
import { buildExtensionWorkerEntry } from './helpers/extension-worker-build.js'
import { WORKER_STREAM_CREDIT_BYTES } from '../src/extension-worker-protocol.js'
import { ExtensionWorkerHost, PreparedJsonResult } from '../src/extension-worker.js'
import { MobileAccessService } from '../src/extensions.js'

const directories: string[] = []
const contexts: Context[] = []
const hosts: ExtensionWorkerHost[] = []

afterEach(async () => {
  for (const host of hosts.splice(0)) await host.terminate('test-end')
  await Promise.all(contexts.splice(0).map(context => context.fiber.dispose()))
  await Promise.all(directories.splice(0).map(directory => rm(directory, { recursive: true, force: true })))
})

async function createExtensionDirectory(root: string, id: string, host: string): Promise<string> {
  const directory = join(root, id)
  await mkdir(directory, { recursive: true })
  await writeFile(join(directory, 'extension.json'), JSON.stringify({ schemaVersion: 1, id, name: id, version: '1.0.0' }))
  await writeFile(join(directory, 'host.mjs'), host)
  return directory
}

function captureLogger(): { lines: unknown[][]; logger: { debug: (...args: unknown[]) => void; info: (...args: unknown[]) => void; warn: (...args: unknown[]) => void; error: (...args: unknown[]) => void } } {
  const lines: unknown[][] = []
  const logger = {
    debug: (...args: unknown[]) => { lines.push(['debug', ...args]) },
    info: (...args: unknown[]) => { lines.push(['info', ...args]) },
    warn: (...args: unknown[]) => { lines.push(['warn', ...args]) },
    error: (...args: unknown[]) => { lines.push(['error', ...args]) },
  }
  return { lines, logger }
}

const callerBase = () => ({ signal: new AbortController().signal, deviceId: 'device' })

describe('extension worker rpc bridge', () => {
  it('activates with metadata-only registration and a logger-only context adapter', async () => {
    const workerModule = await buildExtensionWorkerEntry()
    const directory = await createExtensionDirectory(await mkdtemp(join(tmpdir(), 'dsh-mobile-worker-')), 'demo', `
export default (api) => {
  api.action('meta', { run: () => ({ contextKeys: Object.keys(api.context).sort(), schemaType: typeof api.schema, signalBrand: typeof api.signal }) })
  api.action('echo', { timeoutMs: 1234, run: (_context, input) => input })
  api.route({ method: 'GET', path: 'status', handle: () => ({ body: 'ok' }) })
  api.route({ method: 'GET', path: 'files', kind: 'prefix', timeoutMs: 5000, handle: () => ({ body: 'ok' }) })
  api.effect(() => () => {})
  api.context.logger.info('from-worker', { n: 1 })
}
`)
    const { lines, logger } = captureLogger()
    const host = new ExtensionWorkerHost({
      workerModule,
      hostFile: join(directory, 'host.mjs'),
      manifest: { schemaVersion: 1, id: 'demo', name: 'demo', version: '1.0.0' },
      generation: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      logger,
    })
    hosts.push(host)

    const activated = await host.activate()
    // Registration crosses as plain metadata only — no functions, no schemas.
    expect(activated.actions).toEqual([{ name: 'meta' }, { name: 'echo', timeoutMs: 1234 }])
    expect(activated.routes).toEqual([
      { method: 'GET', path: '/status', kind: 'exact' },
      { method: 'GET', path: '/files', kind: 'prefix', timeoutMs: 5000 },
    ])
    // Runtime identity is distinct from the content-generation hash.
    expect(activated.runtimeId).toMatch(/^[0-9a-f-]{16,}$/u)
    expect(activated.runtimeId).not.toBe('0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')

    const meta = await host.invoke('meta', { value: 1 }, callerBase())
    // The action result crosses as worker-serialized bytes, never as a value.
    expect(meta).toBeInstanceOf(PreparedJsonResult)
    expect((meta as PreparedJsonResult).bytes.toString('utf8'))
      .toBe('{"contextKeys":["logger"],"schemaType":"function","signalBrand":"object"}')

    // The worker context adapter is logger-only and forwards to the parent logger.
    expect(lines).toContainEqual(['info', 'from-worker', { n: 1 }])
  })
})

describe('extension worker streams', () => {
  it('bounds buffering for a slow consumer and propagates cancellation to the worker', async () => {
    const workerModule = await buildExtensionWorkerEntry()
    const root = await mkdtemp(join(tmpdir(), 'dsh-mobile-worker-stream-'))
    directories.push(root)
    const directory = await createExtensionDirectory(root, 'wstream', `
import { Readable } from 'node:stream'
export default (api) => {
  api.route({
    method: 'GET',
    path: 'download',
    handle: (request) => {
      function* chunks() { while (true) yield Buffer.alloc(64 * 1024, 1) }
      const source = Readable.from(chunks())
      request.signal.addEventListener('abort', () => { source.destroy() })
      return { contentType: 'application/octet-stream', body: source }
    },
  })
}
`)
    void directory
    const context = new Context(); contexts.push(context)
    const service = new MobileAccessService(context)
    await service.startLocal(root, context, { hostExecution: { mode: 'worker', workerModule } })

    const active = service.extension('wstream') as { readonly worker: ExtensionWorkerHost } | undefined
    if (active === undefined) throw new Error('wstream extension did not load')
    expect(active.worker).toBeInstanceOf(ExtensionWorkerHost)

    const result = await service.route('wstream', 'GET', '/download', {
      method: 'GET', pathname: '/download', query: new URLSearchParams(), headers: {},
      body: Buffer.alloc(0), signal: new AbortController().signal, deviceId: 'device',
    })
    const stream = result.body as Readable
    expect(typeof stream.pipe).toBe('function')

    // Slow consumer: one 64KB chunk every 20ms, while the worker produces
    // eagerly. Bounded credit keeps parent-side buffering inside the window.
    const received: number[] = []
    await new Promise<void>(resolveDone => {
      const readOne = (): void => {
        const chunk = stream.read() as Buffer | null
        if (chunk === null) return
        received.push(chunk.byteLength)
        if (received.length >= 4) { resolveDone(); return }
        setTimeout(readOne, 20)
      }
      stream.on('readable', readOne)
      setTimeout(readOne, 25)
    })
    expect(received.length).toBeGreaterThanOrEqual(4)
    expect(received.every(size => size === 64 * 1024)).toBe(true)
    expect(active.worker.streamStats().maxBufferedBytes).toBeLessThanOrEqual(WORKER_STREAM_CREDIT_BYTES)
    expect(active.worker.streamStats().activeStreams).toBe(1)

    // Cancellation must reach the worker and drain the stream.
    const closed = new Promise<void>(resolve => { stream.once('close', resolve) })
    stream.destroy()
    await closed
    await new Promise(resolve => { setTimeout(resolve, 150) })
    expect(active.worker.streamStats().activeStreams).toBe(0)
    expect(active.worker.streamStats().cancelledStreams).toBe(1)
  })
})

describe('extension worker termination contract', () => {
  it('keeps the deadline result, settles siblings unavailable, and never replays', async () => {
    const workerModule = await buildExtensionWorkerEntry()
    const root = await mkdtemp(join(tmpdir(), 'dsh-mobile-worker-sibling-'))
    directories.push(root)
    const directory = await createExtensionDirectory(root, 'stalled', `
import { appendFile } from 'node:fs/promises'
const log = new URL('./exec.log', import.meta.url).pathname
export default (api) => {
  api.action('stall', { timeoutMs: 50, run: async () => { await appendFile(log, 'stall\\n'); await new Promise(() => {}) } })
  api.action('slowpoke', { run: async () => { await appendFile(log, 'slowpoke\\n'); await new Promise(resolve => { setTimeout(resolve, 400) }); return { ok: true } } })
}
`)
    void directory
    const context = new Context(); contexts.push(context)
    const service = new MobileAccessService(context)
    await service.startLocal(root, context, { hostExecution: { mode: 'worker', workerModule } })

    const caller = (): { signal: AbortSignal; deviceId: string } => ({ signal: new AbortController().signal, deviceId: 'device' })
    const trigger = service.invoke('stalled', 'stall', {}, caller())
    const sibling = service.invoke('stalled', 'slowpoke', {}, caller())

    // The invocation that triggered the deadline keeps its timeout result.
    await expect(trigger).rejects.toMatchObject({ code: 'extension_action_timeout', status: 500 })
    // Other pending invocations in that worker settle unavailable.
    await expect(sibling).rejects.toMatchObject({ code: 'extension_host_unavailable', status: 503 })

    // No interrupted action is replayed: execution log stays exactly one entry
    // per action even after slowpoke's natural completion time has passed.
    await new Promise(resolve => { setTimeout(resolve, 450) })
    const lines = (await readFile(join(root, 'stalled', 'exec.log'), 'utf8')).trim().split('\n').sort()
    expect(lines).toEqual(['slowpoke', 'stall'])

    // The dead runtime is not silently resurrected for new work.
    await expect(service.invoke('stalled', 'slowpoke', {}, caller())).rejects.toMatchObject({ code: 'extension_host_unavailable', status: 503 })
  })
})
