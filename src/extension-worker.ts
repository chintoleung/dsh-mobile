/**
 * Spike (Phase 2 de-risking): parent-side supervisor for one extension worker.
 *
 * One ExtensionWorkerHost owns one worker per live extension generation. It
 * activates the worker (receiving metadata-only registration), proxies action
 * and route invocations over request IDs, forwards bounded logger traffic, and
 * can cancel and terminate a worker even when the worker is synchronously
 * blocked. Replies from obsolete runtimes are ignored.
 */
import { randomUUID } from 'node:crypto'
import { Worker } from 'node:worker_threads'
import { Readable } from 'node:stream'
import { MobileExtensionError, type LocalExtensionManifest, type MobileRouteRequest, type MobileRouteResponse } from './extensions.js'
import {
  WORKER_DEADLINE_GRACE_MS,
  WORKER_OPERATION_TIMEOUT_MS,
  WORKER_STREAM_CANCEL_GRACE_MS,
  type ParentMessage,
  type WorkerActionMetadata,
  type WorkerRouteMetadata,
  type WorkerMessage,
} from './extension-worker-protocol.js'

/** An action result that crossed the boundary as worker-serialized JSON bytes. */
export class PreparedJsonResult {
  constructor(readonly bytes: Buffer) {}
}

/** Type guard for prepared worker results at the gateway boundary. */
export function isPreparedJson(value: unknown): value is PreparedJsonResult {
  return value instanceof PreparedJsonResult
}

/** Resolve the packaged worker entry next to this module (lib/ adjacency). */
export function defaultExtensionWorkerModule(): string {
  return new URL('./extension-worker-runtime.mjs', import.meta.url).href
}

/** Parent logger shape forwarded from the worker's logger-only context adapter. */
export type WorkerParentLogger = Record<'debug' | 'info' | 'warn' | 'error', (...args: unknown[]) => void>

export interface ExtensionWorkerHostOptions {
  /** Path or URL of the standalone worker entry (lib/extension-worker-runtime.mjs or a test build). */
  readonly workerModule: string
  readonly hostFile: string
  readonly manifest: LocalExtensionManifest
  readonly generation: string
  readonly logger: WorkerParentLogger
  readonly activationTimeoutMs?: number
}

export interface ExtensionWorkerActivation {
  readonly runtimeId: string
  readonly actions: readonly WorkerActionMetadata[]
  readonly routes: readonly WorkerRouteMetadata[]
}

interface PendingRpc {
  readonly resolve: (value: PreparedJsonResult) => void
  readonly reject: (reason: Error) => void
}

const ACTIVATION_TIMEOUT_MS = 5_000
const DISPOSE_GRACE_MS = 2_000

/** Marker used for the activation RPC and the dispose handshake. */
const LIFECYCLE_ID = 'lifecycle'

/** Live worker threads across all supervisors — leak evidence for tests. */
export const activeWorkerCount = { current: 0 }

export class ExtensionWorkerHost {
  private readonly options: ExtensionWorkerHostOptions
  private readonly pending = new Map<string, PendingRpc>()
  private readonly routePending = new Map<string, { readonly resolve: (value: MobileRouteResponse) => void; readonly reject: (reason: Error) => void }>()
  private readonly streams = new Map<string, LiveStream>()
  private maxBufferedBytes = 0
  private cancelledStreams = 0
  private readonly worker: Worker
  private runtimeId: string | undefined
  private dead = false
  private readonly exited: Promise<void>
  private disposed = false

  constructor(options: ExtensionWorkerHostOptions) {
    this.options = options
    this.worker = new Worker(options.workerModule, { type: 'module' } as ConstructorParameters<typeof Worker>[1])
    this.worker.unref()
    activeWorkerCount.current += 1
    this.exited = new Promise<void>(resolve => {
      this.worker.once('exit', () => {
        this.dead = true
        activeWorkerCount.current -= 1
        this.rejectPending(new MobileExtensionError('extension_host_unavailable', `extension ${options.manifest.id} worker exited`, 503))
        for (const stream of this.streams.values()) stream.bridge.destroy()
        this.streams.clear()
        resolve()
      })
    })
    this.worker.on('message', (raw: unknown) => {
      // A malformed or hostile worker message must never crash the parent.
      try { this.handleMessage(raw) } catch { /* dropped */ }
    })
    this.worker.on('error', () => {
      // Uncaught worker exception: force termination so pending callers settle.
      void this.terminate('worker-error')
    })
  }

  /** Activate the generation inside the worker and receive metadata-only registration. */
  async activate(): Promise<ExtensionWorkerActivation> {
    await this.rpc(LIFECYCLE_ID, {
      kind: 'activate',
      id: LIFECYCLE_ID,
      hostFile: this.options.hostFile,
      generation: this.options.generation,
    }, this.options.activationTimeoutMs ?? ACTIVATION_TIMEOUT_MS, new AbortController().signal)
    if (this.activation === undefined) throw new MobileExtensionError('host_activation_failed', `extension ${this.options.manifest.id} worker activation failed`, 500)
    return this.activation
  }

  private activation: ExtensionWorkerActivation | undefined

  /** Metadata-only registration received at activation; empty before activation. */
  actionMetadata(): readonly WorkerActionMetadata[] {
    return this.activation?.actions ?? []
  }

  /** Route metadata received at activation; empty before activation. */
  routeMetadata(): readonly WorkerRouteMetadata[] {
    return this.activation?.routes ?? []
  }

  /** Invoke one action; the result crosses as worker-serialized JSON bytes. */
  async invoke(action: string, input: unknown, caller: { readonly signal: AbortSignal; readonly deviceId: string }): Promise<PreparedJsonResult> {
    if (this.dead) throw new MobileExtensionError('extension_host_unavailable', `extension ${this.options.manifest.id} worker is not running`, 503)
    const id = randomUUID()
    const timeoutMs = this.activation?.actions.find(entry => entry.name === action)?.timeoutMs ?? WORKER_OPERATION_TIMEOUT_MS
    const onAbort = (): void => { this.post({ kind: 'cancel', id }) }
    if (caller.signal.aborted) throw caller.signal.reason ?? new MobileExtensionError('extension_action_cancelled', 'caller detached', 409)
    caller.signal.addEventListener('abort', onAbort, { once: true })
    // The RPC outlives the caller: settlement must observe the underlying work,
    // and an unresponsive worker is terminated after a bounded cleanup grace.
    const reply = this.rpc(id, { kind: 'invoke', id, action, deviceId: caller.deviceId, input }, 0, caller.signal)
    let graceTimer: NodeJS.Timeout | undefined
    const armGrace = (): void => {
      if (graceTimer !== undefined) return
      graceTimer = setTimeout(() => { void this.terminate('deadline-unresponsive') }, WORKER_DEADLINE_GRACE_MS)
      graceTimer.unref?.()
    }
    const settleGrace = (): void => { if (graceTimer !== undefined) { clearTimeout(graceTimer); graceTimer = undefined } }
    // A detached-but-late worker reply disarms the grace timer on settlement.
    void reply.then(settleGrace, settleGrace)
    let deadlineTimer: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        reply,
        new Promise<never>((_, reject) => {
          deadlineTimer = setTimeout(() => {
            armGrace()
            reject(new MobileExtensionError('extension_action_timeout', `extension ${this.options.manifest.id} action ${action} timed out`, 500))
          }, timeoutMs)
          deadlineTimer.unref?.()
        }),
      ])
    } finally {
      if (deadlineTimer !== undefined) clearTimeout(deadlineTimer)
      caller.signal.removeEventListener('abort', onAbort)
      if (graceTimer !== undefined) {
        // The race may have settled by deadline; keep the grace armed only
        // while the underlying RPC is genuinely unresolved.
        if (!this.pending.has(id)) settleGrace()
      }
    }
  }

  /** Invoke one route; streams arrive as credit-controlled chunks over a bridge Readable. */
  async handleRoute(routeIndex: number, request: MobileRouteRequest): Promise<MobileRouteResponse> {
    if (this.dead) throw new MobileExtensionError('extension_host_unavailable', `extension ${this.options.manifest.id} worker is not running`, 503)
    const id = randomUUID()
    // Handler deadline (until the response or stream-start crosses); the stream
    // itself has no handler deadline — cancellation initiates bounded cleanup.
    const timeoutMs = this.activation?.routes[routeIndex]?.timeoutMs ?? WORKER_OPERATION_TIMEOUT_MS
    const onAbort = (): void => {
      this.post({ kind: 'cancel', id })
      this.cancelStream(id, 'caller-abort')
    }
    if (request.signal.aborted) throw request.signal.reason ?? new MobileExtensionError('extension_route_cancelled', 'caller detached', 409)
    request.signal.addEventListener('abort', onAbort, { once: true })
    const reply = new Promise<MobileRouteResponse>((resolve, reject) => {
      this.routePending.set(id, { resolve, reject })
      this.post({
        kind: 'route', id, routeIndex,
        method: request.method, path: request.pathname,
        query: [...request.query.entries()].map(([key, value]) => [key, value] as const),
        headers: request.headers, body: new Uint8Array(request.body), deviceId: request.deviceId,
      })
    })
    let graceTimer: NodeJS.Timeout | undefined
    const armGrace = (): void => {
      if (graceTimer !== undefined) return
      graceTimer = setTimeout(() => { void this.terminate('route-deadline-unresponsive') }, WORKER_DEADLINE_GRACE_MS)
      graceTimer.unref?.()
    }
    const settleGrace = (): void => { if (graceTimer !== undefined) { clearTimeout(graceTimer); graceTimer = undefined } }
    void reply.then(settleGrace, settleGrace)
    let deadlineTimer: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        reply,
        new Promise<never>((_, reject) => {
          deadlineTimer = setTimeout(() => {
            if (this.routePending.has(id)) armGrace()
            reject(new MobileExtensionError('extension_route_timeout', `extension ${this.options.manifest.id} route ${request.method} ${request.pathname} timed out`, 500))
          }, timeoutMs)
          deadlineTimer.unref?.()
        }),
      ])
    } finally {
      if (deadlineTimer !== undefined) clearTimeout(deadlineTimer)
      request.signal.removeEventListener('abort', onAbort)
      if (graceTimer !== undefined && !this.routePending.has(id)) settleGrace()
    }
  }

  /** Observable stream-flow stats for tests and evidence. */
  streamStats(): { readonly activeStreams: number; readonly maxBufferedBytes: number; readonly cancelledStreams: number } {
    return { activeStreams: this.streams.size, maxBufferedBytes: this.maxBufferedBytes, cancelledStreams: this.cancelledStreams }
  }

  /** Stop the generation inside the worker, bounded, then hard-terminate. */
  async dispose(): Promise<void> {
    if (this.disposed || this.dead) { await this.terminate('dispose-skipped'); return }
    this.disposed = true
    try {
      await this.rpc(LIFECYCLE_ID, { kind: 'dispose', id: LIFECYCLE_ID }, DISPOSE_GRACE_MS, new AbortController().signal)
    } catch { /* grace expiry falls through to termination */ }
    await this.terminate('disposed')
  }

  /** Resolve when the worker thread has exited (used by supervised tests). */
  whenExited(): Promise<void> {
    return this.exited
  }

  /** Hard-terminate the worker now; every pending RPC settles unavailable. */
  async terminate(reason: string): Promise<void> {
    if (!this.dead) await this.worker.terminate().catch(() => undefined)
    void reason
    await this.exited
  }

  private async rpc(id: string, message: ParentMessage, timeoutMs: number, signal: AbortSignal): Promise<PreparedJsonResult> {
    if (this.dead) throw new MobileExtensionError('extension_host_unavailable', `extension ${this.options.manifest.id} worker is not running`, 503)
    return await new Promise<PreparedJsonResult>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          this.pending.delete(id)
          reject(new MobileExtensionError('host_load_timeout', `extension ${this.options.manifest.id} worker did not answer`, 500))
        }, timeoutMs)
        timer.unref?.()
      }
      const settle = (): void => { if (timer !== undefined) clearTimeout(timer) }
      // The RPC stays pending past caller abort: settlement observes the
      // underlying work, never the caller's lifetime.
      void signal.aborted
      this.pending.set(id, { resolve: value => { settle(); resolve(value) }, reject: reason => { settle(); reject(reason) } })
      this.post(message)
    })
  }

  private post(message: ParentMessage): void {
    if (!this.dead) this.worker.postMessage(message)
  }

  private handleMessage(raw: unknown): void {
    // Envelope validation: worker traffic is semi-trusted at best.
    if (raw === null || typeof raw !== 'object') return
    const message = raw as { readonly kind?: unknown }
    if (typeof message.kind !== 'string') return
    if (message.kind === 'log') {
      const log = raw as { readonly level?: unknown; readonly args?: unknown }
      if (log.level !== 'debug' && log.level !== 'info' && log.level !== 'warn' && log.level !== 'error') return
      if (!Array.isArray(log.args)) return
      this.options.logger[log.level]?.(...(log.args as readonly unknown[]))
      return
    }
    const tagged = raw as { readonly runtimeId?: unknown; readonly id?: unknown }
    if (typeof tagged.runtimeId !== 'string') return
    if (message.kind === 'activated') {
      const activated = raw as WorkerMessage & { readonly kind: 'activated' }
      // Replies from obsolete runtimes are ignored.
      if (this.runtimeId !== undefined && activated.runtimeId !== this.runtimeId) return
      this.runtimeId = activated.runtimeId
      this.activation = { runtimeId: activated.runtimeId, actions: activated.actions, routes: activated.routes }
      this.pending.get(LIFECYCLE_ID)?.resolve(new PreparedJsonResult(Buffer.alloc(0)))
      this.pending.delete(LIFECYCLE_ID)
      return
    }
    if (typeof tagged.id !== 'string') return
    if (this.runtimeId !== undefined && tagged.runtimeId !== this.runtimeId) return
    if (message.kind === 'result') {
      const result = raw as { readonly kind: 'result'; readonly bytes?: unknown }
      if (!(result.bytes instanceof Uint8Array)) return
      const pending = this.pending.get(tagged.id)
      this.pending.delete(tagged.id)
      pending?.resolve(new PreparedJsonResult(Buffer.from(result.bytes)))
      return
    }
    if (message.kind === 'error') {
      const failure = raw as { readonly kind: 'error'; readonly code?: unknown; readonly message?: unknown; readonly status?: unknown }
      if (typeof failure.code !== 'string' || typeof failure.message !== 'string') return
      const status = Number.isInteger(failure.status) && (failure.status as number) >= 400 && (failure.status as number) <= 599 ? failure.status as number : 500
      const error = new MobileExtensionError(failure.code, failure.message, status)
      const pending = this.pending.get(tagged.id)
      this.pending.delete(tagged.id)
      pending?.reject(error)
      const route = this.routePending.get(tagged.id)
      this.routePending.delete(tagged.id)
      route?.reject(error)
      return
    }
    if (message.kind === 'route-response') {
      const response = raw as { readonly kind: 'route-response'; readonly status?: unknown; readonly contentType?: unknown; readonly headers?: unknown; readonly bytes?: unknown }
      const route = this.routePending.get(tagged.id)
      this.routePending.delete(tagged.id)
      if (route === undefined) return
      if (response.bytes !== undefined && !(response.bytes instanceof Uint8Array)) return
      route.resolve({
        ...(typeof response.status === 'number' ? { status: response.status } : {}),
        ...(typeof response.contentType === 'string' ? { contentType: response.contentType } : {}),
        ...(typeof response.headers === 'object' && response.headers !== null ? { headers: response.headers as Record<string, string> } : {}),
        body: response.bytes === undefined ? Buffer.alloc(0) : Buffer.from(response.bytes),
      })
      return
    }
    if (message.kind === 'stream-start') {
      const start = raw as { readonly kind: 'stream-start'; readonly status?: unknown; readonly contentType?: unknown; readonly headers?: unknown }
      const route = this.routePending.get(tagged.id)
      this.routePending.delete(tagged.id)
      if (route === undefined) return
      const live = this.openStream(tagged.id)
      route.resolve({
        ...(typeof start.status === 'number' ? { status: start.status } : {}),
        ...(typeof start.contentType === 'string' ? { contentType: start.contentType } : {}),
        ...(typeof start.headers === 'object' && start.headers !== null ? { headers: start.headers as Record<string, string> } : {}),
        body: live.bridge,
      })
      return
    }
    if (message.kind === 'stream-chunk') {
      const chunk = raw as { readonly kind: 'stream-chunk'; readonly bytes?: unknown }
      if (!(chunk.bytes instanceof Uint8Array)) return
      const live = this.streams.get(tagged.id)
      if (live === undefined) return
      live.unacked += chunk.bytes.byteLength
      this.maxBufferedBytes = Math.max(this.maxBufferedBytes, this.totalUnacked())
      live.bridge.pushChunk(Buffer.from(chunk.bytes))
      return
    }
    if (message.kind === 'stream-end') {
      this.closeStream(tagged.id)
      return
    }
    if (message.kind === 'stream-error') {
      const failure = raw as { readonly kind: 'stream-error'; readonly message?: unknown }
      const live = this.streams.get(tagged.id)
      this.closeStream(tagged.id)
      live?.bridge.destroy(new Error(typeof failure.message === 'string' ? failure.message : 'worker stream failed'))
    }
  }

  /** Aggregate unacknowledged bytes across all live streams (per-worker bound). */
  private totalUnacked(): number {
    let total = 0
    for (const live of this.streams.values()) total += live.unacked
    return total
  }

  private openStream(id: string): LiveStream {
    const live: LiveStream = {
      bridge: new WorkerStreamBridge(bytes => {
        live.unacked = Math.max(0, live.unacked - bytes)
        // Consumed bytes flow back as credit: the worker's window refills.
        this.post({ kind: 'stream-ack', id, bytes })
      }),
      unacked: 0,
      cancelTimer: undefined,
      ended: false,
    }
    live.bridge.once('close', () => {
      if (!live.ended) this.cancelStream(id, 'consumer-gone')
    })
    this.streams.set(id, live)
    return live
  }

  private closeStream(id: string): void {
    const live = this.streams.get(id)
    if (live === undefined) return
    live.ended = true
    if (live.cancelTimer !== undefined) clearTimeout(live.cancelTimer)
    this.streams.delete(id)
    live.bridge.pushEnd()
  }

  /** The consumer is gone: stop the stream, and terminate an unresponsive worker. */
  private cancelStream(id: string, reason: string): void {
    void reason
    const live = this.streams.get(id)
    if (live === undefined || live.cancelTimer !== undefined) return
    this.cancelledStreams += 1
    this.post({ kind: 'stream-cancel', id })
    live.cancelTimer = setTimeout(() => {
      if (this.streams.has(id)) void this.terminate('stream-cancel-unresponsive')
    }, WORKER_STREAM_CANCEL_GRACE_MS)
    live.cancelTimer.unref?.()
  }

  private rejectPending(reason: Error): void {
    for (const pending of this.pending.values()) pending.reject(reason)
    this.pending.clear()
    for (const route of this.routePending.values()) route.reject(reason)
    this.routePending.clear()
  }
}

interface LiveStream {
  readonly bridge: WorkerStreamBridge
  unacked: number
  cancelTimer: NodeJS.Timeout | undefined
  ended: boolean
}

/**
 * Parent-side stream bridge: chunks arrive under worker credit control and
 * are acknowledged only as the consumer takes them.
 */
class WorkerStreamBridge extends Readable {
  private readonly queue: Buffer[] = []
  private readonly consume: (bytes: number) => void
  private awaiting = false

  constructor(consume: (bytes: number) => void) {
    super({ highWaterMark: 16 * 1024 })
    this.consume = consume
  }

  pushChunk(chunk: Buffer): void {
    if (this.destroyed) return
    if (this.awaiting) {
      this.awaiting = false
      this.push(chunk)
      this.consume(chunk.byteLength)
      return
    }
    this.queue.push(chunk)
  }

  pushEnd(): void {
    if (!this.destroyed) this.push(null)
  }

  override _read(): void {
    const next = this.queue.shift()
    if (next === undefined) {
      this.awaiting = true
      return
    }
    this.push(next)
    this.consume(next.byteLength)
  }
}
