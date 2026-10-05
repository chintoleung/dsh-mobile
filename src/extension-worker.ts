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
    this.exited = new Promise<void>(resolve => {
      this.worker.once('exit', () => {
        this.dead = true
        this.rejectPending(new MobileExtensionError('extension_host_unavailable', `extension ${options.manifest.id} worker exited`, 503))
        for (const stream of this.streams.values()) stream.bridge.destroy()
        this.streams.clear()
        resolve()
      })
    })
    this.worker.on('message', (message: WorkerMessage) => { this.handleMessage(message) })
    this.worker.on('error', () => {
      // Uncaught worker exception: force termination so pending callers settle.
      void this.terminate('worker-error')
    })
  }

  /** Activate the generation inside the worker and receive metadata-only registration. */
  async activate(): Promise<ExtensionWorkerActivation> {
    const reply = await this.rpc(LIFECYCLE_ID, {
      kind: 'activate',
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
    const onAbort = (): void => {
      this.post({ kind: 'cancel', id })
      this.cancelStream(id, 'caller-abort')
    }
    if (request.signal.aborted) throw request.signal.reason ?? new MobileExtensionError('extension_route_cancelled', 'caller detached', 409)
    request.signal.addEventListener('abort', onAbort, { once: true })
    try {
      return await new Promise<MobileRouteResponse>((resolve, reject) => {
        this.routePending.set(id, { resolve, reject })
        this.post({
          kind: 'route', id, routeIndex,
          method: request.method, path: request.pathname,
          query: [...request.query.entries()].map(([key, value]) => [key, value] as const),
          headers: request.headers, body: new Uint8Array(request.body), deviceId: request.deviceId,
        })
      })
    } finally {
      request.signal.removeEventListener('abort', onAbort)
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
      await this.rpc(LIFECYCLE_ID, { kind: 'dispose' }, DISPOSE_GRACE_MS, new AbortController().signal)
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
    const reply = await new Promise<PreparedJsonResult>((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      let timer: NodeJS.Timeout | undefined
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          this.pending.delete(id)
          reject(new MobileExtensionError('host_load_timeout', `extension ${this.options.manifest.id} worker did not answer`, 500))
        }, timeoutMs)
        timer.unref?.()
      }
      const settle = (): void => { if (timer !== undefined) clearTimeout(timer) }
      const wrappedResolve = (value: PreparedJsonResult): void => { settle(); resolve(value) }
      const wrappedReject = (reason: Error): void => { settle(); reject(reason) }
      this.pending.set(id, { resolve: wrappedResolve, reject: wrappedReject })
      signal.addEventListener('abort', () => {
        // The caller detached, but the RPC stays pending: settlement must
        // observe the underlying work, never the caller's lifetime.
      }, { once: true })
      this.post(message)
    })
    return reply
  }

  private post(message: ParentMessage): void {
    if (!this.dead) this.worker.postMessage(message)
  }

  private handleMessage(message: WorkerMessage): void {
    if (message.kind === 'log') {
      this.options.logger[message.level]?.(...message.args)
      return
    }
    if (message.kind === 'activated') {
      // Replies from obsolete runtimes are ignored.
      if (this.runtimeId !== undefined && message.runtimeId !== this.runtimeId) return
      this.runtimeId = message.runtimeId
      this.activation = { runtimeId: message.runtimeId, actions: message.actions, routes: message.routes }
      this.pending.get(LIFECYCLE_ID)?.resolve(new PreparedJsonResult(Buffer.alloc(0)))
      this.pending.delete(LIFECYCLE_ID)
      return
    }
    if (this.runtimeId !== undefined && message.runtimeId !== this.runtimeId) return
    if (message.kind === 'result') {
      const pending = this.pending.get(message.id)
      this.pending.delete(message.id)
      pending?.resolve(new PreparedJsonResult(Buffer.from(message.bytes)))
      return
    }
    if (message.kind === 'error') {
      const pending = this.pending.get(message.id)
      this.pending.delete(message.id)
      pending?.reject(new MobileExtensionError(message.code, message.message, message.status))
      const route = this.routePending.get(message.id)
      this.routePending.delete(message.id)
      route?.reject(new MobileExtensionError(message.code, message.message, message.status))
      return
    }
    if (message.kind === 'route-response') {
      const route = this.routePending.get(message.id)
      this.routePending.delete(message.id)
      route?.resolve({
        ...(message.status === undefined ? {} : { status: message.status }),
        ...(message.contentType === undefined ? {} : { contentType: message.contentType }),
        ...(message.headers === undefined ? {} : { headers: message.headers }),
        body: message.bytes === undefined ? Buffer.alloc(0) : Buffer.from(message.bytes),
      })
      return
    }
    if (message.kind === 'stream-start') {
      const route = this.routePending.get(message.id)
      this.routePending.delete(message.id)
      if (route === undefined) return
      const live = this.openStream(message.id)
      route.resolve({
        ...(message.status === undefined ? {} : { status: message.status }),
        ...(message.contentType === undefined ? {} : { contentType: message.contentType }),
        ...(message.headers === undefined ? {} : { headers: message.headers }),
        body: live.bridge,
      })
      return
    }
    if (message.kind === 'stream-chunk') {
      const live = this.streams.get(message.id)
      if (live === undefined) return
      live.unacked += message.bytes.byteLength
      this.maxBufferedBytes = Math.max(this.maxBufferedBytes, this.totalUnacked())
      live.bridge.pushChunk(Buffer.from(message.bytes))
      return
    }
    if (message.kind === 'stream-end') {
      this.closeStream(message.id)
      return
    }
    if (message.kind === 'stream-error') {
      const live = this.streams.get(message.id)
      this.closeStream(message.id)
      live?.bridge.destroy(new Error(message.message))
    }
  }

  /** Aggregate unacknowledged bytes across all live streams (per-worker bound). */
  private totalUnacked(): number {
    let total = 0
    for (const live of this.streams.values()) total += live.unacked
    return total
  }

  private openStream(id: string): LiveStream {
    const live: LiveStream = { bridge: new WorkerStreamBridge(bytes => { live.unacked = Math.max(0, live.unacked - bytes) }), unacked: 0, cancelTimer: undefined, ended: false }
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
