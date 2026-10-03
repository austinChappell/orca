// Streamed text, as message rows.
//
// One stream is one item. A provider that names its messages keys the item by that
// id; an anonymous stream is keyed by the adapter's stream name plus a serial, and
// ends at the next non-text event so later text starts a fresh item. Deltas go
// through the shared coalescer, so a row is a snapshot of the text so far and a
// burst of tokens costs one write. A stream left open when its turn or the session
// ends keeps the text it received.

import type {
  AgentJournalItemBody,
  AgentJournalItemIdentity,
  AgentJournalRowAttribution
} from '../../../shared/agent-session-journal-types'
import { agentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import {
  boundInlineText,
  DEFAULT_JOURNAL_PAYLOAD_LIMITS
} from '../agent-session-journal/journal-payload-bounds'
import {
  createAgentSessionDeltaCoalescer,
  type AgentSessionDeltaCoalescerDeps
} from '../agent-session-wire/agent-session-delta-coalescer'
import type { StructuredAgentSessionSinkAdmission } from '../agent-session-wire/structured-agent-session-event-sink'
import type {
  ProviderTimelineIdentityScheme,
  ProviderTimelineKey
} from './provider-timeline-identity'
import type {
  ProviderTimelineRowPlacement,
  ProviderTimelineSettlementScope
} from './provider-timeline-items'
import type { ProviderTimelineTextChannel } from './provider-timeline-event'
import {
  ProviderTimelineAdmission,
  type ProviderTimelineSinkWrites
} from './provider-timeline-sink-writes'

type Stream = {
  identity: AgentJournalItemIdentity
  itemId: string
  channel: ProviderTimelineTextChannel
  attribution: AgentJournalRowAttribution
  turnItemId: string | null
  anonymous: boolean
  /** Whether any text reached the journal; a whitespace-only stream completes nothing. */
  written: boolean
}

export type ProviderTimelineTextDelta = {
  streamKey: string
  itemKey?: string
  channel: ProviderTimelineTextChannel
  text: string
}

export class ProviderTimelineTextStreams {
  private readonly streams = new Map<string, Stream>()
  private readonly byItemId = new Map<string, Stream>()
  private readonly coalescer
  private serial = 0
  private admission = new ProviderTimelineAdmission()

  constructor(
    private readonly deps: {
      scheme: ProviderTimelineIdentityScheme
      writes: ProviderTimelineSinkWrites
      generation: () => string
      coalesceMs?: number
      schedule?: AgentSessionDeltaCoalescerDeps['schedule']
    }
  ) {
    this.coalescer = createAgentSessionDeltaCoalescer({
      ...(deps.coalesceMs === undefined ? {} : { windowMs: deps.coalesceMs }),
      ...(deps.schedule ? { schedule: deps.schedule } : {}),
      emit: (itemId, text) => {
        const stream = this.byItemId.get(itemId)
        return stream ? this.persist(stream, text) : true
      }
    })
  }

  delta(
    delta: ProviderTimelineTextDelta,
    placement: ProviderTimelineRowPlacement
  ): StructuredAgentSessionSinkAdmission {
    this.admission = new ProviderTimelineAdmission()
    let stream = this.streams.get(delta.streamKey)
    if (!stream || stream.channel !== delta.channel) {
      this.release(delta.streamKey)
      stream = this.start(delta, placement)
    }
    if (!this.coalescer.append(stream.itemId, delta.text)) {
      this.admission.record({ accepted: false, reason: 'backpressure' })
    }
    return this.admission.result()
  }

  /** Null when no such stream is open. */
  close(streamKey: string, finalText?: string): StructuredAgentSessionSinkAdmission | null {
    const stream = this.streams.get(streamKey)
    if (!stream) {
      return null
    }
    this.admission = new ProviderTimelineAdmission()
    if (finalText === undefined) {
      this.coalescer.flush(stream.itemId)
    } else {
      this.persist(stream, boundInlineText(finalText, DEFAULT_JOURNAL_PAYLOAD_LIMITS).text, true)
    }
    this.forget(streamKey, stream)
    return this.admission.result()
  }

  /** Ends every anonymous stream: a non-text event means their messages are over. */
  releaseAnonymous(): StructuredAgentSessionSinkAdmission {
    this.admission = new ProviderTimelineAdmission()
    for (const [streamKey, stream] of this.streams) {
      if (stream.anonymous) {
        this.release(streamKey)
      }
    }
    return this.admission.result()
  }

  /** Flushes and ends the streams a turn or the session ends, keeping the text they received. */
  settle(scope: ProviderTimelineSettlementScope): StructuredAgentSessionSinkAdmission {
    this.admission = new ProviderTimelineAdmission()
    for (const [streamKey, stream] of this.streams) {
      if (scope === 'session' || stream.turnItemId === scope.turnItemId) {
        this.release(streamKey)
      }
    }
    return this.admission.result()
  }

  flush(): void {
    this.coalescer.flushAll()
  }

  reset(): void {
    for (const stream of this.streams.values()) {
      this.coalescer.forget(stream.itemId)
    }
    this.streams.clear()
    this.byItemId.clear()
    this.serial = 0
  }

  dispose(): void {
    this.reset()
    this.coalescer.dispose()
  }

  private start(delta: ProviderTimelineTextDelta, placement: ProviderTimelineRowPlacement): Stream {
    this.serial += 1
    const key: ProviderTimelineKey = delta.itemKey
      ? { source: 'provider', value: delta.itemKey }
      : { source: 'minted', value: `${this.deps.generation()}:${delta.streamKey}:${this.serial}` }
    const identity = this.deps.scheme.item({ family: 'text', key })
    const stream: Stream = {
      identity,
      itemId: agentJournalItemKey(identity),
      channel: delta.channel,
      attribution: { ...placement.producer, turnScope: placement.scope },
      turnItemId: placement.scope.kind === 'turn' ? placement.scope.turnItemId : null,
      anonymous: delta.itemKey === undefined,
      written: false
    }
    this.streams.set(delta.streamKey, stream)
    this.byItemId.set(stream.itemId, stream)
    return stream
  }

  private release(streamKey: string): void {
    const stream = this.streams.get(streamKey)
    if (stream) {
      this.coalescer.flush(stream.itemId)
      this.forget(streamKey, stream)
    }
  }

  private forget(streamKey: string, stream: Stream): void {
    this.coalescer.forget(stream.itemId)
    this.streams.delete(streamKey)
    this.byItemId.delete(stream.itemId)
  }

  /** False under backpressure, so the coalescer keeps the text for a later flush. */
  private persist(stream: Stream, text: string, providerFinal = false): boolean {
    if (!stream.written && !providerFinal && text.trim().length === 0) {
      return true
    }
    if (!stream.written && text.length === 0) {
      return true
    }
    const body: AgentJournalItemBody = {
      kind: 'message',
      role: stream.channel === 'assistant' ? 'assistant' : 'reasoning',
      blocks: [{ type: 'text', text }]
    }
    const admission = this.deps.writes.append({
      identity: stream.identity,
      body,
      attribution: stream.attribution
    })
    this.admission.record(admission)
    if (!admission.accepted) {
      return false
    }
    stream.written = true
    this.admission.record(this.deps.writes.publish())
    return true
  }
}
