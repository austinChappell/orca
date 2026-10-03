// Turns a provider adapter's grammar events (`provider-timeline-event.ts`) into the
// journal rows every structured lane writes, through the existing event sink.
//
// Owns every timeline rule a provider lane would otherwise re-implement: identity,
// turn scoping, a turn settling once with only the provider's verdict, items and
// requests settling with their turn, an `unverifiable` end carrying no end time,
// and dropping duplicates. It owns nothing the journal already derives — a
// person's Stop becoming a cancellation, a dead generation found after a restart,
// the compare-and-set on an answer — and nothing provider-specific.

import { agentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import type { AgentType } from '../../../shared/agent-session-journal-types'
import { unhandledProviderFrameJournalItem } from '../agent-session-wire/unhandled-provider-frame'
import type { AgentSessionDeltaCoalescerDeps } from '../agent-session-wire/agent-session-delta-coalescer'
import type {
  StructuredAgentSessionEventSink,
  StructuredAgentSessionSinkAdmission
} from '../agent-session-wire/structured-agent-session-event-sink'
import type { ProviderTimelineEvent } from './provider-timeline-event'
import {
  createLegacyProviderTimelineIdentityScheme,
  type ProviderTimelineIdentityScheme
} from './provider-timeline-identity'
import { ProviderTimelineItems, type ProviderTimelineRowPlacement } from './provider-timeline-items'
import {
  ProviderTimelineAdmission,
  ProviderTimelineSinkWrites
} from './provider-timeline-sink-writes'
import { ProviderTimelineTextStreams } from './provider-timeline-text-streams'
import {
  ProviderTimelineTurns,
  type ProviderTimelineTurn,
  type ProviderTimelineTurnEnd
} from './provider-timeline-turns'

/** Why an event wrote nothing. Each is a grammar rule the adapter broke or a replay it repeated. */
export type ProviderTimelineDropRule =
  | 'session-ended'
  | 'turn-duplicate'
  | 'turn-unknown'
  | 'item-settled'
  | 'request-duplicate'
  | 'request-unknown'
  | 'stream-unknown'
  | 'no-turn'

export type ProviderTimelineApplyResult = {
  /** The sink's first refusal among the event's writes, so the adapter can pause reading. */
  admission: StructuredAgentSessionSinkAdmission
  dropped?: ProviderTimelineDropRule
}

export type ProviderTimelineAssembler = {
  apply(event: ProviderTimelineEvent): ProviderTimelineApplyResult
  /** A client's answer to the request with this journal key won; it is never cancelled after. */
  requestResolved(itemId: string): void
  /** The turn id of the open turn, as its row and a client's Stop name it. */
  readonly openTurnId: string | null
  flush(): void
  dispose(): void
}

export type ProviderTimelineAssemblerDeps = {
  sink: StructuredAgentSessionEventSink
  sessionId: string
  agent: AgentType
  /** The acquisition this provider session belongs to; minted keys are unique per generation. */
  generation: string
  /** Defaults to the shared `legacy` identity arm. */
  scheme?: ProviderTimelineIdentityScheme
  coalesceMs?: number
  schedule?: AgentSessionDeltaCoalescerDeps['schedule']
}

const ADMITTED: StructuredAgentSessionSinkAdmission = { accepted: true }

export function createProviderTimelineAssembler(
  deps: ProviderTimelineAssemblerDeps
): ProviderTimelineAssembler {
  let generation = deps.generation
  let ended = false
  let frameSerial = 0
  const scheme =
    deps.scheme ??
    createLegacyProviderTimelineIdentityScheme({ agent: deps.agent, sessionId: deps.sessionId })
  const writes = new ProviderTimelineSinkWrites(deps.sink)
  const turns = new ProviderTimelineTurns({
    sessionId: deps.sessionId,
    scheme,
    sink: deps.sink,
    writes,
    generation: () => generation
  })
  const items = new ProviderTimelineItems({ scheme, writes })
  const streams = new ProviderTimelineTextStreams({
    scheme,
    writes,
    generation: () => generation,
    ...(deps.coalesceMs === undefined ? {} : { coalesceMs: deps.coalesceMs }),
    ...(deps.schedule ? { schedule: deps.schedule } : {})
  })

  const placement = (
    producer?: ProviderTimelineRowPlacement['producer']
  ): ProviderTimelineRowPlacement => ({
    scope: turns.scope,
    ...(producer ? { producer } : {})
  })

  /** A turn's open items, requests, and streams settle in one batch ahead of its row. */
  const settleTurn = (
    turn: ProviderTimelineTurn,
    end: ProviderTimelineTurnEnd
  ): StructuredAgentSessionSinkAdmission => {
    const admission = new ProviderTimelineAdmission()
    const scope = { turnItemId: turn.itemId }
    admission.record(streams.settle(scope))
    const rows = items.takeSettlement(scope)
    if (rows.length > 0) {
      const id = `provider-timeline:turn-end:${deps.sessionId}:${turn.running.turnId}`
      admission.record(writes.settle(id, rows))
    }
    admission.record(turns.end(turn, end))
    return admission.result()
  }

  const endSession = (end: ProviderTimelineTurnEnd): StructuredAgentSessionSinkAdmission => {
    const admission = new ProviderTimelineAdmission()
    admission.record(streams.settle('session'))
    const rows = items.takeSettlement('session')
    if (rows.length > 0) {
      admission.record(
        writes.settle(`provider-timeline:session-end:${deps.sessionId}:${generation}`, rows)
      )
    }
    const open = turns.open
    if (open) {
      admission.record(turns.end(open, end))
    } else {
      admission.record(writes.publish({ lifecycle: true }))
    }
    return admission.result()
  }

  const result = (
    admission: StructuredAgentSessionSinkAdmission | null,
    rule: ProviderTimelineDropRule
  ): ProviderTimelineApplyResult =>
    admission ? { admission } : { admission: ADMITTED, dropped: rule }

  /** A non-text event ends the anonymous streams before it lands, keeping their order. */
  const afterStreams = (write: () => ProviderTimelineApplyResult): ProviderTimelineApplyResult => {
    const released = streams.releaseAnonymous()
    const applied = write()
    return released.accepted ? applied : { ...applied, admission: released }
  }

  const apply = (event: ProviderTimelineEvent): ProviderTimelineApplyResult => {
    if (event.type === 'session.reset') {
      generation = event.generation
      ended = false
      frameSerial = 0
      streams.reset()
      items.reset()
      turns.reset()
      return { admission: ADMITTED }
    }
    if (ended) {
      return { admission: ADMITTED, dropped: 'session-ended' }
    }
    switch (event.type) {
      case 'input.accepted':
        return { admission: turns.acceptInput(event) }
      case 'turn.open':
        return afterStreams(() => {
          const key = turns.keyForOpen(event.turnKey)
          if (!key) {
            return { admission: ADMITTED, dropped: 'turn-duplicate' }
          }
          const admission = new ProviderTimelineAdmission()
          const superseded = turns.open
          if (superseded) {
            // A newer turn ended this one, whoever asked for it.
            admission.record(
              settleTurn(superseded, {
                state: 'interrupted',
                completedAt: event.at,
                outcome: 'superseded'
              })
            )
          }
          admission.record(turns.begin(key, event.at))
          return { admission: admission.result() }
        })
      case 'turn.end': {
        const turn = turns.turnForEnd(event.turnKey)
        if (!turn) {
          return {
            admission: ADMITTED,
            dropped: turns.open || event.turnKey ? 'turn-unknown' : 'no-turn'
          }
        }
        return {
          admission: settleTurn(turn, {
            state: event.state,
            completedAt: event.at,
            ...(event.outcome !== undefined ? { outcome: event.outcome } : {}),
            ...(event.durationMs !== undefined ? { durationMs: event.durationMs } : {})
          })
        }
      }
      case 'item.open':
        return afterStreams(() => ({
          admission: items.openItem(
            event.itemKey,
            event.body,
            placement(event.producer),
            event.outlivesTurn
          )
        }))
      case 'item.update':
        return afterStreams(() => ({
          admission: items.updateItem(event.itemKey, event.body, placement(event.producer))
        }))
      case 'item.close':
        return afterStreams(() =>
          result(
            items.closeItem(event.itemKey, event.body, placement(event.producer)),
            'item-settled'
          )
        )
      case 'text.delta':
        return { admission: streams.delta(event, placement(event.producer)) }
      case 'text.close':
        return result(streams.close(event.streamKey, event.text), 'stream-unknown')
      case 'request.open':
        return afterStreams(() =>
          result(
            items.openRequest(event.requestKey, event.body, placement(event.producer)),
            'request-duplicate'
          )
        )
      case 'request.withdrawn': {
        const row = items.withdrawRequest(event.requestKey)
        if (!row) {
          return { admission: ADMITTED, dropped: 'request-unknown' }
        }
        const id = `provider-timeline:request-withdrawn:${deps.sessionId}:${agentJournalItemKey(row.identity)}`
        const admission = writes.settle(id, [row])
        return { admission: admission.accepted ? writes.publish({ lifecycle: true }) : admission }
      }
      case 'context.usage':
        return { admission: turns.recordContextUsage(event.usage) }
      case 'activity': {
        const open = turns.open
        if (!open) {
          return { admission: ADMITTED, dropped: 'no-turn' }
        }
        writes.setActivity(open.running.turnId, event.text)
        return { admission: ADMITTED }
      }
      case 'provider.frame':
        return afterStreams(() => {
          const frame = unhandledProviderFrameJournalItem(
            deps.agent,
            event.frameKind,
            event.payload
          )
          if (!frame) {
            return { admission: ADMITTED }
          }
          frameSerial += 1
          const key = { source: 'minted' as const, value: `${generation}:${frameSerial}` }
          const admission = writes.append({
            identity: scheme.item({ family: 'frame', key }),
            body: frame.body,
            attribution: { turnScope: turns.scope }
          })
          return { admission: admission.accepted ? writes.publish() : admission }
        })
      case 'session.ended': {
        ended = true
        const admission = endSession(event.verdict)
        streams.reset()
        items.reset()
        turns.reset()
        return { admission }
      }
    }
  }

  return {
    apply,
    requestResolved: (itemId) => items.forgetResolvedRequest(itemId),
    get openTurnId() {
      return turns.open?.running.turnId ?? null
    },
    flush: () => streams.flush(),
    dispose: () => {
      streams.dispose()
      items.reset()
      turns.reset()
    }
  }
}
