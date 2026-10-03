// The session's open turn and the row that records it.
//
// Sole owner of turn identity for an assembled lane: the row carries the turn id this
// holds, and that id is what a client's Stop names. A turn row is thread-scoped, is
// revised to its end and never tombstoned, and every lifecycle write lands only while
// the journal still holds it running — so a turn settles once even across a restart
// that replays its end.

import type { AgentSessionContextUsage } from '../../../shared/agent-session-context-usage'
import {
  agentJournalItemKey,
  agentJournalSubmissionKey
} from '../../../shared/agent-session-journal-item-key'
import {
  AGENT_JOURNAL_THREAD_SCOPE,
  type AgentJournalItemIdentity,
  type AgentJournalTurnLifecycle,
  type AgentJournalTurnOutcome,
  type AgentJournalTurnScope
} from '../../../shared/agent-session-journal-types'
import { agentJournalTurnBody } from '../../../shared/agent-session-turn-record'
import type {
  StructuredAgentSessionEventSink,
  StructuredAgentSessionSinkAdmission
} from '../agent-session-wire/structured-agent-session-event-sink'
import { writeAgentJournalTurnRow } from './agent-journal-turn-row-revision'
import {
  providerTimelineTurnId,
  type ProviderTimelineIdentityScheme,
  type ProviderTimelineKey
} from './provider-timeline-identity'
import {
  ProviderTimelineAdmission,
  type ProviderTimelineSinkWrites
} from './provider-timeline-sink-writes'

/** Turn keys remembered as settled, so a repeated end or a replayed open is recognised. */
const MAX_SETTLED_TURN_KEYS = 256
/** Sends waiting for a turn; a provider that never opens one cannot grow this without bound. */
const MAX_PENDING_INPUTS = 64

export type ProviderTimelineTurn = {
  key: ProviderTimelineKey
  identity: AgentJournalItemIdentity
  /** The row's journal key: the turn scope of every row written inside it. */
  itemId: string
  running: AgentJournalTurnLifecycle
}

/** How a turn ended: the provider's report, or what the host could prove when the child went. */
export type ProviderTimelineTurnEnd =
  | {
      state: 'completed' | 'interrupted'
      completedAt: number
      outcome?: AgentJournalTurnOutcome
      durationMs?: number
    }
  | { state: 'unverifiable' }

type PendingInput = { clientMessageId: string; requestedAt: number }

export class ProviderTimelineTurns {
  private current: ProviderTimelineTurn | null = null
  private last: ProviderTimelineTurn | null = null
  private readonly settledKeys = new Set<string>()
  private pendingInputs: PendingInput[] = []
  private serial = 0

  constructor(
    private readonly deps: {
      sessionId: string
      scheme: ProviderTimelineIdentityScheme
      sink: StructuredAgentSessionEventSink
      writes: ProviderTimelineSinkWrites
      generation: () => string
    }
  ) {}

  get open(): ProviderTimelineTurn | null {
    return this.current
  }

  /** Which turn a row written now belongs to. */
  get scope(): AgentJournalTurnScope {
    return this.current
      ? { kind: 'turn', turnItemId: this.current.itemId }
      : AGENT_JOURNAL_THREAD_SCOPE
  }

  /** The key an incoming open names, or null when it is the open turn or one already settled.
   *  An open naming no turn while one is open is that turn: only a distinct provider turn supersedes. */
  keyForOpen(turnKey: string | undefined): ProviderTimelineKey | null {
    if (turnKey === undefined) {
      if (this.current) {
        return null
      }
      this.serial += 1
      return { source: 'minted', value: `orca-turn:${this.deps.generation()}:${this.serial}` }
    }
    const key: ProviderTimelineKey = { source: 'provider', value: turnKey }
    return this.isCurrent(key) || this.settledKeys.has(keyString(key)) ? null : key
  }

  /** The open turn an end names; null when it names a settled or an unknown turn. */
  turnForEnd(turnKey: string | undefined): ProviderTimelineTurn | null {
    if (!this.current) {
      return null
    }
    return turnKey === undefined || this.isCurrent({ source: 'provider', value: turnKey })
      ? this.current
      : null
  }

  begin(key: ProviderTimelineKey, at: number): StructuredAgentSessionSinkAdmission {
    const identity = this.deps.scheme.turn(key)
    const itemId = agentJournalItemKey(identity)
    const input = this.pendingInputs.shift()
    const turnId = providerTimelineTurnId(key)
    const running: AgentJournalTurnLifecycle = {
      turnId,
      state: 'running',
      // A turn the provider opened with no send of Orca's keys itself.
      userItemId: input ? agentJournalSubmissionKey(input.clientMessageId) : itemId,
      startedAt: at,
      ...(input ? { requestedAt: input.requestedAt } : {})
    }
    this.current = { key, identity, itemId, running }
    this.deps.writes.setActivity(turnId, null)
    return this.writeRunning(this.current)
  }

  /** A send reached the provider: it names the open turn if nothing opened that one, else the next. */
  acceptInput(input: PendingInput): StructuredAgentSessionSinkAdmission {
    const open = this.current
    if (open && open.running.userItemId === open.itemId) {
      open.running = {
        ...open.running,
        userItemId: agentJournalSubmissionKey(input.clientMessageId),
        requestedAt: input.requestedAt
      }
      return this.writeRunning(open)
    }
    if (!open) {
      this.pendingInputs.push(input)
      this.pendingInputs.splice(0, Math.max(0, this.pendingInputs.length - MAX_PENDING_INPUTS))
    }
    return { accepted: true }
  }

  /** Writes the turn's end and forgets it. Callers settle the turn's items first. */
  end(
    turn: ProviderTimelineTurn,
    end: ProviderTimelineTurnEnd
  ): StructuredAgentSessionSinkAdmission {
    if (this.current === turn) {
      this.current = null
      this.deps.writes.setActivity(turn.running.turnId, null)
    }
    this.last = turn
    this.settledKeys.add(keyString(turn.key))
    for (const oldest of this.settledKeys) {
      if (this.settledKeys.size <= MAX_SETTLED_TURN_KEYS) {
        break
      }
      this.settledKeys.delete(oldest)
    }
    const admission = new ProviderTimelineAdmission()
    admission.record(
      writeAgentJournalTurnRow(
        this.deps.sink,
        { identity: turn.identity },
        {
          lifecycle: agentJournalTurnBody(endedLifecycle(turn.running, end)),
          onlyWhileRunning: true
        },
        { publish: false, options: { lifecycle: true } }
      )
    )
    admission.record(this.deps.writes.publish({ lifecycle: true }))
    return admission.result()
  }

  /** Context facts land on the open turn, else the last one: the provider usually answers after the end. */
  recordContextUsage(usage: AgentSessionContextUsage): StructuredAgentSessionSinkAdmission {
    const target = this.current ?? this.last
    return writeAgentJournalTurnRow(
      this.deps.sink,
      target ? { identity: target.identity } : { newest: true },
      { contextUsage: usage },
      { publish: true }
    )
  }

  /** Forgets everything; a new provider session owes nothing to the old one's turns. */
  reset(): void {
    this.current = null
    this.last = null
    this.settledKeys.clear()
    this.pendingInputs = []
    this.serial = 0
  }

  private isCurrent(key: ProviderTimelineKey): boolean {
    return this.current !== null && keyString(this.current.key) === keyString(key)
  }

  private writeRunning(turn: ProviderTimelineTurn): StructuredAgentSessionSinkAdmission {
    const admission = new ProviderTimelineAdmission()
    admission.record(
      writeAgentJournalTurnRow(
        this.deps.sink,
        { identity: turn.identity },
        { lifecycle: agentJournalTurnBody(turn.running), onlyWhileRunning: true },
        // The running row's ts is the turn start itself, so clients read no append lag.
        { publish: false, options: { lifecycle: true, observedAt: turn.running.startedAt } }
      )
    )
    // Keyed apart, so an end's publication never replaces a start's still waiting to run.
    admission.record(
      this.deps.writes.publish({
        lifecycle: true,
        coalescingKey: `turn-start:${this.deps.sessionId}:${turn.running.turnId}`
      })
    )
    return admission.result()
  }
}

function keyString(key: ProviderTimelineKey): string {
  return `${key.source}:${key.value}`
}

/** The end owns the turn's terminal fields; `unverifiable` carries no end and no verdict. */
function endedLifecycle(
  running: AgentJournalTurnLifecycle,
  end: ProviderTimelineTurnEnd
): AgentJournalTurnLifecycle {
  const { turnId, userItemId, startedAt, requestedAt } = running
  const kept = {
    turnId,
    ...(userItemId !== undefined ? { userItemId } : {}),
    ...(startedAt !== undefined ? { startedAt } : {}),
    ...(requestedAt !== undefined ? { requestedAt } : {})
  }
  if (end.state === 'unverifiable') {
    return { ...kept, state: 'unverifiable' }
  }
  return {
    ...kept,
    state: end.state,
    ...(end.outcome !== undefined ? { outcome: end.outcome } : {}),
    completedAt: end.completedAt,
    ...(end.durationMs !== undefined ? { durationMs: end.durationMs } : {})
  }
}
