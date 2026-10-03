// Items and requests the provider has open, and the rows that settle them.
//
// An item's identity comes from its provider key, so a reopen or a replayed event
// addresses the same row. A close carries the whole terminal body; a second close of
// a settled item is a duplicate. Whatever is still open when its turn or the session
// ends is settled here: a running tool call fails, and a pending prompt is cancelled
// unless a client's answer already resolved it.

import type {
  AgentJournalItemBody,
  AgentJournalProducerLinkage,
  AgentJournalTurnScope
} from '../../../shared/agent-session-journal-types'
import { agentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import { cancelledJournalPromptBody } from '../agent-session-journal/journal-prompt-body-bounds'
import type { StructuredAgentSessionSinkAdmission } from '../agent-session-wire/structured-agent-session-event-sink'
import type {
  ProviderTimelineIdentityScheme,
  ProviderTimelineItemFamily
} from './provider-timeline-identity'
import type {
  ProviderTimelineRow,
  ProviderTimelineSinkWrites
} from './provider-timeline-sink-writes'

/** Provider keys remembered as settled, so a repeated close is recognised. */
const MAX_SETTLED_ITEM_KEYS = 512

type TrackedRow = ProviderTimelineRow & {
  family: ProviderTimelineItemFamily
  providerKey: string
  /** The turn row the item was opened inside; null for a thread row. */
  turnItemId: string | null
  outlivesTurn: boolean
}

export type ProviderTimelineRowPlacement = {
  scope: AgentJournalTurnScope
  producer?: AgentJournalProducerLinkage
}

/** Which open rows a settlement covers: one turn's, or everything when the session ends. */
export type ProviderTimelineSettlementScope = { turnItemId: string } | 'session'

export class ProviderTimelineItems {
  private readonly open = new Map<string, TrackedRow>()
  private readonly settled = new Set<string>()
  private readonly pending = new Map<string, TrackedRow>()

  constructor(
    private readonly deps: {
      scheme: ProviderTimelineIdentityScheme
      writes: ProviderTimelineSinkWrites
    }
  ) {}

  openItem(
    itemKey: string,
    body: AgentJournalItemBody,
    placement: ProviderTimelineRowPlacement,
    outlivesTurn = false
  ): StructuredAgentSessionSinkAdmission {
    const row = this.track('item', itemKey, body, placement, outlivesTurn)
    this.settled.delete(mapKey('item', itemKey))
    this.open.set(mapKey('item', itemKey), row)
    return this.write(row, false)
  }

  /** The whole current body. A settled item is revised in place and stays settled. */
  updateItem(
    itemKey: string,
    body: AgentJournalItemBody,
    placement: ProviderTimelineRowPlacement
  ): StructuredAgentSessionSinkAdmission {
    const key = mapKey('item', itemKey)
    const existing = this.open.get(key)
    const row = existing
      ? { ...existing, body }
      : this.track('item', itemKey, body, placement, false)
    if (!this.settled.has(key)) {
      this.open.set(key, row)
    }
    return this.write(row, false)
  }

  /** Null when the item already settled: the close is a duplicate. */
  closeItem(
    itemKey: string,
    body: AgentJournalItemBody,
    placement: ProviderTimelineRowPlacement
  ): StructuredAgentSessionSinkAdmission | null {
    const key = mapKey('item', itemKey)
    if (this.settled.has(key)) {
      return null
    }
    const existing = this.open.get(key)
    const row = existing
      ? { ...existing, body }
      : this.track('item', itemKey, body, placement, false)
    this.open.delete(key)
    this.rememberSettled(key)
    return this.write(row, true)
  }

  /** Null when the same request is already pending. */
  openRequest(
    requestKey: string,
    body: AgentJournalItemBody,
    placement: ProviderTimelineRowPlacement
  ): StructuredAgentSessionSinkAdmission | null {
    const key = mapKey('request', requestKey)
    if (this.pending.has(key)) {
      return null
    }
    const row = this.track('request', requestKey, body, placement, false)
    this.pending.set(key, row)
    return this.write(row, true)
  }

  /** Null when no such request is pending. */
  withdrawRequest(requestKey: string): ProviderTimelineRow | null {
    const key = mapKey('request', requestKey)
    const row = this.pending.get(key)
    this.pending.delete(key)
    return row ? cancelled(row) : null
  }

  /** A client's answer won the journal's compare-and-set; the row is resolved, never re-cancelled. */
  forgetResolvedRequest(itemId: string): void {
    for (const [key, row] of this.pending) {
      if (agentJournalItemKey(row.identity) === itemId) {
        this.pending.delete(key)
      }
    }
  }

  /** Rows that settle with a turn or the session, forgotten as they are returned. */
  takeSettlement(scope: ProviderTimelineSettlementScope): ProviderTimelineRow[] {
    const rows: ProviderTimelineRow[] = []
    const covers = (row: TrackedRow): boolean =>
      scope === 'session' || (!row.outlivesTurn && row.turnItemId === scope.turnItemId)
    for (const [key, row] of this.open) {
      if (!covers(row)) {
        continue
      }
      this.open.delete(key)
      this.rememberSettled(key)
      const body = interruptedBody(row.body)
      if (body) {
        rows.push({ ...row, body })
      }
    }
    for (const [key, row] of this.pending) {
      if (covers(row)) {
        this.pending.delete(key)
        const settled = cancelled(row)
        if (settled) {
          rows.push(settled)
        }
      }
    }
    return rows
  }

  reset(): void {
    this.open.clear()
    this.settled.clear()
    this.pending.clear()
  }

  private track(
    family: ProviderTimelineItemFamily,
    providerKey: string,
    body: AgentJournalItemBody,
    placement: ProviderTimelineRowPlacement,
    outlivesTurn: boolean
  ): TrackedRow {
    return {
      family,
      providerKey,
      identity: this.deps.scheme.item({ family, key: { source: 'provider', value: providerKey } }),
      body,
      attribution: { ...placement.producer, turnScope: placement.scope },
      turnItemId: placement.scope.kind === 'turn' ? placement.scope.turnItemId : null,
      outlivesTurn
    }
  }

  private write(row: ProviderTimelineRow, lifecycle: boolean): StructuredAgentSessionSinkAdmission {
    const admission = this.deps.writes.append(row, lifecycle)
    return admission.accepted ? this.deps.writes.publish(lifecycle ? { lifecycle } : {}) : admission
  }

  private rememberSettled(key: string): void {
    this.settled.add(key)
    for (const oldest of this.settled) {
      if (this.settled.size <= MAX_SETTLED_ITEM_KEYS) {
        break
      }
      this.settled.delete(oldest)
    }
  }
}

function mapKey(family: ProviderTimelineItemFamily, providerKey: string): string {
  return `${family}:${providerKey}`
}

function cancelled(row: ProviderTimelineRow): ProviderTimelineRow | null {
  const body = cancelledJournalPromptBody(row.body)
  return body ? { ...row, body } : null
}

/** What an item still open at its end becomes; null when its last body already stands. */
function interruptedBody(body: AgentJournalItemBody): AgentJournalItemBody | null {
  return body.kind === 'tool-call' && body.state === 'running' ? { ...body, state: 'failed' } : null
}
