// The assembler's writes through the existing event sink. Every write reports the
// sink's admission; the first refusal of an event is what that event returns, so an
// adapter can pause reading under backpressure the way the other lanes do.

import type {
  AgentJournalItemBody,
  AgentJournalItemIdentity,
  AgentJournalRowAttribution
} from '../../../shared/agent-session-journal-types'
import { partitionJournalLifecycleMutations } from '../agent-session-journal/journal-lifecycle-batch-partition'
import {
  journalLifecycleItemMutation,
  type JournalLifecycleMutationInput
} from '../agent-session-journal/journal-row-builders'
import type {
  StructuredAgentSessionEventSink,
  StructuredAgentSessionPublishOptions,
  StructuredAgentSessionSinkAdmission
} from '../agent-session-wire/structured-agent-session-event-sink'

const ADMITTED: StructuredAgentSessionSinkAdmission = { accepted: true }

/** The first refusal among one event's writes; later writes still run so state stays whole. */
export class ProviderTimelineAdmission {
  private refused: StructuredAgentSessionSinkAdmission | null = null

  record(admission: StructuredAgentSessionSinkAdmission): void {
    if (!admission.accepted) {
      this.refused ??= admission
    }
  }

  result(): StructuredAgentSessionSinkAdmission {
    return this.refused ?? ADMITTED
  }
}

export type ProviderTimelineRow = {
  identity: AgentJournalItemIdentity
  body: AgentJournalItemBody
  attribution: AgentJournalRowAttribution
}

export class ProviderTimelineSinkWrites {
  constructor(private readonly sink: StructuredAgentSessionEventSink) {}

  /** One row; `lifecycle` rides the sink's lifecycle budget. */
  append(row: ProviderTimelineRow, lifecycle = false): StructuredAgentSessionSinkAdmission {
    const options = { ...row.attribution, ...(lifecycle ? { lifecycle: true } : {}) }
    if (this.sink.tryAppendItem) {
      return this.sink.tryAppendItem(row.identity, row.body, options)
    }
    this.sink.appendItem(row.identity, row.body, options)
    return ADMITTED
  }

  publish(options: StructuredAgentSessionPublishOptions = {}): StructuredAgentSessionSinkAdmission {
    if (this.sink.tryPublish) {
      return this.sink.tryPublish(options)
    }
    this.sink.publish(options)
    return ADMITTED
  }

  /** Rows that settle together, in one lifecycle batch per partition. */
  settle(
    settlementId: string,
    rows: readonly ProviderTimelineRow[]
  ): StructuredAgentSessionSinkAdmission {
    const mutations: JournalLifecycleMutationInput[] = rows.map((row) =>
      journalLifecycleItemMutation(row.attribution, row.identity, row.body)
    )
    const admission = new ProviderTimelineAdmission()
    const batch = this.sink.tryAppendLifecycleBatch ?? this.sink.appendLifecycleBatch
    if (!batch) {
      for (const row of rows) {
        admission.record(this.append(row, true))
      }
      return admission.result()
    }
    for (const chunk of partitionJournalLifecycleMutations(settlementId, mutations)) {
      admission.record(
        batch.call(this.sink, chunk.settlementId, chunk.mutations, { lifecycle: true }) ?? ADMITTED
      )
    }
    return admission.result()
  }

  setActivity(turnId: string, text: string | null): void {
    this.sink.setActivity?.(text ? { turnId, text } : null)
  }
}
