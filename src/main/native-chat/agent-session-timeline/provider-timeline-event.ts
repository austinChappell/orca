// The narrow grammar a provider adapter speaks to the timeline assembler.
//
// The adapter knows the provider's dialect; the assembler knows the timeline. An
// adapter parses provider traffic into these semantic events and never writes a
// journal row or mints a journal identity itself: every key here is the
// provider's own join key (a turn id, a tool-call id, a message id) or a name the
// adapter chose for an anonymous stream. The assembler mints identity, scopes
// rows to turns, and owns every turn and item rule in
// `agent-session-journal-types.ts`.
//
// Rules an adapter can rely on:
// - Only `turn.open` opens a turn. Items, text, and requests never do: one that
//   arrives with no turn open is written as a thread row. An adapter whose
//   provider starts work on its own (a background wake, auto-compaction) decides
//   that a turn began and says `turn.open` before that work's items.
// - A turn settles once. A repeated or unknown `turn.end` is dropped, and the
//   journal row refuses a second settlement even after a restart.
// - `outcome` is the provider's verdict and is never inferred. An end the host
//   saw rather than heard (`session.ended`) carries none.
// - After `session.ended` every event is dropped until `session.reset`.

import type { AgentSessionContextUsage } from '../../../shared/agent-session-context-usage'
import type {
  AgentJournalApprovalItem,
  AgentJournalItemBody,
  AgentJournalProducerLinkage,
  AgentJournalQuestionItem,
  AgentJournalTurnOutcome
} from '../../../shared/agent-session-journal-types'
import type { StructuredAgentSessionTurnVerdict } from '../agent-session-wire/structured-agent-session-stale-turn-verdict'

/** Bodies an item event may carry. Turn rows are the assembler's; prompts travel as requests. */
export type ProviderTimelineItemBody = Exclude<
  AgentJournalItemBody,
  { kind: 'turn' | 'approval' | 'question' }
>

export type ProviderTimelineRequestBody = AgentJournalApprovalItem | AgentJournalQuestionItem

/** `assistant` is reply text; `reasoning` is the model's visible thinking. */
export type ProviderTimelineTextChannel = 'assistant' | 'reasoning'

type Produced = {
  /** The subagent that produced this, when not the session's own agent. Stamped on the row as is. */
  producer?: AgentJournalProducerLinkage
}

export type ProviderTimelineEvent =
  /** Orca's send reached the provider. The next turn to open names it as its opener. */
  | { type: 'input.accepted'; clientMessageId: string; requestedAt: number }
  /** A turn began. `turnKey` is the provider's turn id when it has one; the assembler mints one otherwise. */
  | { type: 'turn.open'; turnKey?: string; at: number }
  /** The provider ended a turn. Absent `turnKey` means the open turn. */
  | {
      type: 'turn.end'
      turnKey?: string
      at: number
      state: 'completed' | 'interrupted'
      /** The provider's own verdict. Absent means it gave none, which reads as unknown. */
      outcome?: AgentJournalTurnOutcome
      /** The provider's own measured duration. */
      durationMs?: number
    }
  /** Work began. `outlivesTurn` keeps it open past its turn's end (a backgrounded task). */
  | ({
      type: 'item.open'
      itemKey: string
      body: ProviderTimelineItemBody
      outlivesTurn?: true
    } & Produced)
  /** The item's whole current body. Revises a settled item in place too (a replaced plan). */
  | ({ type: 'item.update'; itemKey: string; body: ProviderTimelineItemBody } & Produced)
  /** The item's whole terminal body. A second close of a settled item is dropped. */
  | ({ type: 'item.close'; itemKey: string; body: ProviderTimelineItemBody } & Produced)
  /** Streamed text. `itemKey` is the provider's message id when it names its messages. */
  | ({
      type: 'text.delta'
      streamKey: string
      itemKey?: string
      channel: ProviderTimelineTextChannel
      text: string
    } & Produced)
  /** The stream ended; `text` is the provider's final text, else what streamed is kept. */
  | { type: 'text.close'; streamKey: string; text?: string }
  /** The provider asked the user something and waits on the answer. */
  | ({ type: 'request.open'; requestKey: string; body: ProviderTimelineRequestBody } & Produced)
  /** The provider stopped waiting for an answer it never got. */
  | { type: 'request.withdrawn'; requestKey: string }
  /** What the provider said about its context window, for the open turn or else the last one. */
  | { type: 'context.usage'; usage: AgentSessionContextUsage }
  /** The live activity line for the open turn; null clears it. */
  | { type: 'activity'; text: string | null }
  /** Provider traffic no typed event covers; it becomes the shared bounded fallback row. */
  | { type: 'provider.frame'; frameKind: string; payload: unknown }
  /** The provider child is gone. The verdict is what the host can prove about its end. */
  | { type: 'session.ended'; verdict: StructuredAgentSessionTurnVerdict }
  /** A new provider session began (a new acquisition); keys mint in a fresh namespace. */
  | { type: 'session.reset'; generation: string }
