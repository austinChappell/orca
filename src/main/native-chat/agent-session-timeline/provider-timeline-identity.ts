// How the assembler's keys become persisted journal identities.
//
// The assembler decides WHICH rows are the same row: it mints a key for every turn
// and item from the provider's join keys, or from a per-generation serial when the
// provider names nothing. A scheme only spells that key as an
// `AgentJournalItemIdentity`. It must be pure and deterministic — revisions find
// their row by recomputing it — and it is the one place a provider's persisted
// identity shape lives, so moving a provider to a new identity arm is a change here.

import type {
  AgentJournalItemIdentity,
  AgentType
} from '../../../shared/agent-session-journal-types'

/** A key the provider vouched for, or one the assembler minted because it named nothing. */
export type ProviderTimelineKey = { source: 'provider' | 'minted'; value: string }

/** Which family an item key belongs to; families never share a namespace. */
export type ProviderTimelineItemFamily = 'item' | 'text' | 'request' | 'frame'

export type ProviderTimelineItemRef = {
  family: ProviderTimelineItemFamily
  key: ProviderTimelineKey
}

export type ProviderTimelineIdentityScheme = {
  turn(key: ProviderTimelineKey): AgentJournalItemIdentity
  item(ref: ProviderTimelineItemRef): AgentJournalItemIdentity
}

/** The turn id a turn row carries and a client's Stop names: the provider's, or the minted key. */
export function providerTimelineTurnId(key: ProviderTimelineKey): string {
  return key.value
}

/**
 * The scheme for a provider with no identity arm of its own: the existing `legacy`
 * arm, so no row shape changes. Turn rows keep the `turn-lifecycle:` record
 * prefix the other lanes write. Provider and minted keys are spelled apart so a provider id
 * can never collide with a serial.
 */
export function createLegacyProviderTimelineIdentityScheme(input: {
  agent: AgentType
  sessionId: string
}): ProviderTimelineIdentityScheme {
  const identity = (recordId: string): AgentJournalItemIdentity => ({
    provider: 'legacy',
    agent: input.agent,
    sessionId: input.sessionId,
    recordId
  })
  const spell = (key: ProviderTimelineKey): string =>
    `${key.source === 'provider' ? 'p' : 'm'}:${key.value}`
  return {
    turn: (key) => identity(`turn-lifecycle:${spell(key)}`),
    item: (ref) => identity(`${ref.family}:${spell(ref.key)}`)
  }
}
