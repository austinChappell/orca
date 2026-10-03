// A real assembled lane for tests: grammar events → assembler → deferred sink queue →
// on-disk journal. Assertions read the journal back, so they check what a client sees.

import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { agentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import type {
  AgentJournalMessageItem,
  AgentJournalRenderItem,
  AgentJournalToolCallItem,
  AgentJournalTurnLifecycle
} from '../../../shared/agent-session-journal-types'
import { readAgentJournalTurn } from '../../../shared/agent-session-turn-record'
import { createTrackedJournalOpener } from '../agent-session-journal/journal-host-database-test-support'
import type { AgentSessionJournal } from '../agent-session-journal/journal-store'
import { createDeferredStructuredAgentSessionEventSink } from '../agent-session-wire/structured-agent-session-event-sink'
import { testEventSinkLogging } from '../agent-session-wire/structured-agent-session-logger-test-support'
import {
  createProviderTimelineAssembler,
  type ProviderTimelineAssembler,
  type ProviderTimelineAssemblerDeps
} from './provider-timeline-assembler'
import {
  createLegacyProviderTimelineIdentityScheme,
  type ProviderTimelineItemFamily
} from './provider-timeline-identity'

export const SESSION = 'session-timeline'
export const AGENT = 'grok'
export const GENERATION = 'gen-1'

const scheme = createLegacyProviderTimelineIdentityScheme({ agent: AGENT, sessionId: SESSION })

/** The journal key the assembler gives a provider-keyed item. */
export function providerItemId(family: ProviderTimelineItemFamily, key: string): string {
  return agentJournalItemKey(scheme.item({ family, key: { source: 'provider', value: key } }))
}

/** The journal key of a provider-keyed turn's row. */
export function providerTurnItemId(turnKey: string): string {
  return agentJournalItemKey(scheme.turn({ source: 'provider', value: turnKey }))
}

export function runningTool(name: string): AgentJournalToolCallItem {
  return { kind: 'tool-call', name, input: { name }, state: 'running' }
}

export function assistantText(text: string): AgentJournalMessageItem {
  return { kind: 'message', role: 'assistant', blocks: [{ type: 'text', text }] }
}

const journals = createTrackedJournalOpener()
const cleanups: (() => Promise<void>)[] = []

/** Call from `afterEach`. */
export async function closeProviderTimelineRigs(): Promise<void> {
  for (const cleanup of cleanups.splice(0)) {
    await cleanup()
  }
  await journals.closeAll()
}

export type ProviderTimelineRig = {
  journal: AgentSessionJournal
  assembler: ProviderTimelineAssembler
  sink: ReturnType<typeof createDeferredStructuredAgentSessionEventSink>['sink']
  /** A fresh assembler on the same journal, as a restarted host builds. */
  restart(overrides?: Partial<ProviderTimelineAssemblerDeps>): ProviderTimelineAssembler
  rows(): Promise<AgentJournalRenderItem[]>
  row(itemId: string): Promise<AgentJournalRenderItem | undefined>
  turn(turnId: string): Promise<AgentJournalTurnLifecycle | undefined>
}

export async function openProviderTimelineRig(
  overrides: Partial<ProviderTimelineAssemblerDeps> = {}
): Promise<ProviderTimelineRig> {
  const root = await mkdtemp(join(tmpdir(), 'orca-provider-timeline-'))
  const journal = await journals.open({
    identity: {
      sessionId: SESSION,
      workspaceId: 'workspace-1',
      hostId: 'local',
      agent: AGENT,
      providerHandle: { kind: 'opaque', agent: AGENT, value: 'provider-session-1' }
    },
    stateDirectory: root,
    now: () => 1_000
  })
  const deferred = createDeferredStructuredAgentSessionEventSink(testEventSinkLogging())
  deferred.bind({ journal, fence: 1, publish: () => {} })
  cleanups.push(async () => {
    deferred.close()
    await rm(root, { recursive: true, force: true })
  })
  const build = (more: Partial<ProviderTimelineAssemblerDeps> = {}) =>
    createProviderTimelineAssembler({
      sink: deferred.sink,
      sessionId: SESSION,
      agent: AGENT,
      generation: GENERATION,
      // Every delta writes at once unless a test drives the window itself.
      schedule: (run) => {
        run()
        return () => {}
      },
      ...overrides,
      ...more
    })
  const rows = async () => {
    await deferred.drained()
    return journal.snapshot().items
  }
  return {
    journal,
    assembler: build(),
    sink: deferred.sink,
    restart: build,
    rows,
    row: async (itemId) => (await rows()).find((item) => item.itemId === itemId),
    turn: async (turnId) =>
      (await rows())
        .map((item) => readAgentJournalTurn(item.body))
        .find((turn) => turn?.turnId === turnId) ?? undefined
  }
}
