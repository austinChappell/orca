import { afterEach, describe, expect, it } from 'vitest'
import { agentJournalSubmissionKey } from '../../../shared/agent-session-journal-item-key'
import {
  closeProviderTimelineRigs,
  GENERATION,
  openProviderTimelineRig,
  providerTurnItemId
} from './provider-timeline-assembler-test-support'

afterEach(closeProviderTimelineRigs)

describe('provider timeline turns', () => {
  it('opens a running turn and settles it with the provider verdict and duration', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'input.accepted', clientMessageId: 'send-1', requestedAt: 900 })
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    expect(rig.assembler.openTurnId).toBe('turn-1')
    expect(await rig.turn('turn-1')).toEqual({
      turnId: 'turn-1',
      state: 'running',
      userItemId: agentJournalSubmissionKey('send-1'),
      startedAt: 1_000,
      requestedAt: 900
    })
    const runningRow = await rig.row(providerTurnItemId('turn-1'))
    // The running row is stamped at the turn start, not at append time.
    expect(runningRow?.observedAt).toBe(1_000)

    rig.assembler.apply({
      type: 'turn.end',
      at: 3_000,
      state: 'completed',
      outcome: 'success',
      durationMs: 1_800
    })
    expect(rig.assembler.openTurnId).toBeNull()
    const row = await rig.row(providerTurnItemId('turn-1'))
    expect(row?.body).toEqual({
      kind: 'turn',
      turnId: 'turn-1',
      state: 'completed',
      outcome: 'success',
      userItemId: agentJournalSubmissionKey('send-1'),
      startedAt: 1_000,
      requestedAt: 900,
      completedAt: 3_000,
      durationMs: 1_800
    })
    // Turn rows belong to no turn, and are revised rather than replaced.
    expect(row?.turnScope).toEqual({ kind: 'thread' })
    expect(row?.revision).toBeGreaterThan(runningRow?.revision ?? Infinity)
  })

  it('records a cancelled turn as interrupted with the cancellation verdict', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({
      type: 'turn.end',
      at: 2_000,
      state: 'interrupted',
      outcome: 'cancellation'
    })
    expect(await rig.turn('turn-1')).toMatchObject({
      state: 'interrupted',
      outcome: 'cancellation',
      completedAt: 2_000
    })
  })

  it('keeps a provider-reported failure a completed turn whose verdict says it failed', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed', outcome: 'failure' })
    expect(await rig.turn('turn-1')).toMatchObject({ state: 'completed', outcome: 'failure' })
  })

  it('leaves the verdict absent when the provider gave none', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed' })
    const turn = await rig.turn('turn-1')
    expect(turn?.state).toBe('completed')
    expect(turn).not.toHaveProperty('outcome')
  })

  it('mints a turn id when the provider names none, and keys a turn no send opened by its own row', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', at: 1_000 })
    const turnId = rig.assembler.openTurnId
    expect(turnId).toBe(`orca-turn:${GENERATION}:1`)
    const rows = await rig.rows()
    const [row] = rows
    expect(rows).toHaveLength(1)
    expect(row?.body).toMatchObject({ kind: 'turn', turnId, userItemId: row?.itemId })
  })

  it('names the open turn with a send that arrives after a provider-opened turn began', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({ type: 'input.accepted', clientMessageId: 'send-1', requestedAt: 1_100 })
    // A second send folds into the turn; it does not replace the opener.
    rig.assembler.apply({ type: 'input.accepted', clientMessageId: 'send-2', requestedAt: 1_200 })
    expect(await rig.turn('turn-1')).toMatchObject({
      userItemId: agentJournalSubmissionKey('send-1'),
      requestedAt: 1_100
    })
  })

  it('gives each queued send to the next turn that opens, in order', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'input.accepted', clientMessageId: 'send-1', requestedAt: 900 })
    rig.assembler.apply({ type: 'input.accepted', clientMessageId: 'send-2', requestedAt: 950 })
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({ type: 'turn.end', at: 1_500, state: 'completed' })
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-2', at: 2_000 })
    expect((await rig.turn('turn-1'))?.userItemId).toBe(agentJournalSubmissionKey('send-1'))
    expect((await rig.turn('turn-2'))?.userItemId).toBe(agentJournalSubmissionKey('send-2'))
  })

  it('ends a still-open turn as superseded when the provider opens a different one', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-2', at: 2_000 })
    expect(await rig.turn('turn-1')).toMatchObject({
      state: 'interrupted',
      outcome: 'superseded',
      completedAt: 2_000
    })
    expect(await rig.turn('turn-2')).toMatchObject({ state: 'running', startedAt: 2_000 })
  })

  it('drops a repeated open, a repeated end, and an end for a turn it never opened', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    expect(rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_100 }).dropped).toBe(
      'turn-duplicate'
    )
    // An open naming no turn while one is open is the same turn, not a new one.
    expect(rig.assembler.apply({ type: 'turn.open', at: 1_200 }).dropped).toBe('turn-duplicate')
    expect(
      rig.assembler.apply({ type: 'turn.end', turnKey: 'turn-9', at: 1_500, state: 'completed' })
        .dropped
    ).toBe('turn-unknown')
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed', outcome: 'success' })
    expect(
      rig.assembler.apply({ type: 'turn.end', turnKey: 'turn-1', at: 3_000, state: 'interrupted' })
        .dropped
    ).toBe('turn-unknown')
    expect(rig.assembler.apply({ type: 'turn.end', at: 3_000, state: 'completed' }).dropped).toBe(
      'no-turn'
    )
    // A replayed open of a settled turn neither reopens nor rewrites it.
    expect(rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 4_000 }).dropped).toBe(
      'turn-duplicate'
    )
    expect(await rig.turn('turn-1')).toMatchObject({
      state: 'completed',
      outcome: 'success',
      startedAt: 1_000,
      completedAt: 2_000
    })
  })

  it('never rewrites a settled turn when a restarted host replays its history', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turnKey: 'turn-1', at: 1_000 })
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed', outcome: 'success' })
    await rig.rows()

    // A new host process has no memory of the turn; only the journal knows it ended.
    const restarted = rig.restart({ generation: 'gen-2' })
    restarted.apply({ type: 'turn.open', turnKey: 'turn-1', at: 5_000 })
    restarted.apply({ type: 'turn.end', at: 6_000, state: 'interrupted', outcome: 'cancellation' })
    expect(await rig.turn('turn-1')).toEqual({
      turnId: 'turn-1',
      state: 'completed',
      outcome: 'success',
      userItemId: providerTurnItemId('turn-1'),
      startedAt: 1_000,
      completedAt: 2_000
    })
  })
})
