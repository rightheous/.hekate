# Initiative v1 lifecycle

The Initiative service turns at most one Agenda candidate into a local proposal
per cycle. The v1 Agenda selector is still empty, so `--initiative-once` reports
`no_candidate` until candidate selection is connected.

## Ledger and projection

The event ledger records `initiative_proposed`, followed by
`initiative_readied` in the same revision-checked SQLite transaction. `ready`
means the proposal passed local provenance checks and may be shown for review;
it does not authorize an action or delivery. The configured user can dismiss a
proposal with `initiative_dismissed`. Replay rebuilds the `CurrentState`
projection, and migration 0014 materializes its `initiatives` table.

The service rechecks the selected revision, evidence event integrity, latest
source event, source version, target ownership, fingerprint, and foreground
lease immediately before recording. A changed revision defers the candidate
without writing either lifecycle event. Fingerprints remain reserved after
dismissal, so an unchanged source cannot recreate a dismissed proposal.

Ownership and versions are read from the current projection: observations
belong to their actor; goals, tasks, runs, and working states require a goal
owned by or shared with the target; positions, conflicts, and commitments must
include the target; decisions must address the target; and identity, memory,
relationship, and principal sources must be target scoped. Explicit position,
conflict, and identity versions are used where available. Other supported
sources use the count of ledger events whose subject is that entity.

## Local commands

```text
hekate --initiative-once
hekate --initiatives
hekate --dismiss-initiative INITIATIVE_ID
hekate --chat
```

`--chat` shows one unseen ready proposal in the local terminal after startup or
an interaction. The chat starts a bounded Initiative worker that yields while
the foreground lease is active and backs off after errors. The worker stops
when chat exits. There is no systemd unit or automatic startup.

Initiative v1 does not call the cognitive model, send messages, execute
capabilities, or resume Runs. It does not interpret a historical or user-owned
deletion Goal as permission to act.
