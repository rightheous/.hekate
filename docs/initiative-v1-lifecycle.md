# Initiative v1 lifecycle

The Initiative service turns at most one Agenda candidate into a local proposal
per cycle. The selector can choose an unresolved question for an open or
negotiating conflict, a follow-up to an open HEKATE commitment to the user, or
a reconsideration of an active HEKATE position when related newer evidence
exists. `--initiative-once` runs this selection and processes one candidate; it
reports `no_candidate` when the agenda has none.

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

`--chat` shows at most one not-yet-shown Ready proposal after startup and after
each user message. Each proposal is shown at most once during that REPL run;
Dismissed proposals are not shown. The display is local review only: it does not
approve, execute, or send the proposal, and shown IDs are not saved across
restarts.

`--chat` does not start an Initiative worker by default. Pass
`--initiative-worker` with `--chat` to start the bounded worker; it yields while
the foreground lease is active, backs off after errors, and stops when chat
exits. There is no systemd unit or automatic startup.

Initiative v1 does not call the cognitive model, send messages, execute
capabilities, or resume Runs. It does not interpret a historical or user-owned
deletion Goal as permission to act.
