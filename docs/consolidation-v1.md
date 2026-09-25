# Consolidation v1

Consolidation coordinates the existing Sleep and MemoryIntegration flow. It
does not keep a second memory store or advance `sleep_cursor` itself.

## Ownership

- `SleepCoordinator` owns `SleepRun`, bounded seed/context construction,
  candidate validation, and the observation cursor transaction.
- `SemanticRecall` and `EmbeddingIndexer` own retrieval and embeddings. Recall
  restores source Event IDs, hashes, and as-of sequence; foreground semantic
  lookup has a 150 ms budget, Sleep has a separate 3 s budget, and deterministic
  local lexical retrieval covers provider failure or timeout. Foreground index
  work runs in a bounded retry task; `--embedding-index-once` rebuilds missing
  vectors from the event ledger after process exit.
- `MemoryIntegration` owns non-model verification, stale-target checks, and
  atomic Memory or HEKATE Position materialization. The legacy plain-text
  Memory candidate still creates a Lesson. A `memory_revision` candidate uses
  `hekate.memory_revision.v1`, binding replace/expire to an active Memory's
  promotion Event ID/hash and candidate as-of revision. Explicit preference
  memories and user Positions are outside Sleep's write authority.
- `Projector` owns all durable projection changes and replay validation.

## Data flow

Observation and result Events remain canonical. A new observation opens one
Sleep run at a fixed high-water revision; Sleep recalls related observations,
active memories, and active HEKATE Positions, then emits provenance-bound
`NeedsValidation` candidates. A non-model principal verifies or rejects a
candidate. Application rechecks evidence, target status/version, and the
candidate's as-of revision, then writes lifecycle and materialization Events in
one expected-revision batch. Later foreground Context is built from the
projected active state; embeddings accelerate retrieval but do not define
whether a Memory or Position is active.

Sleep continues to advance only its existing observation cursor, in the same
batch as its candidates and completion Event. Generated Sleep and integration
Events are not Sleep seeds, so they cannot start an idle review loop.

The new `MemoryRevisionMaterialized` event records the verifier, target and
replacement IDs, source/counterevidence, evidence hashes, fingerprint, and
as-of revision. A new SQLite projection table supports inspection, while the
event ledger and serde-defaulted `CurrentState` field keep existing snapshots
and replay compatible. Replacing links the new Memory to its predecessor;
expiring only changes active status. Neither operation deletes the source
Observation or lifecycle Events.
