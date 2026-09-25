# Initiative v1 contract

Initiative v1 defines a typed, provenance-bound proposal contract. The Agenda
selector returns evidence-backed questions for open or negotiating conflicts,
follow-ups for HEKATE's open commitments to the user, and candidates to
reconsider an active HEKATE position when related observations arrive after it.
Proposals are local review items; none is delivered or authorized to invoke a
capability.

## Types and fields

- `InitiativeKind` identifies a candidate as `Question`, `Challenge`, or
  `FollowUp`.
- `AgendaCandidate` is the reviewable candidate returned by agenda selection.
- `InitiativeProposal` preserves its ID, content, rationale, source Event IDs,
  status, and creation time. It adds the candidate kind, source entity and
  version, target principal, as-of revision, and fingerprint so a proposal
  retains the provenance used to create it.

| Field | Meaning |
|---|---|
| `id` | Stable identifier for a persisted proposal. |
| `kind` | Question, challenge, or follow-up represented by the candidate. |
| `content` | Proposed text shown to the target principal. |
| `rationale` | Why the evidence supports raising this initiative. |
| `source_entity` | Entity kind and ID that the initiative concerns. |
| `source_version` | Version of that entity used to form the candidate. |
| `source_event_ids` | Event IDs supporting the candidate; ordering and duplicates do not affect its fingerprint. |
| `target_principal_id` | Principal the candidate is directed to. |
| `as_of_revision` | Projection revision used during selection; excluded from the fingerprint. |
| `fingerprint` | Deterministic deduplication key for the semantic source and target. |
| `status` | Current initiative lifecycle state. |
| `created_at` | Proposal creation timestamp; excluded from the fingerprint. |

## Statuses and fingerprint

The existing `InitiativeStatus` values remain available. In v1 the only
transitions are `Proposed` to `Ready` or `Dismissed`. `AwaitingApproval` and
`Delivered` remain reserved for a future external-delivery workflow.

`initiative_fingerprint` hashes a versioned canonical tuple containing the
initiative kind, source `EntityRef` (entity kind and ID), source version,
target principal ID, and sorted unique evidence Event IDs. It excludes the
ledger revision, creation time, content, and rationale so unrelated changes do
not invalidate deduplication.

## File ownership

- `src/core/initiative.rs` owns the serialized types and fingerprint function.
- `src/core/mod.rs` exposes the core contract.
- `src/runtime/initiative/agenda.rs` selects and orders candidates from the
  verified ledger and current projection.
- `src/runtime/initiative/service.rs` validates candidate provenance and
  target scope, then records proposals and dismissals in the event ledger.
- `src/runtime/initiative/worker.rs` runs bounded periodic cycles, deferring
  while a foreground lease is active and backing off after errors.
- `src/runtime/initiative/mod.rs` connects those modules and re-exports the
  shared types.

## Non-goals

This contract does not change EventKind, CurrentState, projection or replay,
database migrations, model prompts, external delivery, or capability
execution. Provenance checks and fingerprints support local deduplication;
they do not approve or send proposals.
