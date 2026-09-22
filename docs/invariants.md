# Invariants

- The event ledger is canonical; projections can be discarded and replayed.
- Event hashes and the current-state snapshot hash must verify before use.
- A commit supplies the expected base revision. A changed head produces a stale-context error and no partial final batch.
- Every evidence reference in a judgment exists in the supplied context.
- HEKATE can only write its own position; user positions belong to the observed user.
- Open conflicts include reasons, reconsideration conditions, and unresolved questions. Resolved conflicts include a resolution.
- Memory candidates are separate from active memories. Promotion is explicit; rejection does not activate recall.
- Policy decides whether an intent is allowed and whether approval is required. The model cannot execute an action by asserting that it did so.
- Workspace paths resolve below the configured root. Writes have receipts and verification records.
- External messages with the same channel/message identity are processed once.
- Sleep advances its observation cursor only in the same transaction as candidate and completion events; failed or interrupted runs leave it unchanged.
- Sleep source and counterevidence IDs must be present in the bounded sleep context, and sleep candidates never project into Memory, Position, Identity, Conflict, Relationship, or Goal state.
- Completion claims start at `needs_validation`; only an explicit non-model actor can move them to `verified` or `rejected`.
- Verified completion claims require ledger/artifact provenance, a valid as-of sequence, and no blocker. Confidence never grants verification.
