# Invariants

- The event ledger is canonical; projections can be discarded and replayed.
- Event hashes and the current-state snapshot hash must verify before use.
- A commit supplies the expected base revision. A changed head produces a stale-context error and no partial final batch.
- Every evidence reference in a judgment exists in the supplied context.
- HEKATE can only write its own position; user positions belong to the observed user.
- Open conflicts include reasons, reconsideration conditions, and unresolved questions. Resolved conflicts include a resolution.
- Memory candidates are separate from active memories. Promotion is explicit; rejection does not activate recall.
- ResponseProfile is a derived, read-only value: only active principal-scoped explicit-preference Memory with strict `hekate.response_preference.v1` JSON content contributes, and it is recomputed from projected state rather than stored or injected into the model prompt.
- Policy decides whether an intent is allowed and whether approval is required. The model cannot execute an action by asserting that it did so.
- Workspace paths resolve below the configured root. Writes have receipts and verification records.
- External messages with the same channel/message identity are processed once.
