# v1 progress

- Persistent SQLite WAL ledger, snapshot, projections, replay, integrity checks, and stale-context commits are implemented.
- Hermes `hekate-qwen` is the default cognitive model through the existing local OpenAI-compatible endpoint.
- Identity, relationship, positions, conflicts, goals, tasks, runs, attempts, working state, memory, action intents, approvals, operations, receipts, verification, and artifacts have typed state and event paths.
- CLI supports interaction, external message deduplication, inspection, recovery/resume, memory candidate decisions, approval decisions, and operation execution.
- Workspace access is limited to read/search and approved writes under the configured root.
