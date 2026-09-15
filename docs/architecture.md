# HEKATE v1 architecture

The stable path is `CLI/channel observation -> recovery -> focus -> context -> Hermes Thought Cycle -> judgment validation -> policy -> event batch -> SQLite projection`. The model owns interpretation and judgment. HEKATE owns identity, relationship, memory, goals, tasks, runs, attempts, conflicts, approvals, operations, receipts, verification, and recovery.

`events` is the canonical append-only ledger. `current_state` and the entity tables are projections. A final interaction batch appends the decision, position/conflict changes, action intent, approval or operation plan, response, working state, and run transition in one SQLite transaction. The snapshot carries a hash and recovery replays the ledger and compares the complete projection.

The local capability surface is intentionally small: read or search UTF-8 files below the configured workspace root, and write one file below that root only after policy approval. There is no shell, delete, broad filesystem, or network capability.
