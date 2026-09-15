ALTER TABLE current_state ADD COLUMN integrity_hash TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_candidates (
    memory_candidate_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS active_memories (
    memory_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts (
    receipt_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS verifications (
    verification_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    status TEXT NOT NULL,
    state_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS approvals_status ON approvals (status);
CREATE INDEX IF NOT EXISTS operations_idempotency ON operations (status, operation_id);
CREATE INDEX IF NOT EXISTS memory_candidates_status ON memory_candidates (status);
