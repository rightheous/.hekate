CREATE TABLE IF NOT EXISTS completion_criteria (
    criterion_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    description TEXT NOT NULL CHECK (length(trim(description)) > 0),
    normalized_description TEXT NOT NULL,
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    created_at TEXT NOT NULL,
    UNIQUE (criterion_id, task_id),
    UNIQUE (task_id, normalized_description)
);

CREATE TABLE IF NOT EXISTS completion_claims (
    claim_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    criterion_id TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('needs_validation', 'verified', 'rejected')),
    confidence INTEGER NOT NULL CHECK (confidence BETWEEN 0 AND 100),
    evidence_json TEXT NOT NULL,
    blocker TEXT,
    fingerprint TEXT NOT NULL UNIQUE,
    as_of_sequence INTEGER NOT NULL CHECK (as_of_sequence >= 0),
    supersedes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (criterion_id, task_id)
        REFERENCES completion_criteria (criterion_id, task_id)
);

CREATE INDEX IF NOT EXISTS completion_criteria_task
    ON completion_criteria (task_id);
CREATE INDEX IF NOT EXISTS completion_claims_criterion
    ON completion_claims (task_id, criterion_id, as_of_sequence);
