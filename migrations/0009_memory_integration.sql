CREATE TABLE IF NOT EXISTS integration_candidate_verifications (
    candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES integration_candidates(candidate_id),
    previous_disposition TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('verified', 'rejected')),
    actor_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    evidence_refs_json TEXT NOT NULL,
    as_of_revision INTEGER NOT NULL CHECK (as_of_revision >= 0),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS integration_candidate_materializations (
    candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES integration_candidates(candidate_id),
    memory_candidate_id TEXT NOT NULL REFERENCES memory_candidates(memory_candidate_id),
    memory_id TEXT NOT NULL REFERENCES active_memories(memory_id),
    source_event_ids_json TEXT NOT NULL,
    counterevidence_event_ids_json TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    as_of_revision INTEGER NOT NULL CHECK (as_of_revision >= 0),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS integration_candidate_verifications_disposition
    ON integration_candidate_verifications (disposition);
CREATE INDEX IF NOT EXISTS integration_candidate_materializations_memory
    ON integration_candidate_materializations (memory_id);
