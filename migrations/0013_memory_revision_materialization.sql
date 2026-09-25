ALTER TABLE integration_candidate_verifications
    ADD COLUMN verification_event_id TEXT REFERENCES events(event_id);

CREATE TABLE memory_revision_materializations (
    candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES integration_candidates(candidate_id),
    verification_event_id TEXT NOT NULL REFERENCES events(event_id),
    target_memory_id TEXT NOT NULL REFERENCES active_memories(memory_id),
    replacement_memory_id TEXT REFERENCES active_memories(memory_id),
    expected_event_id TEXT NOT NULL REFERENCES events(event_id),
    expected_event_hash TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('replace', 'expire')),
    source_event_ids_json TEXT NOT NULL,
    counterevidence_event_ids_json TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    as_of_revision INTEGER NOT NULL CHECK (as_of_revision >= 0),
    created_at TEXT NOT NULL
);

CREATE INDEX memory_revision_materializations_target
    ON memory_revision_materializations (target_memory_id);
