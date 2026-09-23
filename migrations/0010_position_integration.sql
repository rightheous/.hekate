CREATE TABLE position_integration_materializations (
    candidate_id TEXT PRIMARY KEY NOT NULL REFERENCES integration_candidates(candidate_id),
    position_id TEXT NOT NULL REFERENCES positions(position_id),
    position_event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id),
    action TEXT NOT NULL CHECK (action IN ('establish', 'revise', 'withdraw')),
    prior_position_json TEXT,
    source_event_ids_json TEXT NOT NULL,
    counterevidence_event_ids_json TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    as_of_revision INTEGER NOT NULL CHECK (as_of_revision >= 0),
    created_at TEXT NOT NULL
);

CREATE INDEX position_integration_materializations_position
    ON position_integration_materializations (position_id);
