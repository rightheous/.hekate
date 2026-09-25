CREATE TABLE initiatives (
    initiative_id TEXT PRIMARY KEY NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('question', 'challenge', 'follow_up')),
    status TEXT NOT NULL CHECK (status IN ('proposed', 'ready', 'dismissed')),
    source_entity_kind TEXT NOT NULL,
    source_entity_id TEXT NOT NULL,
    source_version INTEGER NOT NULL CHECK (source_version >= 0),
    target_principal_id TEXT NOT NULL,
    as_of_revision INTEGER NOT NULL CHECK (as_of_revision >= 0),
    source_event_ids_json TEXT NOT NULL,
    content TEXT NOT NULL,
    rationale TEXT NOT NULL,
    created_at TEXT NOT NULL,
    proposal_json TEXT NOT NULL
);

CREATE INDEX initiatives_target_status ON initiatives (target_principal_id, status);
