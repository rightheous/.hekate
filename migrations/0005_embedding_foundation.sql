CREATE TABLE embedding_spaces (
    embedding_space_id TEXT PRIMARY KEY NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    revision TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    normalized INTEGER NOT NULL CHECK (normalized IN (0, 1)),
    query_prefix TEXT NOT NULL,
    document_prefix TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE embedding_records (
    id TEXT PRIMARY KEY NOT NULL,
    embedding_space_id TEXT NOT NULL REFERENCES embedding_spaces(embedding_space_id),
    entity_kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    source_text_hash TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    vector_blob BLOB NOT NULL,
    created_at TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    UNIQUE (embedding_space_id, entity_kind, entity_id, chunk_index, source_text_hash)
);

CREATE INDEX embedding_records_active_space_idx
    ON embedding_records (embedding_space_id, active);

CREATE TABLE embedding_index_state (
    embedding_space_id TEXT PRIMARY KEY NOT NULL REFERENCES embedding_spaces(embedding_space_id),
    last_success_at TEXT,
    last_error_kind TEXT,
    updated_at TEXT NOT NULL
);
