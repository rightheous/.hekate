CREATE TABLE IF NOT EXISTS cognitive_traces (
    trace_id TEXT PRIMARY KEY,
    outcome TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    context_sequence INTEGER NOT NULL,
    context_hash TEXT NOT NULL,
    trace_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS cognitive_traces_context
    ON cognitive_traces (context_sequence, created_at);
