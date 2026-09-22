CREATE TABLE sleep_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    status TEXT NOT NULL,
    high_water_revision INTEGER NOT NULL,
    cursor_before INTEGER NOT NULL,
    cursor_after INTEGER,
    seed_event_ids_json TEXT NOT NULL,
    processed_observation_count INTEGER NOT NULL,
    created_candidate_count INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    error_kind TEXT
);

CREATE TABLE integration_candidates (
    candidate_id TEXT PRIMARY KEY NOT NULL,
    sleep_run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    content TEXT NOT NULL,
    rationale TEXT NOT NULL,
    source_event_ids_json TEXT NOT NULL,
    counterevidence_event_ids_json TEXT NOT NULL,
    confidence INTEGER NOT NULL CHECK (confidence BETWEEN 0 AND 100),
    fingerprint TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE sleep_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cursor INTEGER NOT NULL
);

INSERT OR IGNORE INTO sleep_state (id, cursor) VALUES (1, 0);

CREATE INDEX integration_candidates_status
    ON integration_candidates (status);
CREATE INDEX sleep_runs_status
    ON sleep_runs (status);
