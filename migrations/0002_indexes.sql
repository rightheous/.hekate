CREATE INDEX IF NOT EXISTS events_kind_sequence ON events (kind, sequence);
CREATE INDEX IF NOT EXISTS events_correlation ON events (correlation_id, sequence);
CREATE INDEX IF NOT EXISTS events_subject ON events (subject_kind, subject_id, sequence);
CREATE INDEX IF NOT EXISTS operations_status ON operations (status);
CREATE INDEX IF NOT EXISTS artifacts_hash ON artifacts (content_hash);
