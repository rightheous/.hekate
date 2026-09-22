ALTER TABLE integration_candidates
    ADD COLUMN disposition TEXT NOT NULL DEFAULT 'needs_validation';

ALTER TABLE integration_candidates
    ADD COLUMN as_of_revision INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sleep_runs
    ADD COLUMN context_budget_report_json TEXT;

UPDATE integration_candidates
SET status = 'needs_validation'
WHERE status = 'pending';

CREATE INDEX integration_candidates_disposition
    ON integration_candidates (disposition);
