CREATE TABLE foreground_leases (
    owner_id TEXT PRIMARY KEY,
    expires_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_foreground_leases_expiry ON foreground_leases(expires_at_ms);
