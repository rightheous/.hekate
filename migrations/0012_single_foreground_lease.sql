CREATE TABLE foreground_lease (
    slot INTEGER PRIMARY KEY CHECK (slot = 1),
    owner_id TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL
);

INSERT INTO foreground_lease (slot, owner_id, expires_at_ms)
SELECT 1, owner_id, expires_at_ms
FROM foreground_leases
ORDER BY expires_at_ms DESC
LIMIT 1;

DROP TABLE foreground_leases;
