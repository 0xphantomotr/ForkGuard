ALTER TABLE deliveries ADD COLUMN payload JSONB;

-- Backfill existing rows if any, though there shouldn't be any in a dev environment
UPDATE deliveries SET payload = '{}' WHERE payload IS NULL;

ALTER TABLE deliveries ALTER COLUMN payload SET NOT NULL;