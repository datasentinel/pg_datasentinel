-- Create a table and trigger autovacuum on it.
\o /dev/null
DROP TABLE IF EXISTS test_av;
CREATE TABLE test_av (id int);
ALTER TABLE test_av SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 0);
INSERT INTO test_av SELECT generate_series(1, 1000);
DELETE FROM test_av;
-- Wait for autovacuum to run.
SELECT pg_sleep(5);
\o

-- Verify the view is queryable and returns the expected columns.
SELECT
    seq           IS NOT NULL AS has_seq,
    logged_at     IS NOT NULL AS has_logged_at,
    operation     IS NOT NULL AS has_operation,
    datname       IS NOT NULL AS has_datname,
    schemaname    IS NOT NULL AS has_schemaname,
    relname       IS NOT NULL AS has_relname,
    message       IS NOT NULL AS has_message
FROM ds_autovacuum_activity
ORDER BY seq
;

-- Clean up.
DROP TABLE IF EXISTS test_av;
