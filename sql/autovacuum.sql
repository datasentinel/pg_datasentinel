-- Wait for autovacuum to run.
SELECT pg_sleep(5);

-- Verify the view is queryable and returns the expected columns.
SELECT
    seq           IS NOT NULL AS has_seq,
    logged_at     IS NOT NULL AS has_logged_at,
    operation     IS NOT NULL AS has_operation,
    datname       IS NOT NULL AS has_datname,
    schemaname    IS NOT NULL AS has_schemaname,
    relname       IS NOT NULL AS has_relname,
    relid         IS NOT NULL AS has_relid,
    message       IS NOT NULL AS has_message
FROM ds_autovacuum_activity
ORDER BY seq
;

select count(*) >= 2 AS autovacuum_view_ok from ds_autovacuum_activity;


