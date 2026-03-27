-- Wait for autovacuum to run.
SELECT pg_sleep(5);

-- Verify the view is queryable and returns the expected columns.
SELECT
    count(seq IS NOT NULL) AS has_seq,
    count(logged_at     IS NOT NULL) AS has_logged_at,
    count(datname       IS NOT NULL) AS has_datname,
    count(schemaname    IS NOT NULL) AS has_schemaname,
    count(relname       IS NOT NULL) AS has_relname,
    count(relid         IS NOT NULL) AS has_relid,
    count(is_aggressive IS NOT NULL) AS has_is_aggressive,
    count(is_automatic     IS NOT NULL) AS has_is_automatic,
    count(message       IS NOT NULL) AS has_message
FROM ds_vacuum_activity where message ~* 'test_av';

-- A normal autovacuum should not be flagged as aggressive
SELECT bool_and(is_aggressive = false) AS not_aggressive
FROM ds_vacuum_activity
WHERE message ~* 'test_av';

-- Autovacuum entries should be flagged as automatic
SELECT bool_and(is_automatic = true) AS all_automatic
FROM ds_vacuum_activity
WHERE message ~* 'test_av';

select ds_vacuum_activity_reset();

select count(*) from ds_vacuum_activity where message ~* 'test_av';

