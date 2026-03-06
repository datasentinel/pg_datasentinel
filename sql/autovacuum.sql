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
    count(aggressive    IS NOT NULL) AS has_aggressive,
    count(message       IS NOT NULL) AS has_message
FROM ds_autovacuum_activity where message ~* 'test_av';

-- A normal autovacuum should not be flagged as aggressive
SELECT bool_and(aggressive = false) AS not_aggressive
FROM ds_autovacuum_activity
WHERE message ~* 'test_av';

select ds_autovacuum_activity_reset();

select count(*) from ds_autovacuum_activity where message ~* 'test_av';

