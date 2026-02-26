CREATE EXTENSION pg_datasentinel;

-- Verify the autovacuum view is queryable
SELECT count(*) >= 0 AS autovacuum_view_ok FROM ds_autovacuum_activity;

-- Verify the stat activity view is queryable
SELECT count(*) >= 0 AS stat_activity_ok FROM ds_stat_activity;

DROP EXTENSION pg_datasentinel;
