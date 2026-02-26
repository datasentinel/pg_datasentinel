CREATE EXTENSION IF NOT EXISTS pg_datasentinel;

-- Verify the autovacuum view is queryable
SELECT count(*) >= 0 AS autovacuum_view_ok FROM ds_autovacuum_activity;

-- Verify the stat activity view is queryable
SELECT count(*) >= 0 AS stat_activity_ok FROM ds_stat_activity;

select ds_autovacuum_activity_reset();

show log_autovacuum_min_duration;
show autovacuum_naptime;

-- Create a table and trigger autovacuum on it.
DROP TABLE IF EXISTS test_av;
CREATE TABLE test_av (id int);
ALTER TABLE test_av SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 0);
INSERT INTO test_av SELECT generate_series(1, 1000);
DELETE FROM test_av;