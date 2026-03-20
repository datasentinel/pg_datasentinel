DROP EXTENSION IF EXISTS pg_datasentinel;
CREATE EXTENSION pg_datasentinel;

-- Verify the vacuum view is queryable
SELECT count(*) >= 0 AS vacuum_view_ok FROM ds_vacuum_activity;

-- Verify the stat activity view is queryable
SELECT count(*) >= 0 AS stat_activity_ok FROM ds_stat_activity;

-- Verify ds_container_resources is queryable and is_container is always set
SELECT is_container IS NOT NULL AS is_container_not_null FROM ds_container_resources;

select ds_vacuum_activity_reset();

show log_autovacuum_min_duration;
show autovacuum_naptime;

-- Create a table and trigger autovacuum on it.
DROP TABLE IF EXISTS test_av;
CREATE TABLE test_av (id int);
ALTER TABLE test_av SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 0);
INSERT INTO test_av SELECT generate_series(1, 1000);
DELETE FROM test_av;