-- Setup: extension already created by init.sql
SELECT ds_autovacuum_activity_reset();
SELECT ds_autoanalyze_activity_reset();

SET pg_datasentinel.maintenance_force_verbose = on;


-- Test 1: Default (ignore_system_schemas = on) - pg_catalog entries must be filtered out.
SHOW pg_datasentinel.ignore_system_schemas;

ANALYZE VERBOSE pg_catalog.pg_publication;

SELECT count(*) AS pg_catalog_analyze_ignored
FROM ds_autoanalyze_activity
WHERE schemaname = 'pg_catalog';

SELECT ds_autoanalyze_activity_reset();


-- Test 2: With ignore_system_schemas = off, pg_catalog entries are captured.
SET pg_datasentinel.ignore_system_schemas = off;

ANALYZE VERBOSE pg_catalog.pg_publication;

SELECT count(*) AS pg_catalog_analyze_captured
FROM ds_autoanalyze_activity
WHERE schemaname = 'pg_catalog' AND relname = 'pg_publication';


-- Restore default.
SET pg_datasentinel.ignore_system_schemas = on;
