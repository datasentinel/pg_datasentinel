-- Setup
SELECT ds_analyze_activity_reset();

CREATE TABLE test_mv (id int);
-- INSERT INTO test_mv SELECT generate_series(1, 5);
-- DELETE FROM test_mv;

-- Disable force_verbose so verbosity is controlled explicitly.
SET pg_datasentinel.maintenance_force_verbose = off;

-- Test 2: ANALYZE VERBOSE should be captured in ds_analyze_activity.
ANALYZE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_analyze_activity
WHERE message ~* 'test_mv';

ANALYZE VERBOSE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_analyze_activity
WHERE message ~* 'test_mv';

SELECT ds_analyze_activity_reset();

-- Test 4: With maintenance_force_verbose = on, plain ANALYZE is captured.
SET pg_datasentinel.maintenance_force_verbose = on;

-- Test 2: ANALYZE VERBOSE should be captured in ds_analyze_activity.
ANALYZE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_analyze_activity
WHERE message ~* 'test_mv';

SELECT ds_analyze_activity_reset();

ANALYZE VERBOSE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_analyze_activity
WHERE message ~* 'test_mv';

DROP TABLE test_mv;
