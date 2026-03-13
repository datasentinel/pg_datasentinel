-- Setup
SELECT ds_autoanalyze_activity_reset();

CREATE TABLE test_mv (id int);

-- Disable force_verbose so verbosity is controlled explicitly.
SET pg_datasentinel.maintenance_force_verbose = off;                                                                                                                                                                                                                                                                                                        


-- Test 1: only ANALYZE VERBOSE should be captured in ds_autoanalyze_activity.
ANALYZE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_autoanalyze_activity
WHERE message ~* 'test_mv';

ANALYZE VERBOSE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_autoanalyze_activity
WHERE message ~* 'test_mv';

SELECT ds_autoanalyze_activity_reset();


-- Test 2: With maintenance_force_verbose = on, plain ANALYZE is captured.
SET pg_datasentinel.maintenance_force_verbose = on;                                                                                                                                                                                                                                                                                                        


ANALYZE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_autoanalyze_activity
WHERE message ~* 'test_mv';

SELECT ds_autoanalyze_activity_reset();

ANALYZE VERBOSE test_mv;

SELECT datname, schemaname, relname, message AS manual_analyze_captured
FROM ds_autoanalyze_activity
WHERE message ~* 'test_mv';
