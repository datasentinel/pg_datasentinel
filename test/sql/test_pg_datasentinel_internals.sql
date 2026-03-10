SET client_min_messages = WARNING;

-- Load the parent extension first
CREATE EXTENSION pg_datasentinel;

-- Load test extension
CREATE EXTENSION test_pg_datasentinel_internals;

-- Test pgds_parse_table_from_message() from pgds_utils.c
SELECT test_pgds_parse_table_from_message();

-- Test pgds_parse_vacuum_stats() and match_to_int64() from pgds_utils.c
SELECT test_pgds_parse_vacuum_stats();

-- Test pgds_parse_cpu_stats() and match_to_double() from pgds_utils.c
SELECT test_pgds_parse_cpu_stats();

-- Test pgds_proc.c functions (Linux only; returns SKIPPED on other platforms)
SELECT test_pgds_proc_functions();

-- Test pgds_cgroup.c functions (Linux only; returns SKIPPED on other platforms)
SELECT test_pgds_cgroup_info();

-- Cleanup
DROP EXTENSION test_pg_datasentinel_internals;
DROP EXTENSION pg_datasentinel;
