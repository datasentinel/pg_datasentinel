SET client_min_messages = WARNING;

-- Load the parent extension first
CREATE EXTENSION pg_datasentinel;

-- Load test extension
CREATE EXTENSION test_pg_datasentinel_internals;

-- Test parse_table_from_message() from pgds_utils.c
SELECT test_pgds_utils();

-- Cleanup
DROP EXTENSION test_pg_datasentinel_internals;
DROP EXTENSION pg_datasentinel;
