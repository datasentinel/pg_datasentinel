SET client_min_messages = WARNING;

-- Load the parent extension first
CREATE EXTENSION datasentinel_diag;

-- Load test extension
CREATE EXTENSION test_datasentinel_diag_internals;

-- Test parse_table_from_message() from dsdiag_utils.c
SELECT test_dsdiag_utils();

-- Cleanup
DROP EXTENSION test_datasentinel_diag_internals;
DROP EXTENSION datasentinel_diag;
