-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pg_datasentinel" to load this file. \quit

CREATE FUNCTION test_pgds_parse_table_from_message()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_pgds_parse_vacuum_stats()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
