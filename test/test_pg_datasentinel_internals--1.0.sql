-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pg_datasentinel" to load this file. \quit

CREATE FUNCTION test_pgds_utils()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
