-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_datasentinel_diag" to load this file. \quit

CREATE FUNCTION test_dsdiag_utils()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
