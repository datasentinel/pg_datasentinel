CREATE FUNCTION ds_stat_pids(
    OUT pid int8,
    OUT memory_bytes int8,
    OUT temp_bytes int8,
    OUT plan_id int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;


CREATE VIEW ds_stat_activity AS
    SELECT s.*, ds.memory_bytes, ds.temp_bytes, ds.plan_id
    FROM pg_stat_activity AS s, ds_stat_pids() AS ds
    WHERE s.pid = ds.pid;


CREATE FUNCTION ds_autovacuum_msgs(
    OUT seq       int4,
    OUT logged_at timestamptz,
    OUT operation text,
    OUT datname   text,
    OUT schemaname text,
    OUT relname    text,
    OUT relid      oid,
    OUT message   text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_autovacuum_activity AS
    SELECT * FROM ds_autovacuum_msgs();