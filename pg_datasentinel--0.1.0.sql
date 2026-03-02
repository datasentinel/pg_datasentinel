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
    OUT seq                  int4,
    OUT logged_at            timestamptz,
    OUT datname              text,
    OUT schemaname           text,
    OUT relname              text,
    OUT relid                oid,
    OUT heap_pages           int8,
    OUT pages_removed        int8,
    OUT pages_remain         int8,
    OUT pages_scanned        int8,
    OUT tuples_removed       int8,
    OUT tuples_remain        int8,
    OUT user_cpu             float8,
    OUT sys_cpu              float8,
    OUT elapsed              float8,
    OUT message              text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_autovacuum_activity AS
    SELECT * FROM ds_autovacuum_msgs();

CREATE FUNCTION ds_autovacuum_activity_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;


CREATE FUNCTION ds_autoanalyze_msgs(
    OUT seq                        int4,
    OUT logged_at                  timestamptz,
    OUT datname                    text,
    OUT schemaname                 text,
    OUT relname                    text,
    OUT relid                      oid,
    OUT sample_blks_total          int8,
    OUT ext_stats_total            int8,
    OUT child_tables_total         int8,
    OUT user_cpu                   float8,
    OUT sys_cpu                    float8,
    OUT elapsed                    float8,
    OUT message                    text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_autoanalyze_activity AS
    SELECT * FROM ds_autoanalyze_msgs();

CREATE FUNCTION ds_autoanalyze_activity_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;