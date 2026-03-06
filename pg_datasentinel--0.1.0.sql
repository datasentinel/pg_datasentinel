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


CREATE FUNCTION ds_tempfile_msgs(
    OUT seq       int4,
    OUT logged_at timestamptz,
    OUT datname   text,
    OUT username  text,
    OUT pid       int4,
    OUT bytes     int8,
    OUT message   text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_tempfile_activity AS
    SELECT * FROM ds_tempfile_msgs();

CREATE FUNCTION ds_tempfile_activity_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;


CREATE FUNCTION ds_container_resource_info(
    OUT cgroup_version    int4,
    OUT cpu_limit         float8,
    OUT mem_limit_bytes   int8
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_container_resources AS
    SELECT cgroup_version,
        cpu_limit,
        mem_limit_bytes
    FROM ds_container_resource_info();


CREATE FUNCTION ds_checkpoint_msgs(
    OUT seq             int4,
    OUT logged_at       timestamptz,
    OUT is_restartpoint bool,
    OUT start_t         timestamptz,
    OUT end_t           timestamptz,
    OUT bufs_written    int4,
    OUT segs_added      int4,
    OUT segs_removed    int4,
    OUT segs_recycled   int4,
    OUT write_time      float8,
    OUT sync_time       float8,
    OUT total_time      float8,
    OUT sync_rels       int4,
    OUT longest_sync    float8,
    OUT average_sync    float8,
    OUT message         text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_checkpoint_activity AS
    SELECT * FROM ds_checkpoint_msgs();

CREATE FUNCTION ds_checkpoint_activity_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION ds_activity_reset_all()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;