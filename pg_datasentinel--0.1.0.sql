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
    OUT aggressive           bool,
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

CREATE VIEW ds_activity_summary AS
WITH
    av AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_autovacuum_activity),
    aa AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_autoanalyze_activity),
    cp AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_checkpoint_activity),
    tf AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_tempfile_activity)
SELECT
    av.cnt::int4  AS autovacuum_count,   av.oldest AS autovacuum_oldest,   av.latest AS autovacuum_latest,
    aa.cnt::int4  AS autoanalyze_count,  aa.oldest AS autoanalyze_oldest,  aa.latest AS autoanalyze_latest,
    cp.cnt::int4  AS checkpoint_count,   cp.oldest AS checkpoint_oldest,   cp.latest AS checkpoint_latest,
    tf.cnt::int4  AS tempfile_count,     tf.oldest AS tempfile_oldest,     tf.latest AS tempfile_latest
FROM av, aa, cp, tf;


CREATE FUNCTION ds_xid_snapshot_msgs(
    OUT seq               int4,
    OUT logged_at         timestamptz,
    OUT next_xid          int8,
    OUT next_mxid         int8,
    OUT oldest_xid_db     oid
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_xid_snapshots AS
    SELECT * FROM ds_xid_snapshot_msgs();


CREATE FUNCTION ds_wraparound_risk_info(
    OUT snapshot_count                  int4,
    OUT oldest_snapshot_at              timestamptz,
    OUT newest_snapshot_at              timestamptz,
    OUT current_xid                     int8,
    OUT xids_to_aggressive_vacuum       int8,
    OUT xids_to_wraparound              int8,
    OUT txid_rate_per_sec               float8,
    OUT oldest_xid_database             text,
    OUT eta_aggressive_vacuum           interval,
    OUT eta_wraparound                  interval,
    OUT current_mxid                    int8,
    OUT mxids_to_aggressive_vacuum      int8,
    OUT mxids_to_wraparound             int8,
    OUT mxid_rate_per_sec               float8,
    OUT oldest_mxid_database            text,
    OUT eta_aggressive_vacuum_mxid      interval,
    OUT eta_wraparound_mxid             interval
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_wraparound_risk AS
    SELECT snapshot_count,
        newest_snapshot_at - oldest_snapshot_at AS snapshot_span,
        txid_rate_per_sec,
        current_xid,
        xids_to_aggressive_vacuum,
        xids_to_wraparound,
        mxid_rate_per_sec,
        current_mxid,
        mxids_to_aggressive_vacuum,
        mxids_to_wraparound,
        oldest_xid_database,
        oldest_mxid_database,
        LEAST(eta_aggressive_vacuum, COALESCE(eta_aggressive_vacuum_mxid, eta_aggressive_vacuum)) AS eta_aggressive_vacuum,
        LEAST(eta_wraparound, COALESCE(eta_wraparound_mxid, eta_wraparound)) AS eta_wraparound
    FROM ds_wraparound_risk_info();