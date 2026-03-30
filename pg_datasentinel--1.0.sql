CREATE FUNCTION ds_stat_pids(
    OUT pid int8,
    OUT rss_memory_bytes int8,
    OUT pss_memory_bytes int8,
    OUT temp_bytes int8,
    OUT plan_id int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;


CREATE VIEW ds_stat_activity AS
    SELECT s.*, ds.rss_memory_bytes, ds.pss_memory_bytes, ds.temp_bytes, ds.plan_id
    FROM pg_stat_activity AS s, ds_stat_pids() AS ds
    WHERE s.pid = ds.pid;


CREATE FUNCTION ds_vacuum_msgs(
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
    OUT is_aggressive        bool,
    OUT is_automatic         bool,
    OUT message              text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_vacuum_activity AS
    SELECT * FROM ds_vacuum_msgs();

CREATE FUNCTION ds_vacuum_activity_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;


CREATE FUNCTION ds_analyze_msgs(
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
    OUT is_automatic               bool,
    OUT message                    text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_analyze_activity AS
    SELECT * FROM ds_analyze_msgs();

CREATE FUNCTION ds_analyze_activity_reset()
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
    OUT cgroup_version       int4,
    OUT cpu_limit            float8,
    OUT mem_limit_bytes      int8,
    OUT cpu_pressure_pct_60s float8,
    OUT mem_used             int8
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

CREATE VIEW ds_container_resources AS
    SELECT cpu_limit,
        cpu_pressure_pct_60s,
        mem_limit_bytes,
        mem_used as mem_used_bytes,
        cgroup_version
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

CREATE FUNCTION ds_xid_snapshots_reset()
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
           FROM ds_vacuum_activity),
    aa AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_analyze_activity),
    cp AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_checkpoint_activity),
    tf AS (SELECT count(*) AS cnt, min(logged_at) AS oldest, max(logged_at) AS latest
           FROM ds_tempfile_activity)
SELECT
    av.cnt::int4  AS vacuum_count,        av.oldest AS vacuum_oldest,        av.latest AS vacuum_latest,
    aa.cnt::int4  AS analyze_count,       aa.oldest AS analyze_oldest,       aa.latest AS analyze_latest,
    cp.cnt::int4  AS checkpoint_count,   cp.oldest AS checkpoint_oldest,   cp.latest AS checkpoint_latest,
    tf.cnt::int4  AS tempfile_count,     tf.oldest AS tempfile_oldest,     tf.latest AS tempfile_latest
FROM av, aa, cp, tf;


CREATE FUNCTION ds_xid_snapshot_msgs(
    OUT seq               int4,
    OUT logged_at         timestamptz,
    OUT next_xid          int8,
    OUT next_mxid         int8,
    OUT oldest_xid_db     oid,
    OUT txid_rate_per_sec float8,
    OUT mxid_rate_per_sec float8
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
with risk as (
    SELECT 
        COALESCE(LEAST(eta_aggressive_vacuum, eta_aggressive_vacuum_mxid), eta_aggressive_vacuum, eta_aggressive_vacuum_mxid) AS eta_aggressive_vacuum,
        COALESCE(LEAST(eta_wraparound, eta_wraparound_mxid), eta_wraparound, eta_wraparound_mxid) AS eta_wraparound,
        CASE
            WHEN xids_to_wraparound < mxids_to_wraparound
                THEN oldest_xid_database
            ELSE oldest_mxid_database
        END AS datname,
        txid_rate_per_sec,
        mxid_rate_per_sec,
        snapshot_count,
        oldest_snapshot_at,
        newest_snapshot_at,
        CASE
            WHEN snapshot_count > 1
                THEN newest_snapshot_at - oldest_snapshot_at
        END AS snapshot_interval,
        xids_to_aggressive_vacuum,
        xids_to_wraparound,
        mxids_to_aggressive_vacuum,
        mxids_to_wraparound
    FROM ds_wraparound_risk_info()
)
select
    CASE
        WHEN eta_aggressive_vacuum IS NULL THEN NULL
        else
        CASE WHEN extract(day from eta_aggressive_vacuum) > 0 THEN extract(day from eta_aggressive_vacuum)::int || 'd ' else '' end ||
            EXTRACT(hour FROM eta_aggressive_vacuum)::int || 'h ' ||
            EXTRACT(minute FROM eta_aggressive_vacuum)::int || 'm ' ||
            FLOOR(EXTRACT(second FROM eta_aggressive_vacuum))::int || 's'
    END AS eta_aggressive_vacuum_fmt,
    CASE
        WHEN eta_wraparound IS NULL THEN NULL
        else
        CASE WHEN extract(day from eta_wraparound) > 0 THEN extract(day from eta_wraparound)::int || 'd ' else '' end ||
            EXTRACT(hour FROM eta_wraparound)::int || 'h ' ||
            EXTRACT(minute FROM eta_wraparound)::int || 'm ' ||
            FLOOR(EXTRACT(second FROM eta_wraparound))::int || 's'
    END AS eta_wraparound_fmt,
    datname,
    txid_rate_per_sec,
    mxid_rate_per_sec,
    snapshot_count,
    oldest_snapshot_at,
    newest_snapshot_at,
    CASE
        WHEN snapshot_interval IS NULL THEN NULL
        else        
        CASE WHEN extract(day from snapshot_interval) > 0 THEN extract(day from snapshot_interval)::int || 'd ' else '' end ||
            EXTRACT(hour FROM snapshot_interval)::int || 'h ' ||
            EXTRACT(minute FROM snapshot_interval)::int || 'm ' ||
            FLOOR(EXTRACT(second FROM snapshot_interval))::int || 's'
    END AS snapshot_interval,
    xids_to_aggressive_vacuum,
    xids_to_wraparound,
    mxids_to_aggressive_vacuum,
    mxids_to_wraparound,
    eta_aggressive_vacuum,
    eta_wraparound
FROM risk;

DO $$
BEGIN
    CREATE ROLE ds_reader;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END
$$;

REVOKE EXECUTE ON FUNCTION
    ds_stat_pids(),
    ds_vacuum_msgs(),
    ds_vacuum_activity_reset(),
    ds_analyze_msgs(),
    ds_analyze_activity_reset(),
    ds_tempfile_msgs(),
    ds_tempfile_activity_reset(),
    ds_container_resource_info(),
    ds_checkpoint_msgs(),
    ds_checkpoint_activity_reset(),
    ds_xid_snapshots_reset(),
    ds_activity_reset_all(),
    ds_xid_snapshot_msgs(),
    ds_wraparound_risk_info()
FROM PUBLIC;

GRANT EXECUTE ON FUNCTION
    ds_stat_pids(),
    ds_vacuum_msgs(),
    ds_analyze_msgs(),
    ds_tempfile_msgs(),
    ds_container_resource_info(),
    ds_checkpoint_msgs(),
    ds_xid_snapshot_msgs(),
    ds_wraparound_risk_info()
TO ds_reader;

GRANT SELECT ON
    ds_stat_activity,
    ds_vacuum_activity,
    ds_analyze_activity,
    ds_tempfile_activity,
    ds_container_resources,
    ds_checkpoint_activity,
    ds_activity_summary,
    ds_xid_snapshots,
    ds_wraparound_risk
TO ds_reader;