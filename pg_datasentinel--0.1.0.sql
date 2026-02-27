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
    OUT heap_blks_total      int8,
    OUT heap_blks_scanned    int8,
    OUT heap_blks_vacuumed   int8,
    OUT index_vacuum_count   int8,
    OUT max_dead_tuple_bytes int8,
    OUT dead_tuple_bytes     int8,
    OUT num_dead_item_ids    int8,
    OUT indexes_total        int8,
    OUT indexes_processed    int8,
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
    OUT sample_blks_scanned        int8,
    OUT ext_stats_total            int8,
    OUT ext_stats_computed         int8,
    OUT child_tables_total         int8,
    OUT child_tables_done          int8,
    OUT current_child_table_relid  oid,
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