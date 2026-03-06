-- Test ds_wraparound_risk and ds_xid_snapshots

-- Start clean
SELECT ds_activity_reset_all();

-- Verify views are queryable even when empty
SELECT count(*) >= 0 AS snapshots_view_ok FROM ds_xid_snapshots;

-- Trigger first checkpoint to capture an initial XID snapshot
CHECKPOINT;

-- Give the checkpointer a moment to emit the log message
SELECT pg_sleep(0.3);

-- After first checkpoint we should have exactly 1 snapshot
SELECT count(*) AS snapshot_count FROM ds_xid_snapshots;

-- Verify snapshot fields are populated
SELECT
    count(seq         IS NOT NULL) AS has_seq,
    count(logged_at   IS NOT NULL) AS has_logged_at,
    count(next_xid    IS NOT NULL) AS has_next_xid,
    count(next_mxid   IS NOT NULL) AS has_next_mxid,
    count(oldest_xid_db IS NOT NULL) AS has_oldest_xid_db
FROM ds_xid_snapshots;

-- Verify XID values are positive
SELECT
    bool_and(next_xid  > 0) AS xid_positive,
    bool_and(next_mxid > 0) AS mxid_positive
FROM ds_xid_snapshots;

-- The wraparound risk view always returns exactly one row
SELECT count(*) AS risk_rows FROM ds_wraparound_risk;

-- With 1 snapshot: live distances must be populated, rate/ETA must be NULL
SELECT
    snapshot_count                         = 1  AS one_snapshot,
    oldest_xid_database                   IS NOT NULL AS has_db_name,
    xids_to_aggressive_vacuum             IS NOT NULL AS has_xids_to_aggvac,
    xids_to_wraparound                    IS NOT NULL AS has_xids_to_wrap,
    xids_to_aggressive_vacuum             >  0        AS aggvac_positive,
    xids_to_wraparound                    >  0        AS wrap_positive,
    txid_rate_per_sec                     IS NULL     AS rate_null_when_1snap,
    eta_aggressive_vacuum                 IS NULL     AS eta_aggvac_null,
    eta_wraparound                        IS NULL     AS eta_wrap_null
FROM ds_wraparound_risk;

-- Create and populate a table to generate a few XIDs, then do a second checkpoint
CREATE TABLE wraparound_test (id serial, val text);
INSERT INTO wraparound_test (val) SELECT 'row ' || g FROM generate_series(1, 100) g;
DROP TABLE wraparound_test;

-- Advance time by faking: we cannot advance the clock, so we rely on the
-- throttle being bypassed when the ring buffer has < 1 snapshot in the last hour.
-- The second CHECKPOINT triggers pgds_log_xid_snapshot, but the 1-hour guard will
-- prevent a second entry if it runs within the same second as the first.
-- We check that snapshot_count is >= 1 (already established above).
CHECKPOINT;
SELECT pg_sleep(0.3);

-- snapshot_count may still be 1 due to the 1-hour throttle;
-- verify the view still returns 1 row and live fields remain valid
SELECT
    count(*)                              = 1          AS still_one_row,
    bool_and(snapshot_count              >= 1)         AS at_least_one_snap,
    bool_and(xids_to_aggressive_vacuum   >  0)         AS aggvac_still_positive,
    bool_and(xids_to_wraparound          >  0)         AS wrap_still_positive
FROM ds_wraparound_risk;

-- Cleanup
SELECT ds_activity_reset_all();
SELECT count(*) AS after_reset FROM ds_xid_snapshots;
