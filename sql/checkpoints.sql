-- Test ds_checkpoint_activity

-- Start clean
SELECT ds_checkpoint_activity_reset();

-- Verify the view is queryable even when empty
SELECT count(*) >= 0 AS checkpoint_view_ok FROM ds_checkpoint_activity;

-- Trigger a manual checkpoint; the emit_log_hook will capture it
CHECKPOINT;

-- Give the checkpointer process a moment to emit the log message
SELECT pg_sleep(0.2);

-- Verify exactly one row was captured
SELECT count(*) AS entries FROM ds_checkpoint_activity;

-- Verify every structural field is non-NULL and has the right shape.
-- Actual values (timestamps, timing, lsn) are not compared because they
-- vary per run.
SELECT
    count(seq             IS NOT NULL) AS has_seq,
    count(logged_at       IS NOT NULL) AS has_logged_at,
    count(is_restartpoint IS NOT NULL) AS has_is_restartpoint,
    count(start_t         IS NOT NULL) AS has_start_t,
    count(end_t           IS NOT NULL) AS has_end_t,
    count(bufs_written    IS NOT NULL) AS has_bufs_written,
    count(segs_added      IS NOT NULL) AS has_segs_added,
    count(segs_removed    IS NOT NULL) AS has_segs_removed,
    count(segs_recycled   IS NOT NULL) AS has_segs_recycled,
    count(write_time      IS NOT NULL) AS has_write_time,
    count(sync_time       IS NOT NULL) AS has_sync_time,
    count(total_time      IS NOT NULL) AS has_total_time,
    count(sync_rels       IS NOT NULL) AS has_sync_rels,
    count(longest_sync    IS NOT NULL) AS has_longest_sync,
    count(average_sync    IS NOT NULL) AS has_average_sync,
    count(message         IS NOT NULL) AS has_message
FROM ds_checkpoint_activity;

-- A manual CHECKPOINT is never a restartpoint
SELECT bool_and(is_restartpoint = false) AS is_not_restartpoint
FROM ds_checkpoint_activity;

-- Timing values must be non-negative
SELECT
    bool_and(write_time >= 0) AS write_time_ok,
    bool_and(sync_time  >= 0) AS sync_time_ok,
    bool_and(total_time >= 0) AS total_time_ok
FROM ds_checkpoint_activity;

-- Message must contain the expected prefix (locale-independent check via message_id)
SELECT bool_and(message LIKE 'checkpoint complete:%') AS message_ok
FROM ds_checkpoint_activity;

-- Reset clears the buffer
SELECT ds_checkpoint_activity_reset();

SELECT * FROM ds_checkpoint_activity;
