SET work_mem = '1MB';
SET log_temp_files = 0;

select ds_tempfile_activity_reset();

\o /dev/null
SELECT max(val)
FROM (
    SELECT md5(random()::text) AS val
    FROM generate_series(1, 300000) ORDER BY val
) t
;

\o

SELECT
    count(seq IS NOT NULL) AS has_seq,
    count(logged_at       IS NOT NULL) AS has_logged_at,
    count(datname         IS NOT NULL) AS has_datname,
    count(username        IS NOT NULL) AS has_username,
    count(pid             IS NOT NULL) AS has_pid,
    count(bytes           IS NOT NULL) AS has_bytes,
    count(message         IS NOT NULL) AS has_message,
    count(bytes > 0)                      AS has_positive_bytes,
    count(message NOT LIKE '%s%' AND
          message NOT LIKE '%ld%')        AS has_concrete_message
FROM ds_tempfile_activity
WHERE datname = current_database()
  AND username = current_user;

select ds_tempfile_activity_reset();

select * from ds_tempfile_activity;
