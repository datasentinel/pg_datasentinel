-- Verify the view is queryable and returns the expected columns.
SELECT
    count(seq IS NOT NULL) AS has_seq,
    count(logged_at     IS NOT NULL) AS has_logged_at,
    count(datname       IS NOT NULL) AS has_datname,
    count(schemaname    IS NOT NULL) AS has_schemaname,
    count(relname       IS NOT NULL) AS has_relname,
    count(relid         IS NOT NULL) AS has_relid,
    count(message       IS NOT NULL) AS has_message
FROM ds_autoanalyze_activity where message ~* 'test_av';

select ds_autoanalyze_activity_reset();

select count(*) from ds_autoanalyze_activity where message ~* 'test_av';

