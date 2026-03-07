-- Verify the view is queryable and returns the expected columns.
SELECT
    bool_and(seq        IS NOT NULL) AS has_seq,
    bool_and(logged_at  IS NOT NULL) AS has_logged_at,
    bool_and(datname    IS NOT NULL) AS has_datname,
    bool_and(schemaname IS NOT NULL) AS has_schemaname,
    bool_and(relname    IS NOT NULL) AS has_relname,
    bool_and(relid      IS NOT NULL) AS has_relid,
    bool_and(message    IS NOT NULL) AS has_message
FROM ds_autoanalyze_activity where message ~* 'test_av';

select ds_autoanalyze_activity_reset();

select count(*) from ds_autoanalyze_activity where message ~* 'test_av';

