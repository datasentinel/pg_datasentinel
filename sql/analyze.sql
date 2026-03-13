-- Verify the view is queryable and returns the expected columns.
SELECT
    bool_and(seq          IS NOT NULL) AS has_seq,
    bool_and(logged_at    IS NOT NULL) AS has_logged_at,
    bool_and(datname      IS NOT NULL) AS has_datname,
    bool_and(schemaname   IS NOT NULL) AS has_schemaname,
    bool_and(relname      IS NOT NULL) AS has_relname,
    bool_and(relid        IS NOT NULL) AS has_relid,
    bool_and(is_automatic IS NOT NULL) AS has_is_automatic,
    bool_and(message      IS NOT NULL) AS has_message
FROM ds_analyze_activity where message ~* 'test_av';

-- Autoanalyze entries should be flagged as automatic
SELECT bool_and(is_automatic = true) AS all_automatic
FROM ds_analyze_activity
WHERE message ~* 'test_av';

select ds_analyze_activity_reset();

select count(*) from ds_analyze_activity where message ~* 'test_av';

