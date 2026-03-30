  
  select * from ds_vacuum_activity_reset();

  select * from ds_vacuum_activity;
  
  DO $$                                                                                                                                                                                                                                                                                                                                                         
  BEGIN                                                                                                                                                                                                                                                                                                                                                       
      RAISE LOG 'automatic vacuum of table "re"';
  END;
  $$;

  select * from ds_vacuum_activity;

  select * from ds_analyze_activity_reset();

  select * from ds_analyze_activity;

  DO $$
  BEGIN
      RAISE LOG 'automatic analyze of table "re"';
  END;
  $$;

  select * from ds_analyze_activity;