SELECT
  CASE WHEN cycle_time < '04:30:00'
       THEN DATE_SUB(cycle_date, INTERVAL 0 DAY)
       ELSE cycle_date END                AS `date`,
  cycle_time                              AS `time`,
  CASE WHEN cycle_time >= '04:30:00' AND cycle_time < '17:30:00' THEN 1
       WHEN cycle_time >= '20:00:00' OR  cycle_time <  '04:30:00' THEN 3
       END                                AS shift,
  compact_as_final_measure_pct            AS co_final_percentage,
  compact_preset_pct                      AS cosp_percent,
  sand_input_temp_c                       AS temp_st1c,
  cycle_time_sec                          AS total_seconds,
  total_water_l                           AS total_water,
  readjust_water_l                        AS wd1,
  compact_as1_measure_pct                 AS co1
FROM mixer_cycle_data
ORDER BY cycle_date, cycle_time
