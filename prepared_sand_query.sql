-- =====================================================================
-- Prepared sand (sand_lab) + production_data join.
-- Produces one row per sand_lab reading, back-filled with the component_id
-- of whichever production_data run was active at that moment on the same
-- Disa line (or on 150T, which applies to any line).
--
-- The leading SET statements run as separate statements by prepared_sand.py
-- so the @vars persist for the main SELECT (same MySQL session).
-- =====================================================================

SET @start_date = CURRENT_DATE - INTERVAL 6 DAY;             -- 7 days ago (inclusive of today)
SET @end_date   = DATE_ADD(@start_date, INTERVAL 7 DAY);     -- end of today (exclusive)

WITH sl AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.entered) AS rid,
         CASE WHEN s.location REGEXP '(^|[^0-9])150 *[tT]' THEN '150T'
              WHEN s.location REGEXP '[dD][iI][sS][aA][^0-9]*[123]([^0-9]|$)'
                   THEN REGEXP_SUBSTR(REGEXP_SUBSTR(s.location, '[dD][iI][sS][aA][^0-9]*[0-9]+'), '[0-9]+')
         END AS line_key
  FROM sand_lab s
  WHERE s.entered >= @start_date AND s.entered < @end_date
),
pd AS (
  SELECT p.*,
         TIMESTAMP(p.production_date, p.start_time) AS start_dt,
         CASE WHEN p.end_time < p.start_time
              THEN TIMESTAMP(DATE_ADD(p.production_date, INTERVAL 1 DAY), p.end_time)
              ELSE TIMESTAMP(p.production_date, p.end_time) END AS end_dt,
         REGEXP_SUBSTR(p.line, '[0-9]+') AS pline_key
  FROM production_data p
  WHERE p.production_date >= DATE_SUB(@start_date, INTERVAL 1 DAY)
    AND p.production_date <  DATE_ADD(@end_date,   INTERVAL 1 DAY)
),
matched AS (
  SELECT sl.*,
         pd.component_id,
         ROW_NUMBER() OVER (PARTITION BY sl.rid ORDER BY pd.start_dt DESC) AS rn
  FROM sl
  LEFT JOIN pd
    ON sl.entered BETWEEN pd.start_dt AND pd.end_dt
   AND (sl.line_key = '150T' OR sl.line_key = pd.pline_key)
  WHERE sl.line_key IS NOT NULL
),
filled AS (
  SELECT m.*,
         COUNT(m.component_id) OVER (PARTITION BY m.line_key ORDER BY m.entered, m.rid) AS grp
  FROM matched m
  WHERE m.rn = 1
)
SELECT
  mb                          AS active_clay,
  NULL                        AS ambient_temp,
  compact                     AS compactibility,
  FIRST_VALUE(component_id) OVER (
      PARTITION BY line_key, grp ORDER BY entered, rid
  )                           AS component_id,
  NULL                        AS cone_jolt_test,
  CAST(DATE(entered) AS DATETIME) AS `date`,
  friability                  AS friability_index,
  gs                          AS gcs,
  gf                          AS gfn_afs,
  NULL                        AS heat_no,
  NULL                        AS inert_fines,
  NULL                        AS loi,
  moist                       AS moisture,
  NULL                        AS oolitics,
  NULL                        AS pH_value,
  perm                        AS permeability,
  NULL                        AS relative_humidity,
  NULL                        AS shatter_index_no,
  NULL                        AS shear_strength,
  CASE WHEN TIME(entered) BETWEEN '04:30:00' AND '17:00:00' THEN '1'
       WHEN TIME(entered) >= '20:00:00' OR TIME(entered) <= '04:30:00' THEN '3'
  END                         AS shift,
  NULL                        AS specimen_weight,
  NULL                        AS split_strength,
  NULL                        AS temp_of_sand_after_mix,
  NULL                        AS temp_of_sand_before_mix,
  TIME(entered)               AS `time`,
  NULL                        AS total_clay,
  NULL                        AS volatile_combustible_matter,
  NULL                        AS volatile_matter,
  NULL                        AS wet_tensile_strength,
  NULL                        AS foundry_line_id,
  NULL                        AS upload_session_id,
  NULL                        AS dry_compression_strength,
  enteredby                   AS quality_inspector,
  location                    AS temp_line,
  NULL                        AS inventory_detail_json
FROM filled
ORDER BY entered
