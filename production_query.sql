-- =====================================================================
-- Pouring runs + mixer averages  (optimized)
-- The leading SET statements run as separate statements by production_booking.py
-- so the @vars persist for the main SELECT (same MySQL session).
-- =====================================================================

SET @lag_min   = 11;                                -- minutes between mixing and pouring
-- SET @from_date = CURRENT_DATE - INTERVAL 1 DAY;     -- yesterday  (2-day rolling
-- SET @to_date   = CURRENT_DATE;                      -- today       window, same as AIW)
-- Option A: everything from a specific start date through today
SET @from_date = '2026-01-01';
SET @to_date   = CURRENT_DATE;

WITH base AS (
    SELECT
        prid, entered, number, qty, type,
        DATE(entered - INTERVAL 270 MINUTE)                  AS prod_date,
        IF(TIME(entered) >= '04:30:00'
           AND TIME(entered) < '17:30:00', 1, 3)             AS shift_no
    FROM mueller_data.pouring_data
    WHERE type IN ('AUTOPOUR-1', 'AUTOPOUR-2', 'AUTOPOUR-3')
      -- only read the dates we need (uses the index on type, entered)
      AND entered >= TIMESTAMP(@from_date) + INTERVAL 270 MINUTE
      AND entered <  TIMESTAMP(@to_date)   + INTERVAL 1 DAY + INTERVAL 270 MINUTE
      -- drop the 17:30–20:00 gap here, before any further work
      AND NOT (TIME(entered) >= '17:30:00' AND TIME(entered) < '20:00:00')
),
flagged AS (
    SELECT *,
        LAG(number)  OVER w AS prev_number,
        LAG(entered) OVER w AS prev_entered
    FROM base
    WINDOW w AS (PARTITION BY type ORDER BY entered, prid)   -- one sort, reused
),
runs AS (
    SELECT *,
        SUM(prev_number IS NULL
            OR prev_number <> number
            OR entered > prev_entered + INTERVAL 600 SECOND)  -- 10 min gap = new run
        OVER (PARTITION BY type ORDER BY entered, prid
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_id
    FROM flagged
),
run_summary AS (
    SELECT type, run_id, prod_date, shift_no, number,
           MIN(entered) AS start_ts,
           MAX(entered) AS end_ts,
           SUM(qty)     AS boxes
    FROM runs
    GROUP BY type, run_id, prod_date, shift_no, number
)
SELECT
    r.prod_date                             AS `Date`,
    r.shift_no                              AS `Shift`,
    r.number                                AS `Component ID`,
    REPLACE(r.type, 'AUTOPOUR-', 'Disa')    AS `Line`,
    TIME(r.start_ts)                        AS `Start Time`,
    TIME(r.end_ts)                          AS `End Time`,
    r.boxes                                 AS `No of Boxes Poured`,
    ROUND(AVG(m.weight_bento_kg), 2)        AS `weight_bento_kg`,
    ROUND(AVG(m.poids_sn), 2)               AS `poids_sn`,
    ROUND(AVG(m.total_water_l), 2)          AS `total_water_l`,
    ROUND(r.boxes * 170.1 / 5000, 0)        AS `No of Batches`
FROM run_summary r
LEFT JOIN mueller_data.mixer_cycle_data m
       -- 1) cheap date filter first: lets MySQL use the (cycle_date, cycle_time) index
       ON  m.cycle_date BETWEEN DATE(r.start_ts - INTERVAL @lag_min MINUTE)
                            AND DATE(r.end_ts   - INTERVAL @lag_min MINUTE)
       -- 2) exact time window (same rule as before)
       AND TIMESTAMP(m.cycle_date, m.cycle_time)
           BETWEEN r.start_ts - INTERVAL @lag_min MINUTE
               AND r.end_ts   - INTERVAL @lag_min MINUTE
GROUP BY r.type, r.run_id, r.prod_date, r.shift_no, r.number,
         r.start_ts, r.end_ts, r.boxes
ORDER BY r.prod_date, r.shift_no, `Line`, r.start_ts
