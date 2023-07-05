WITH tmp as (SELECT
    "index" as gant_index,
    "Code" as code,
    "DurPlanD" as dur_plan_d,
    "Start",
    CASE
        WHEN date_part('day', "Fin") = 1 
            THEN "Fin" - INTERVAL '1 day'
        ELSE "Fin" 
    END AS end_date,
    "VolPlan" AS vol,
    project_type,
    "object" 
    
  FROM {{source('spider', 'raw_spider__gandoper')}} ),


tmp2 AS (

    SELECT
        t.*,
        CASE
            WHEN dur_plan_d is NULL THEN end_date
            ELSE end_date - (INTERVAL '1 day' * round(dur_plan_d))
        END as start_date

FROM tmp t)



SELECT
    t.*,
    EXTRACT(YEAR FROM start_date) AS start_year,
    EXTRACT(MONTH FROM start_date) AS start_month

FROM tmp2 t

