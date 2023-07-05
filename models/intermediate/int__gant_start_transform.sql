WITH tmp as (SELECT
    "index" as gant_index,
    "Code" as code,
    "DurPlanD" as dur_plan_d,
    "Start",
    "Fin" as end_date,
    "VolPlan" AS vol,
    project_type,
    "object" 
    
  FROM {{source('spider', 'raw_spider__gandoper')}} 
  WHERE project_type = 'проект' AND "Start" IS NOT NULL AND "VolPlan" IS NOT NULL),


tmp2 AS (

    SELECT
        t.*,
        CASE
        WHEN dur_plan_d IS NULL THEN end_date
        ELSE 
            CASE
            WHEN  date_part('day', (date_trunc('month', end_date) + interval '1 month' - interval '1 day')) >= round(dur_plan_d)
                THEN
                    CASE 
                    WHEN date_part('day', end_date) <= round(dur_plan_d)
                        THEN end_date - (INTERVAL '1 day' * floor(dur_plan_d))
                        ELSE end_date - (INTERVAL '1 day' * round(dur_plan_d))
                    END
                ELSE end_date - (INTERVAL '1 day' * round(dur_plan_d))
            END
        END as start_date

FROM tmp t)



SELECT
    t.*,
    EXTRACT(YEAR FROM start_date) AS start_year,
    EXTRACT(MONTH FROM start_date) AS start_month

FROM tmp2 t

