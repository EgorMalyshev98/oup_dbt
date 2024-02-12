WITH tmp as (SELECT
    "index" as gant_index,
    "Code" as code,
    "DurPlanD" as dur_plan_d,
    "Start",
    "Fin" as end_date,
    "VolPlan" AS vol,
    project_type,
    "object",
    "Calen"
    
  FROM {{source('spider', 'raw_spider__gandoper')}} 
  WHERE project_type = 'проект' AND "Start" IS NOT NULL AND "VolPlan" IS NOT NULL and "Fin" is not null),
{# Добавлено условие <"Fin" is not null>, чтобы избавиться от операций связанных с переключателем #}

tmp2 AS (

SELECT
    t.*,
    CASE
    WHEN dur_plan_d IS NULL 
        THEN end_date
        ELSE 
        end_date - INTERVAL '1 day' * dur_plan_d
    END as start_date

FROM tmp t)



SELECT
    t.*
FROM tmp2 t

