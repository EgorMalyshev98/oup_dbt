with
    tmp as (
        select
            "index" as gant_index,
            "Code" as code,
            "DurPlanD" as dur_plan_d,
            "Start",
            "Fin" as end_date,
            "VolPlan" as vol,
            project_type,
            "object",
            "Calen"

        from {{ source("spider", "raw_spider__gandoper") }}
        where
            project_type = 'проект'
            and "Start" is not null
            and "VolPlan" is not null
            -- Добавлено условие <"Fin" is not null>, чтобы избавиться от операций
            -- связанных с переключателем
            and "Fin" is not null
    ),

    tmp2 as (

        select
            t.*,
            case
                when t.dur_plan_d is null
                then t.end_date
                else t.end_date - interval '1 day' * t.dur_plan_d
            end as start_date

        from tmp as t
    )

select t.*
from tmp2 as t
