with
    -- tbl1 - объединение (union all) 2-х таблиц <Техника рабочие> и <Нормативная
    -- трудоемкость>,
    -- которое применяется в витрине <mart__1C>
    tbl1 as (
        -- 1 шаг Ненормируемые работы из таблицы '1_c__technique_workers'
        select ctw.work_id, ctw.analytics_value, ctw.hours as "Фактическая трудоемкость"
        from {{ source('1c', '1_c__technique_workers') }} as ctw
        {# "oup"."1c"."1_c__technique_workers" as ctw #}
        -- фильтр ниже присваивает статус ненормируемая работа всем работам, которых
        -- нет в таблице "1_c__norm_workload"
        where
            -- выбираем ненормируемые работы и ненормативные ресурсы на нормируемых
            -- работах
            not exists (
                select 1
                from "oup"."1c"."1_c__norm_workload" as nwd
                where
                    nwd.work_id = ctw.work_id
                    and nwd.analytics_value = ctw.analytics_value
            )
        union all
        -- 2 шаг Нормируемые работы из таблицы '1_c__norm_workload'
        select
            cnw.work_id,
            cnw.analytics_value,
            cnw.fact_workload as "Фактическая трудоемкость"
        from {{ source('1c', '1_c__norm_workload') }} as cnw
    ),
    {# "oup"."1c"."1_c__norm_workload" as cnw #}
    -- tbl2 - сумма фактической трудоемкости (фактических часов) в витрине <mart__1C>
    tbl2 as (
        select sum(tbl1."Фактическая трудоемкость") as "hours_from_mart" from tbl1
    ),

    -- tbl3 -- сумма фактической трудоемкости (фактических часов) в таблице <Техника
    -- рабочие>
    tbl3 as (
        select sum(ctw.hours) as "hours_from_tchnq_wrkrs"
        from {{ source('1c', '1_c__technique_workers') }} as ctw
    {# oup."1c"."1_c__technique_workers" as ctw  #}
    )

-- Определение разницы между фактическими часами в витрине <mart__1C> и 
-- в таблице <Техника рабочие> 
select
    round(
        (tbl2."hours_from_mart" - tbl3."hours_from_tchnq_wrkrs")
        / tbl3."hours_from_tchnq_wrkrs"
        * 100,
        2
    ) as "delta_hours"
-- delta_hours - отклонение суммы часов в витрине <mart__1C> от суммы таблицы <Техника
-- рабочие> в %.
-- за 100% принята сумма часов из таблицы <Техника рабочие>. 		
from tbl2, tbl3
where
    round(
        (tbl2."hours_from_mart" - tbl3."hours_from_tchnq_wrkrs")
        / tbl3."hours_from_tchnq_wrkrs"
        * 100,
        2
    )
    {# "delta_hours" 0,1% от суммы часов в таблице <Техника рабочие>  принятое значение  #}
    >= 0.1
