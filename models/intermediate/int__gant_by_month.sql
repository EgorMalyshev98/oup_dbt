{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code'], 'type': 'hash'}]
    )
}}

{# 
  Разбивка операции по месяцам. 
  Увелечиение даты начала операции рекурсивной функцией на 1 месяц, 
  пока месяц начала операции не будет равен месяцу окночания.
#}

with recursive cte as (
    select
        gant_index,
        code,
        start_date,
        end_date,
        project_type,
        "object",
        "Calen"

    from {{ ref('int__gant_start_transform') }}

    union all

    select
        gant_index,
        code,
        start_date + interval '1 MONTH' as start_date,
        end_date,
        project_type,
        "object",
        "Calen"

    from cte
    where date_trunc('month', start_date + interval '1 MONTH') <= date_trunc('month', end_date)
),


{# расчет даты начала и окончания для каждой части операции #}
cte_1 as (
    select
        gant_index,
        code,
        start_date,
        end_date,
        project_type,
        "object",
        "Calen",

        case

            when start_date = min(start_date) over (partition by code, gant_index) -- первая часть
                then start_date
            else date_trunc('month', start_date)

        end as new_start_date,

        case

            when start_date = min(start_date) over (partition by code, gant_index) -- первая часть
                then least(end_date, date_trunc('month', start_date) + interval '1 month')
            when date_trunc('month', start_date) = date_trunc('month', end_date)
                then end_date
            else date_trunc('month', start_date) + interval '1 month'

        end as new_end_date
    from cte
),
{# вставить cte c календарными исключениями #}
cte_3_ as ( -- noqa: ST03
    select
        cte_1.gant_index,
        cte_1.code,
        project_type,
        "object",
        case
            when extract(month from cte_1.new_start_date) = extract(month from rsce."Start")
                then
                    /*поиск месяца операции в списке исключений*/
                    case
                        when cte_1.new_start_date between
                            (
                                rsce."Start"
                                + (date_trunc('year', cte_1.new_start_date) - date_trunc('year', rsce."Start"))
                            )
                            and
                            (rsce."Fin" + (date_trunc('year', cte_1.new_end_date) - date_trunc('year', rsce."Fin")))
                            then (
                                rsce."Fin_true"
                                + (date_trunc('year', cte_1.new_end_date) - date_trunc('year', rsce."Fin_true"))
                            )
                        /*если дата начала находится между началом и окончанием календарного исключения,
                        * то начало операции равно окончанию исключения*/
                        else cte_1.new_start_date /*в остальных случаях начало не меняется*/
                    end
            else cte_1.new_start_date
        end as start_date,
        case
            when extract(month from cte_1.new_end_date) = extract(month from rsce."Fin")
                then
                    /*поиск месяца операции в списке исключений*/
                    case
                        when (
                            cte_1.new_end_date between
                            (
                                rsce."Start"
                                + (date_trunc('year', cte_1.new_start_date) - date_trunc('year', rsce."Start"))
                            )
                            and
                            (rsce."Fin" + (date_trunc('year', cte_1.new_end_date) - date_trunc('year', rsce."Fin")))
                        )
                        and extract(month from cte_1.new_end_date) = extract(month from cte_1.new_start_date)
                            then (
                                rsce."Fin_true"
                                + (date_trunc('year', cte_1.new_start_date) - date_trunc('year', rsce."Fin_true"))
                            )
                        /* для операций в начале января */
                        when cte_1.new_end_date between
                            (
                                rsce."Start"
                                + (date_trunc('year', cte_1.new_start_date) - date_trunc('year', rsce."Start"))
                            )
                            and
                            (rsce."Fin" + (date_trunc('year', cte_1.new_end_date) - date_trunc('year', rsce."Fin")))
                            then (
                                rsce."Start_true"
                                + (date_trunc('year', cte_1.new_start_date) - date_trunc('year', rsce."Start_true"))
                            )
                        /*если дата окончания находится между началом и окончанием календарного исключения,
                        * то окончание операции равно окончанию исключения */
                        else cte_1.new_end_date /*в остальных случаях окончание не меняется*/
                    end
            else cte_1.new_end_date
        end as end_date

    from cte_1
    left join {{ source('spider', 'raw_spider__calen_except') }} as rsce --raw_spider__calen_except rsce
        on extract(month from cte_1.new_start_date) = extract(month from rsce."Start")
            and cte_1."Calen" = rsce."Calen_Code"
    where
        --не рассматривать исключения календарей SR_10 и SR_20
        coalesce(cte_1."Calen" like 'SR_%' and extract(month from cte_1.new_start_date) in (1, 2, 3, 12), false) is false  -- noqa: LT05
),

cte_2 as (
    select
        gant_index,
        code,
        project_type,
        "object",
        new_start_date as start_date,
        new_end_date as end_date,
        extract(year from new_start_date) as start_year,
        extract(month from new_start_date) as start_month,
        extract(epoch from new_end_date - new_start_date) as duration,
        count(*) over (partition by code, gant_index) as row_oper_count -- кол-во строк, относящихся к одной операции

    from cte_1
),
{# исключить перерывы внутри суток #}
cte_3 as (
    select
        gant_index,
        code,
        start_date,
        end_date,
        project_type,
        "object",
        start_year,
        start_month,
        duration,

        case
            when duration = 0
                then 1
            else duration / sum(duration) over (partition by code, gant_index)
        end as weight

    from cte_2
    where not (duration = 0 and row_oper_count > 1)
)

select
    t.gant_index,
    t.code,
    t.start_year,
    t.start_month,
    'план' as smr_type,
    --доп поля

    r."object",
    weight * r."c_pln_SMRSsI" as smr_ss,
    weight * r."c_pln_SMRSpI" as smr_sp,

    round((weight * (coalesce(r."c_pln_SMRSsI", 0) + coalesce(r."c_pln_SMRSpI", 0)))) as "SMRFull",
    round((
        weight
        * (
            -coalesce(r."c_pln_AmLiz", 0)
            - coalesce(r."c_pln_FnOpTr", 0)
            - coalesce(r."c_pln_FuelMiM", 0)
            - coalesce(r."c_pln_Materl", 0)
            - coalesce(r."c_pln_OplGpd", 0)
            - coalesce(r."c_pln_OpSbRb", 0)
            - coalesce(r."c_pln_PrMatl", 0)
            - coalesce(r."c_pln_ProZtr", 0)
            - coalesce(r."c_pln_St_Mex", 0)
            - coalesce(r."c_pln_StrVzn", 0)
            - coalesce(r."c_pln_UslStH", 0)
            - coalesce(r."c_pln_RepMiM", 0)
            - coalesce(r."c_pln_NkRuch", 0)
        )
    )) as "ZATRATY",
    round((
        weight
        * (
            -coalesce(r."c_pln_AmLiz", 0)
            - coalesce(r."c_pln_FnOpTr", 0)
            - coalesce(r."c_pln_FuelMiM", 0)
            - coalesce(r."c_pln_Materl", 0)
            - coalesce(r."c_pln_OplGpd", 0)
            - coalesce(r."c_pln_OpSbRb", 0)
            - coalesce(r."c_pln_PrMatl", 0)
            - coalesce(r."c_pln_ProZtr", 0)
            - coalesce(r."c_pln_St_Mex", 0)
            - coalesce(r."c_pln_StrVzn", 0)
            - coalesce(r."c_pln_UslStH", 0)
            - coalesce(r."c_pln_RepMiM", 0)
            - coalesce(r."c_pln_NkRuch", 0)
            + coalesce(r."c_pln_SMRSpI", 0)
            + coalesce(r."c_pln_SMRSsI", 0)
            + coalesce(r."c_pln_UslGpd", 0)
        )
    )) as "PRIBYL",
    round((weight * (coalesce(r."c_pln_UslGpd", 0)))) as "UslGpd"

from cte_3 as t

inner join
    {{ source('spider', 'raw_spider__gandoper') }} as r

    {# to do: уникальный индекс в таблице исходных данных #}
    on t.gant_index = r."index" and t.code = r."Code" and t.project_type = r.project_type
