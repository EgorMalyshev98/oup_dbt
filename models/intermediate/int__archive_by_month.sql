{{ config(materialized="table", indexes=[{"columns": ["code"], "type": "hash"}]) }}

{# 
  Разбивка факта исполнения операции по месяцам. 
  Увелечиение даты начала исполнения рекурсивной функцией на 1 месяц, 
  пока месяц начала операции не будет равен месяцу окночания.
#}
with recursive
    tmp1 as (
        select
            "index" as archive_index,
            "OperCode" as code,
            "Start" as start_date,
            "Fin" as end_date,
            "Vol" as vol,
            extract(year from "Start") as start_year,
            extract(month from "Start") as start_month,
            project_type,
            1 as num_of_parts

        from {{ source("spider", "raw_spider__archive") }}
        where "ResCode" is null and project_type = 'проект'

        union all

        select
            archive_index,
            code,
            start_date + interval '1 MONTH' as start_date,
            end_date,
            vol,
            extract(year from start_date + interval '1 MONTH') as start_year,
            extract(month from start_date + interval '1 MONTH') as start_month,
            project_type,
            num_of_parts + 1 as num_of_parts

        from tmp1

        where
            date_trunc('month', start_date + interval '1 MONTH')
            <= date_trunc('month', end_date)
    ),

    cte_1 as (
        select
            archive_index,
            code,
            start_date,
            end_date,
            project_type,

            case

                when
                    start_date = min(start_date) over (partition by code, archive_index)  -- первая часть операции
                then start_date
                else date_trunc('month', start_date)

            end as new_start_date,

            case

                when
                    start_date = min(start_date) over (partition by code, archive_index)  -- первая часть операции
                then
                    least(
                        end_date, date_trunc('month', start_date) + interval '1 month'
                    )
                when date_trunc('month', start_date) = date_trunc('month', end_date)
                then end_date
                else date_trunc('month', start_date) + interval '1 month'

            end as new_end_date
        from tmp1
    ),

    cte_2 as (
        select
            archive_index,
            code,
            project_type,
            new_start_date as start_date,
            new_end_date as end_date,
            extract(year from new_start_date) as start_year,
            extract(month from new_start_date) as start_month,
            extract(epoch from new_end_date - new_start_date) as duration,
            -- кол-во строк, относящихся к одной операции
            count(*) over (partition by code, archive_index) as row_oper_count

        from cte_1
    ),

    cte_3 as (
        select
            archive_index,
            code,
            start_date,
            end_date,
            project_type,
            start_year,
            start_month,
            duration,

            case
                when duration = 0
                then 1
                else duration / sum(duration) over (partition by code, archive_index)
            end as weight

        from cte_2
        where not (duration = 0 and row_oper_count > 1)
    ),

    {# tmp2 as (
        select
            code,
            start_date,
            end_date,
            vol,
            start_year,
            start_month,
            archive_index,
            project_type,

            case

                when
                    start_date = max(start_date) over (partition by code, archive_index)
                    and date_trunc('month', start_date) = date_trunc('month', end_date)
                then date_part('day', end_date)
                when
                    start_date = min(start_date) over (partition by code, archive_index)
                then
                    case
                        when
                            date_part(
                                'day',
                                (date_trunc('month', start_date) + interval '1 month')
                                - date_trunc('day', start_date)
                            )
                            = 0
                        then 1
                        else
                            date_part(
                                'day',
                                (date_trunc('month', start_date) + interval '1 month')
                                - date_trunc('day', start_date)
                            )
                    end
                else
                    date_part(
                        'day',
                        (
                            date_trunc('month', start_date)
                            + interval '1 MONTH'
                            - date_trunc('month', start_date)
                        )
                    )
            end as num_days  -- использовать только для расчета весов

        from tmp1
    ),

    tmp3 as (
        select
            archive_index,
            code,
            project_type,
            start_year,
            start_month,
            vol,
            num_days,
            num_days / sum(num_days) over (partition by code, archive_index) as weight

        from tmp2
    ), #}
    final as (
        select
            row_number() over () as id,
            t.archive_index,
            t.code,
            t.start_year,
            t.start_month,
            'факт' as smr_type,
            sum(weight * r."c_aac_SMRSsI") as smr_ss,
            sum(weight * r."c_aac_SMRSpI") as smr_sp,

            {# дополнительные поля #}
            r."object",
            {# sum(weight * r."WorkLoadFact") as workload, #}
            round(
                sum(
                    weight
                    * (coalesce(r."c_aac_SMRSsI", 0) + coalesce(r."c_aac_SMRSpI", 0))
                )
            ) as "SMRFull",
            round(
                sum(
                    weight * (
                        - coalesce(r."c_aac_AmLiz", 0)
                        - coalesce(r."c_aac_FnOpTr", 0)
                        - coalesce(r."c_aac_FuelMiM", 0)
                        - coalesce(r."c_aac_Materl", 0)
                        - coalesce(r."c_aac_OplGpd", 0)
                        - coalesce(r."c_aac_OpSbRb", 0)
                        - coalesce(r."c_aac_PrMatl", 0)
                        - coalesce(r."c_aac_ProZtr", 0)
                        - coalesce(r."c_aac_St_Mex", 0)
                        - coalesce(r."c_aac_StrVzn", 0)
                        - coalesce(r."c_aac_UslStH", 0)
                        - coalesce(r."c_aac_RepMiM", 0)
                        - coalesce(r."c_aac_NkRuch", 0)
                    )
                )
            ) as "ZATRATY",
            round(
                sum(
                    weight * (
                        - coalesce(r."c_aac_AmLiz", 0)
                        - coalesce(r."c_aac_FnOpTr", 0)
                        - coalesce(r."c_aac_FuelMiM", 0)
                        - coalesce(r."c_aac_Materl", 0)
                        - coalesce(r."c_aac_OplGpd", 0)
                        - coalesce(r."c_aac_OpSbRb", 0)
                        - coalesce(r."c_aac_PrMatl", 0)
                        - coalesce(r."c_aac_ProZtr", 0)
                        - coalesce(r."c_aac_St_Mex", 0)
                        - coalesce(r."c_aac_StrVzn", 0)
                        - coalesce(r."c_aac_UslStH", 0)
                        - coalesce(r."c_aac_RepMiM", 0)
                        - coalesce(r."c_aac_NkRuch", 0)
                        + coalesce(r."c_aac_SMRSpI", 0)
                        + coalesce(r."c_aac_SMRSsI", 0)
                        + coalesce(r."c_aac_UslGpd", 0)
                    )
                )
            ) as "PRIBYL",
            round(sum(weight * (coalesce(r."c_aac_UslGpd", 0)))) as "UslGpd"

        from cte_3 as t

        inner join
            {{ source("spider", "raw_spider__archive") }} as r

            on t.archive_index = r."index"
            and t.code = r."OperCode"
            and t.project_type = r.project_type

        group by
            {# to do: уникальный индекс в таблице исходных данных #}
            t.archive_index, t.code, t.start_year, t.start_month, r."object"
    )

{# оставляем только ключевые значения #}
select *
from final
where
    ("ZATRATY" is not null or "PRIBYL" is not null or "SMRFull" is not null)
    and code = 'Tv_OX_4.1.2.2.311214917_Art_Art1n1'
