/*
 * Проверка: плановые значения Стоимость проекта в ОВНЗ
 */
with
    test as (

        select "object", nz_year, nz_month, project_type, "status"
        from
            (
                select
                    "object",
                    nz_year,
                    nz_month,
                    project_type,
                    sum("s_pln_CC_SMRFull") as smr_plan,  -- СТОИМОСТЬ проекта [План]
                    sum("s_act_CC_SMRFull") as smr_fact,  -- СТОИМОСТЬ проекта [Факт]
                    case
                        when sum("s_pln_CC_SMRFull") != 0
                        then 'Плановые значения в ОВНЗ'
                        else ''
                    end as "status"
                from {{ source("spider", "raw_spider__gandoper") }}  -- raw_spider__gandoper g 
                where project_type = 'ОВНЗ'
                group by object, nz_year, nz_month, project_type
            ) as foo

    )

select "object", nz_year, nz_month, project_type, "status"
from test
where status = 'Плановые значения в ОВНЗ'
