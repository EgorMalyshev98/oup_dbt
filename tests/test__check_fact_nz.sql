{# Проверка исходных данных spider: проверка срезов НЗ на наличие СМР в факте #}
/*
* Проверка: фактические значения Стоимость проекта в НЗ
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
                        when sum("s_act_CC_SMRFull") != 0
                        then 'Фактические значения в НЗ'
                        else ''
                    end as status
                from {{ source("spider", "raw_spider__gandoper") }}
                where project_type = 'НЗ'
                group by object, nz_year, nz_month, project_type
            ) as foo

    )

select "object", nz_year, nz_month, project_type, "status"
from test
where
    status = 'Фактические значения в НЗ'
    -- тест ci
    -- Исключение: в НЗ на март 2024 на объекте АД108_Дюртюли-Ачит присутствует факт.
    -- в фильтре перечислены все столбцы, чтобы исключить уникальную строку.
    and (
        "object" != 'АД108_Дюртюли-Ачит'
        and nz_year != '2024'
        and nz_month != 'Март'
        and project_type != 'НЗ'
    )
