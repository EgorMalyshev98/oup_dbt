{# Проверка исходных данных spider: сравнение СМР в Гантте работ и в стоимостных составляющих #}
-- 4. Новая проверка с делением на 10000
with
    test as (
        select
            "object",
            nz_year,
            nz_month,
            project_type,
            "status",
            "СТОИМОСТЬ проекта [Факт]",
            "Всего [Факт]",
            "СТОИМОСТЬ проекта [План]",
            "Всего [План]"
        from
            (

                -- гантт работ
                select
                    "object",
                    nz_year,
                    nz_month,
                    project_type,
                    "СТОИМОСТЬ проекта [Факт]",
                    "СТОИМОСТЬ проекта [План]",
                    "Всего [Факт]",
                    "Всего [План]",
                    case
                        when
                            round("СТОИМОСТЬ проекта [Факт]" / 10000)
                            = round("Всего [Факт]" / 10000)
                        then ''
                        when
                            round("СТОИМОСТЬ проекта [План]" / 10000)
                            = round("Всего [План]" / 10000)
                        then ''
                        else 'ОШИБКА'
                    end as "status"

                from
                    (
                        select
                            "object",
                            nz_year,
                            nz_month,
                            project_type,
                            concat(
                                "object", "nz_year", "nz_month", "project_type"
                            ) as "link1",
                            -- СТОИМОСТЬ проекта [Факт]
                            sum(coalesce("s_act_CC_SMRSsI", 0)) + sum(
                                coalesce("s_act_CC_SMRSpI", 0)
                            ) as "СТОИМОСТЬ проекта [Факт]",
                            -- СТОИМОСТЬ проекта [План]
                            sum(coalesce("s_pln_CC_SMRSsI", 0)) + sum(
                                coalesce("s_pln_CC_SMRSpI", 0)
                            ) as "СТОИМОСТЬ проекта [План]"

                        from {{ source("spider", "raw_spider__gandoper") }}
                        group by
                            "object",
                            nz_year,
                            nz_month,
                            project_type,
                            concat("object", "nz_year", "nz_month", "project_type")
                    ) as t1

                inner join

                    -- стоимостные составляющие 
                    (
                        select

                            concat(
                                "object", "nz_year", "nz_month", "project_type"
                            ) as "link2",
                            sum("f_act_Total") as "Всего [Факт]",  -- Всего [Факт]
                            sum("f_pln_Total") as "Всего [План]"  -- Всего [План]

                        from {{ source("spider", "raw_spider__cost") }}
                        where "Code" = 'SMRSsI' or "Code" = 'SMRSpI'
                        group by concat("object", "nz_year", "nz_month", "project_type")

                    ) as t2

                    on t1.link1 = t2.link2
            ) as foo
        where "status" = 'ОШИБКА'

    )

select "object", nz_year, nz_month, project_type, "status"
from test
where

    -- Исключаем Аксай из выборки, потому что там присутствует стоимость единицы на
    -- дату в свойствах стоимостных составляющих
    status = 'ОШИБКА' and object != 'АД076_М-4_Дон_км1036-км1072_Аксай_С'
