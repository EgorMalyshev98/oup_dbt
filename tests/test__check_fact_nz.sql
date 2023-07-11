{# Проверка исходных данных spider: проверка срезов НЗ на наличие СМР в факте #}
/*
* Проверка: фактические значения Стоимость проекта в НЗ
*/

with test as (

select 
    "object",
    nz_year,
    nz_month,
    project_type,
    "status"
from (
    select
        "object",
        nz_year,
        nz_month,
        project_type,
        sum("s_pln_CC_SMRFull"), --СТОИМОСТЬ проекта [План]
        sum("s_act_CC_SMRFull"), --СТОИМОСТЬ проекта [Факт]
        case 
            when sum("s_act_CC_SMRFull") != 0 then 'Фактические значения в НЗ'
            else ''
        end status
    from raw_spider__gandoper g 
    where project_type = 'НЗ'
    group by 
        object,
        nz_year,
        nz_month,
        project_type) as foo
    
)

select 
    "object",
    nz_year,
    nz_month,
    project_type,
    "status"
from test
where status = 'Фактические значения в НЗ'