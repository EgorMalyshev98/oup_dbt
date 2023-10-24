{{
  config(
    materialized = 'table')
}}

select 
    "Наименование работ и затрат",
    "Шифр единичной расценки",
    replace("СМР П", ' ', '') as "СМР П",
    "object",
    concat("Шифр единичной расценки",'    ',"Наименование работ и затрат") as "Позиция из КВ",
    concat("Шифр единичной расценки",'    ', "object") unique_test

from {{ source('excel', 'raw__vdc_by_objects') }} rvdc