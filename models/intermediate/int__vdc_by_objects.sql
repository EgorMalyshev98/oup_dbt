{{
  config(
    materialized = 'table')
}}

select --noqa: ST06
    "Наименование работ и затрат",
    "Шифр единичной расценки",
    "СМР П",
    "object",
    "объем",
    "Единицы измерения",
    concat("Шифр единичной расценки", '    ', "Наименование работ и затрат") as "Позиция из КВ",
    concat("Шифр единичной расценки", '    ', "object") as unique_test,
    "Единичная расценка"


from {{ source('excel', 'raw__vdc_by_objects') }}
