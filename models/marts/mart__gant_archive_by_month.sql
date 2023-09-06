
{{
    config(materialized='table')
}}


WITH tmp as (

    SELECT
        code,
        start_year,
        start_month,
        smr_ss,
        smr_sp, 
        "object",
        smr_type,
        "ZATRATY",
        "PRIBYL",
        "SMRFull"

    FROM {{ ref('int__gant_by_month') }}

        UNION ALL

    SELECT
        t1."code",
        t1."start_year",
        t1."start_month",
        t1.smr_ss,
        t1.smr_sp, 
        t1."object",
        t1.smr_type,
        t1."ZATRATY",
        t1."PRIBYL",
        t1."SMRFull"
        

    FROM {{ ref('int__archive_by_month') }} t1
    
    ),

/* 
vdc - ведомость договорной цены объектов. Объединение нескольких контрактных ведомостей
*/
vdc as(
    select 
        "Наименование работ и затрат",
        "Шифр единичной расценки",
        concat("Шифр единичной расценки",'    ',"Наименование работ и затрат") as "Позиция из КВ",
        replace("Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        "object"
    from {{source('excel', 'raw_excel__vdc_ad108')}} vdc_108
    where "Шифр единичной расценки" != ''

/*
условие < != '' > убирает строки без контрактных позиций
*/

    UNION

    select
        vdc_069."Наименование работ и затрат",
        vdc_069."Шифр единичной расценки",
        concat(vdc_069."Шифр единичной расценки",'    ',vdc_069."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_069."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_069."object"
    from {{source('excel', 'raw_excel__vdc_ad069')}} vdc_069
    where vdc_069."Шифр единичной расценки" != ''


    UNION

    select
        vdc_069_1."Наименование работ и затрат",
        vdc_069_1."Шифр единичной расценки",
        concat(vdc_069_1."Шифр единичной расценки",'    ',vdc_069_1."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_069_1."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_069_1."object"
    from {{source('excel', 'raw_excel__vdc_ad069_1')}} vdc_069_1
    where vdc_069_1."Шифр единичной расценки" != ''


    UNION

    select
        vdc_095."Наименование работ и затрат",
        vdc_095."Шифр единичной расценки",
        concat(vdc_095."Шифр единичной расценки",'    ',vdc_095."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_095."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_095."object"
    from {{source('excel', 'raw_excel__vdc_ad095')}} vdc_095
    where vdc_095."Шифр единичной расценки" != ''


    UNION

    select
        vdc_080."Наименование работ и затрат",
        vdc_080."Шифр единичной расценки",
        concat(vdc_080."Шифр единичной расценки",'    ',vdc_080."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_080."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_080."object"
    from {{source('excel', 'raw_excel__vdc_ad080')}} vdc_080
    where vdc_080."Шифр единичной расценки" != ''


    UNION

    select
        vdc_101."Наименование работ и затрат",
        vdc_101."Шифр единичной расценки",
        concat(vdc_101."Шифр единичной расценки",'    ',vdc_101."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_101."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_101."object"
    from {{source('excel', 'raw_excel__vdc_ad101')}} vdc_101
    where vdc_101."Шифр единичной расценки" != ''


    UNION

    select
        vdc_076."Наименование работ и затрат",
        vdc_076."Шифр единичной расценки",
        concat(vdc_076."Шифр единичной расценки",'    ',vdc_076."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_076."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_076."object"
    from {{source('excel', 'raw_excel__vdc_ad076')}} vdc_076
    where vdc_076."Шифр единичной расценки" != ''
      

    UNION

    select
        vdc_089."Наименование работ и затрат",
        vdc_089."Шифр единичной расценки",
        concat(vdc_089."Шифр единичной расценки",'    ',vdc_089."Наименование работ и затрат") as "Позиция из КВ",
        replace(vdc_089."Стоимость работ, руб. без НДС", ' ', '') as "СМР П",
        vdc_089."object"
    from {{source('excel', 'raw_excel__vdc_ad089')}} vdc_089
    where vdc_089."Шифр единичной расценки" != ''
    ),


final as (
    SELECT t.*,

        CASE
            WHEN length(CAST(start_month AS TEXT)) = 2 THEN CAST(start_month AS TEXT)
            ELSE CONCAT('0', CAST(t.start_month AS TEXT))::text
        END AS s_month,

        g."Name",
        g."Ispol",
        g."IspolUch",
/*Временная мера для избавления от недопустимых значений поля Real*/
        case g."Real" 
            when null then null
            when 'Собственные силы' then 'Собственными силами'
            when 'Собственными силами' then 'Собственными силами'
            when 'Субподряд' then 'Силами субподрядных организаций'
            when 'Силами субподрядных организаций' then 'Силами субподрядных организаций'
            else null
        end as "Силы реализации",

        {# g."Real", #}
        g."SNT_Knstr",
        g."SNT_KnstrE",
        g."SNT_TypeKnstrE",
        g."SNT_Obj",
        g."Num_Con",

        {# case
            when t.smr_ss is not null then 'Собственными силами'
            when t.smr_sp is not null then 'Силами субподрядных организаций'
            else ''
        end as "Силы реализации", #}

        case 
            when g."SNT_Knstr" = 'Земляное полотно' then 'Земляное полотно'
            when g."SNT_Knstr" = 'Дорожная одежда' then 'Дорожная одежда'
            ELSE 'Прочие работы'

        end as "Конструктив",

        vdc."Наименование работ и затрат",        
        vdc."Позиция из КВ",
        vdc."СМР П"


    FROM tmp t
        JOIN {{ source('spider', 'raw_spider__gandoper') }} g
        ON t.code = g."Code" and t."object" = g."object" and g.project_type = 'проект'

        left join vdc
        on g."Num_Con" = vdc."Шифр единичной расценки" and g."object" = vdc."object"
)

SELECT * FROM final