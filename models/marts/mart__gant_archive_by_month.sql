
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
        "SMRFull",
        "UslGpd"

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
        t1."SMRFull",
        "UslGpd"
        

    FROM {{ ref('int__archive_by_month') }} t1
    
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

        left join {{ ref('int__vdc_by_objects')}} vdc
        on g."Num_Con" = vdc."Шифр единичной расценки" and g."object" = vdc."object"
)

SELECT * FROM final