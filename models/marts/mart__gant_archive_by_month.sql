
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


final as (
    SELECT t.*,

        CASE
            WHEN length(CAST(start_month AS TEXT)) = 2 THEN CAST(start_month AS TEXT)
            ELSE CONCAT('0', CAST(t.start_month AS TEXT))::text
        END AS s_month,

        g."Name",
        g."Ispol",
        g."IspolUch", 
        g."Real",
        g."SNT_Knstr",
        g."SNT_KnstrE",
        g."SNT_TypeKnstrE",
        g."SNT_Obj",
        g."Num_Con",

        case 
            when g."SNT_Knstr" = 'Земляное полотно' then 'Земляное полотно'
            when g."SNT_Knstr" = 'Дорожная одежда' then 'Дорожная одежда'
            ELSE 'Прочие работы'

        end as "Конструктив"


    FROM tmp t
        JOIN {{ source('spider', 'raw_spider__gandoper') }} g
        ON t.code = g."Code" and t.object = g.object and g.project_type = 'проект'
)

SELECT * FROM final  