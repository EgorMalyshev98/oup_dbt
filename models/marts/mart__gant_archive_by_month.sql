
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
        object,
        smr_type

    FROM {{ ref('int__gant_by_month') }}

        UNION ALL

    SELECT

        a.code,
        a.start_year,
        a.start_month,
        a.smr_ss,
        a.smr_sp,
        a.object,
        a.smr_type

    FROM {{ ref('int__archive_by_month') }} a
    
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
        g."SNT_Obj"

    FROM tmp t
        JOIN {{ source('spider', 'raw_spider__gandoper') }} g
        ON t.code = g."Code" and t.object = g.object and g.project_type = 'проект'
)

SELECT * FROM final


    