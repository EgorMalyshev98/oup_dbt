
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
        {# "Name",
        "Ispol",
        "IspolUch", 
        "Real",
        "SNT_Knstr",
        "SNT_KnstrE",
        "SNT_Obj" #}
    FROM {{ ref('int__gant_by_month') }}

        UNION ALL

    SELECT
        code,
        start_year,
        start_month,
        smr_ss,
        smr_sp,
        object,
        smr_type
        
        
    FROM {{ ref('int__archive_by_month') }}
    
    )

SELECT * FROM tmp


    