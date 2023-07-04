WITH test as (

    SELECT 
        sum("c_aac_SMRSsI") + sum("c_aac_SMRSpI") as raw_total_fact,

    (
        SELECT 
            sum("c_pln_SMRSsI") + sum("c_pln_SMRSpI") 
        FROM {{ source('spider', 'raw_spider__gandoper') }}
        WHERE project_type = 'проект'
        
    )   as raw_total_plan,

    (
        SELECT sum(smr_sp) + sum(smr_ss)
        FROM {{ ref('mart__gant_archive_by_month') }}

    )   as fin_total_smr


    FROM {{ source('spider', 'raw_spider__archive') }}
    WHERE project_type = 'проект'

)

SELECT

    raw_total_plan + raw_total_fact as raw_total,
    fin_total_smr

FROM test

WHERE abs((raw_total_plan + raw_total_fact) - fin_total_smr) > 10000
