with test as (

    select
        sum("c_aac_SMRSsI") + sum("c_aac_SMRSpI") as raw_total_fact,

        (
            select sum("c_pln_SMRSsI") + sum("c_pln_SMRSpI")
            from {{ source('spider', 'raw_spider__gandoper') }}
            where project_type = 'проект'

        ) as raw_total_plan,

        (
            select sum(smr_sp) + sum(smr_ss)
            from {{ ref('mart__gant_archive_by_month') }}

        ) as fin_total_smr


    from {{ source('spider', 'raw_spider__archive') }}
    where project_type = 'проект'

)

select

    raw_total_plan + raw_total_fact as raw_total,
    fin_total_smr

from test

where abs((raw_total_plan + raw_total_fact) - fin_total_smr) > 10000
