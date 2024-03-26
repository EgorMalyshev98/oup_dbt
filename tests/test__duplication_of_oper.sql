{# 
    Проверка дублирования операций из-за дублирования контрактных номеров в ВДЦ
 #}
with
    t1 as (
        select
            pln."Code",  -- noqa
            mgabm.code,  -- noqa
            {# coalesce("c_pln_SMRSsI", 0) as smr_ss_pln, #}
            {# coalesce("c_pln_SMRSpI", 0) as smr_sp_pln, #}
            coalesce("c_pln_SMRSsI", 0) + coalesce("c_pln_SMRSpI", 0) as smr_full_pln,
            {# sum(coalesce(mgabm.smr_ss, 0)),  -- noqa #}
            {# sum(coalesce(mgabm.smr_sp, 0)),  -- noqa #}
            sum(coalesce(mgabm.smr_sp, 0))  -- noqa
            + sum(coalesce(mgabm.smr_ss, 0)) as smr_full_mart
        from {{ source("spider", "raw_spider__gandoper") }} as pln

        inner join
            {{ ref("mart__gant_archive_by_month") }} as mgabm on pln."Code" = mgabm.code

        where pln.project_type = 'проект' and mgabm.smr_type = 'план'

        group by mgabm.code, "Code", "c_pln_SMRSsI", "c_pln_SMRSpI"
    ),

    final_pln as (
        select *, smr_full_pln - smr_full_mart as delta
        from t1
        where
            (smr_full_pln != 0 or smr_full_mart != 0)
            and (smr_full_pln - smr_full_mart != 0)
    ),

    fnl as (
        select *, abs(delta) from final_pln where abs(delta) > 1 order by delta, "Code"  -- noqa
    ),

    t1f as (
        select
        "OperCode",
        {# sum(coalesce("c_aac_SMRSsI", 0)) as smr_ss_fct, #}
        {# sum(coalesce("c_aac_SMRSpI", 0)) as smr_sp_fct, #}
        sum(coalesce("c_aac_SMRSsI", 0)) + sum(coalesce("c_aac_SMRSpI", 0)) as smr_full_fct
        from {{ source("spider", "raw_spider__archive") }} as fct

        where
    fct.project_type = 'проект'
    and fct."ResCode" is null

        group by "OperCode"
    ),

    fnlf as (
select 
		t1f."OperCode",
		mgabm.code,
		{# t1f.smr_ss_fct, #}
		{# t1f.smr_sp_fct, #}
		t1f.smr_full_fct,
        {# sum(coalesce(mgabm.smr_ss, 0)),  -- noqa #}
        {# sum(coalesce(mgabm.smr_sp, 0)),  -- noqa #}
        sum(coalesce(mgabm.smr_sp, 0))  -- noqa
        + sum(coalesce(mgabm.smr_ss, 0)) as smr_full_mart,
        t1f.smr_full_fct - (sum(coalesce(mgabm.smr_sp, 0)) 
       						+ sum(coalesce(mgabm.smr_ss, 0))) 
       						/* smr_full_mart */ as delta
		
		from t1f
		inner join
		"oup"."public"."mart__gant_archive_by_month" as mgabm
        on t1f."OperCode" = mgabm.code 
        where mgabm.smr_type = 'факт' 
        group by 
        t1f."OperCode",
        mgabm.code,
		{# t1f.smr_ss_fct, #}
		{# t1f.smr_sp_fct, #}
		t1f.smr_full_fct
        having ((t1f.smr_full_fct - (sum(coalesce(mgabm.smr_sp, 0)) 
       						+ sum(coalesce(mgabm.smr_ss, 0))))) not between -1 and 1)

select count(*)
from fnl
having count(*) > 0

union

select count(*)
from fnlf
having count(*) > 0
