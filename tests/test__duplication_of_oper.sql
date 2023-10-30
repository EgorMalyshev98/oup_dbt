{# 
    Проверка дублирования операций из-за дублирования контрактных номеров в ВДЦ
 #}


with t1 as (	
select 
	"Code",
	coalesce("c_pln_SMRSsI",0) smr_ss_pln,
	coalesce("c_pln_SMRSpI",0) smr_sp_pln,
	coalesce("c_pln_SMRSsI",0) + coalesce("c_pln_SMRSpI",0) as smr_full_pln,
	mgabm.code,
	sum(coalesce(mgabm.smr_ss,0)) ,
	sum(coalesce(mgabm.smr_sp,0)),
	sum(coalesce(mgabm.smr_sp,0)) + sum(coalesce(mgabm.smr_ss,0)) as smr_full_mart
from "oup"."public"."raw_spider__gandoper" pln

inner join oup.public.mart__gant_archive_by_month mgabm on mgabm.code = pln."Code"

where pln.project_type = 'проект' and mgabm.smr_type = 'план' 

group by 
	mgabm.code,
	"Code",
	"c_pln_SMRSsI",
	"c_pln_SMRSpI"
	),

final_pln as (select *,
smr_full_pln - smr_full_mart delta
from t1
where (smr_full_pln != 0 and smr_full_mart != 0) and (smr_full_pln - smr_full_mart !=0)),

fnl as (select *,
abs(delta)
from final_pln
where abs(delta) > 1
order by delta, "Code"),

t1f as (	
select 
	"OperCode",
	coalesce("c_aac_SMRSsI",0) smr_ss_fct,
	coalesce("c_aac_SMRSpI",0) smr_sp_fct,
	coalesce("c_aac_SMRSsI",0) + coalesce("c_aac_SMRSpI",0) as smr_full_fct,
	mgabm.code,
	sum(coalesce(mgabm.smr_ss,0)) ,
	sum(coalesce(mgabm.smr_sp,0)),
	sum(coalesce(mgabm.smr_sp,0)) + sum(coalesce(mgabm.smr_ss,0)) as smr_full_mart
from "oup"."public"."raw_spider__archive" fct

inner join oup.public.mart__gant_archive_by_month mgabm on mgabm.code = fct."OperCode"

where fct.project_type = 'проект' and mgabm.smr_type = 'факт' and fct."ResCode" is not null

group by 
	mgabm.code,
	"OperCode",
	"c_aac_SMRSsI",
	"c_aac_SMRSpI"
	),

final_fct as (select *,
smr_full_fct - smr_full_mart delta
from t1f
where (smr_full_fct != 0 and smr_full_mart != 0) and (smr_full_fct - smr_full_mart !=0)),

fnlf as (select *,
abs(delta)
from final_fct
where abs(delta) > 1
order by delta, "OperCode")

select count(*)
from fnl
having count(*) > 0

union 

select count(*)
from fnlf
having count(*) > 0