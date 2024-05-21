{# 
    Проверка дублирования операций из-за дублирования контрактных номеров в ВДЦ 
 #}
with 
-- план 
	t1 as (
	select 
		pln1."Code", 
--		pln1.smr_full_pln, 
--		mgabm1.smr_full_mart, 
		abs(
			pln1.smr_full_pln - mgabm1.smr_full_mart 
		) as delta 
	from (
		select 
			pln."object", 
			pln."Code", 
			sum(
				coalesce(pln."c_pln_SMRSsI", 0) + coalesce(pln."c_pln_SMRSpI", 0)
				) as smr_full_pln 
		from  {{ source("spider", "raw_spider__gandoper") }} as pln
        -- "oup"."spider"."raw_spider__gandoper" as pln 
		where 
			pln.project_type = 'проект' 
			and (pln."c_pln_SMRSsI" is not null or pln."c_pln_SMRSpI" is not null) 
		group by 
			pln."object", 
			pln."Code"
		) as pln1 -- группировка 
	inner join (
		select 
			mgabm."object",
			mgabm."code",
			sum(
				coalesce(mgabm.smr_sp, 0) 
            	+ coalesce(mgabm.smr_ss, 0)
            	) as smr_full_mart
		from {{ ref("mart__gant_archive_by_month") }} as mgabm 
        -- "oup"."public"."mart__gant_archive_by_month" as mgabm 
		where 
			mgabm.smr_type = 'план' and 
			(mgabm.smr_sp is not null or mgabm.smr_ss is not null)  
		group by 
			mgabm."object",
			mgabm."code"
		) as mgabm1 -- группировка 
	on pln1."Code" = mgabm1."code" and pln1."object" = mgabm1."object" 
	where 
		abs(
			pln1.smr_full_pln - mgabm1.smr_full_mart 
		) > 1 
	), 
-- факт 
	t2 as (
	select 
		fct1."OperCode", 
		fct1.smr_full_fct, 
		mgabm2.smr_full_mart, 
		abs(
			fct1.smr_full_fct - mgabm2.smr_full_mart 
		) as delta 
	from (
		select 
			fct."object", 
			fct."OperCode", 
			sum(
				coalesce(fct."c_aac_SMRSsI", 0) + coalesce(fct."c_aac_SMRSpI", 0)
				) as smr_full_fct 
		from {{ source("spider", "raw_spider__archive") }} as fct 
        -- "oup"."spider"."raw_spider__archive" as fct 
		where 
			fct.project_type = 'проект' 
			and fct."ResCode" is null
			and (fct."c_aac_SMRSsI" is not null or fct."c_aac_SMRSpI" is not null) 
		group by 
			fct."object", 
			fct."OperCode"
		) as fct1 -- группировка 
	inner join (
		select 
			mgabm."object",
			mgabm."code",
			sum(
				coalesce(mgabm.smr_sp, 0) 
            	+ coalesce(mgabm.smr_ss, 0)
            	) as smr_full_mart
		from {{ ref("mart__gant_archive_by_month") }} as mgabm 
        -- "oup"."public"."mart__gant_archive_by_month" as mgabm 
		where 
			mgabm.smr_type = 'факт' and 
			(mgabm.smr_sp is not null or mgabm.smr_ss is not null)  
		group by 
			mgabm."object",
			mgabm."code"
		) as mgabm2 -- группировка 
	on fct1."OperCode" = mgabm2."code" and fct1."object" = mgabm2."object" 
	where 
		abs(
			fct1.smr_full_fct - mgabm2.smr_full_mart 
		) > 1 
	) 
-- итог 
select count(*)
from t1
having count(*) > 0
--
union
--
select count(*)
from t2
having count(*) > 0 
