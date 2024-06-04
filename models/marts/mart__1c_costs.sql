{{ config(materialized="table") }}
-- Затраты на операциях из 1С
with tbl1 as (
	select 
		ctw.res_name ,
		ctw.res_code ,
		ctw.hours ,
		cw.type_work_name ,
		cw.enrp_type ,
		replace(cw.structure_unique_code, ' ', '') as structure_unique_code, 
		replace(cw.structure_works_code, ' ', '') as structure_works_code, 
		case 
			when ctw.contragent_value = '5eced384-83e4-4caf-b27b-edc76962546b' 
				-- ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО 
				or ctw.analytics_value = '5da5e5d1-6257-11ec-a16c-00224dda35d0' 
				-- Основной рабочий (Mont) 
			then false 
			else true 
		end	as nt_res,
		df.fot, 
		dm.leasing, 
		dm.repair, 
		dm.fuel ,
		dske."Kol_Ekip",
		cz.territory_value,
		cz.zhfvr_date,
		cw.structure_is_delete
	from {{ source('1c', '1_c__technique_workers') }} as ctw 
    --oup."1c"."1_c__technique_workers" ctw 
	left join {{ source('1c', '1_c__works') }} as cw
    --oup."1c"."1_c__works" cw 
	on ctw.work_id = cw.work_id 
	left join {{ source('1c', '1_c__zhufvr') }} as cz
    --oup."1c"."1_c__zhufvr" cz 
	on cw.zhufvr_id = cz.link 
	left join {{ source('dicts', 'dict__fot') }} as df
    --oup.public.dict__fot df 
	on ctw.res_name = df.res 
		and (cz.zhfvr_date >= df."start"
			and cz.zhfvr_date < df.finish)
	left join {{ source('dicts', 'dict__mim') }} as dm
    --oup.public.dict__mim dm 
	on ctw.res_name = dm.res 
		and (cz.zhfvr_date >= dm."start"
			and cz.zhfvr_date < dm.finish)
	left join {{ source('dicts', 'dict__spider_kol_ekip') }} as dske
    --oup.public.dict__spider_kol_ekip dske 
	on ctw.res_code = dske."Code" 
)
, tbl2 as (
select 
	sum(tbl1.hours) as hours,
	sum(case 
		when tbl1.nt_res = false 
		then tbl1.hours  
		else null 
	end) as own_hours , 	
	sum(case 
		when tbl1.nt_res = true 
		then tbl1.hours  
		else null 
	end) as nt_hours , 	
	tbl1.structure_unique_code,
	dcots.spider_name,
	sum(case 
		when tbl1.nt_res = false 
		then tbl1.hours * tbl1.fot * tbl1."Kol_Ekip" 
		else null 
	end) as fot , 	
	sum(case 
		when tbl1.nt_res = false 
		then tbl1.hours * tbl1.leasing 
		else null 
	end) as leasing , 	
	sum(case 
		when tbl1.nt_res = false 
		then tbl1.hours * tbl1.repair 
		else null 
	end) as repair , 
	sum(case 
		when tbl1.nt_res = false 
		then tbl1.hours * tbl1.fuel 
		else null 
	end) as fuel  
from tbl1 
left join {{ source('dicts', 'dict__1c_objects_to_spider') }} as dcots
--oup.public.dict__1c_objects_to_spider dcots 
on tbl1.territory_value = dcots.territory_value
where tbl1.structure_unique_code in (
	select distinct 
		rsg."f_CodeIdent"
	from {{ source('spider', 'raw_spider__gandoper') }} as rsg
    --oup.spider.raw_spider__gandoper rsg 
	where 
		rsg.project_type = 'проект'
		and rsg."object" in ('АД089_М-12_км663-км729_С_(8 этап)')
		and rsg."Start" >= '2022-01-01 00:00:00'
		and rsg."Fin" <= '2024-01-01 00:00:00'
		and rsg."WorkLoadFact" is not null)
	or (
	tbl1.structure_works_code in (
	select distinct 
		regexp_replace(rsg."Code", 'Kzn_', '')
	from {{ source('spider', 'raw_spider__gandoper') }} as rsg
    --oup.spider.raw_spider__gandoper rsg 
	where 
		rsg.project_type = 'проект'
		and rsg."object" in ('АД089_М-12_км663-км729_С_(8 этап)')
		and rsg."Start" >= '2022-01-01 00:00:00'
		and rsg."Fin" <= '2024-01-01 00:00:00'
		and rsg."WorkLoadFact" is not null)
	and tbl1.structure_is_delete = true
	and tbl1.zhfvr_date < '2022-11-01 00:00:00')
group by 
	tbl1.structure_unique_code,
	dcots.spider_name
)
select 
	tbl2.spider_name as "object",
	tbl2.structure_unique_code,
	tbl3."Name",
	tbl2.hours,
	tbl2.own_hours,
	tbl2.nt_hours,
	tbl2.fot as fot_1c,
	tbl2.leasing as leasing_1c,
	tbl2.repair as repair_1c,
	tbl2.fuel as fuel_1c,
    coalesce(tbl2.fot, 0) 
    + coalesce(tbl2.leasing, 0)
    + coalesce(tbl2.repair, 0)
    + coalesce(tbl2.fuel, 0)
    as cost_1c,
	tbl3.wrkld,
	tbl3.nt_wrkld,
	tbl3.own_wrkld,
	tbl3."c_act_AmLiz" as leasing_sp,
	tbl3."c_act_RepMiM" as repair_sp,
	tbl3."c_act_FuelMiM" as fuel_sp,
	tbl3."c_act_FnOpTr" as fot_sp, 
	coalesce(tbl3."c_act_AmLiz", 0)
	+ coalesce(tbl3."c_act_RepMiM", 0)
	+ coalesce(tbl3."c_act_FuelMiM", 0) 
	+ coalesce(tbl3."c_act_FnOpTr", 0) 
    as cost_sp,
    case
        when tbl3."Start" >= '2022-01-01 00:00:00'
            and tbl3."Fin" <= '2023-01-01 00:00:00'
        then '2022'
        when tbl3."Start" >= '2023-01-01 00:00:00'
            and tbl3."Fin" <= '2024-01-01 00:00:00'
        then '2023'
        else 'между 2022 и 2023'
    end as "period"
from tbl2
left join (
select 
	rsa."object", 
	rsg."f_CodeIdent",
	rsg."Name",
	rsa.wrkld,
	rsa.nt_wrkld,
	rsa.own_wrkld,
	rsg."c_act_AmLiz" ,
	rsg."c_act_RepMiM" ,
	rsg."c_act_FuelMiM" ,
	rsg."c_act_FnOpTr",
    rsg."Start",
    rsg."Fin"
from (
	select 
		rsa."OperCode",
		rsa."object",
		sum(rsa."WorkLoadFact") "wrkld",
		sum(
			case
				when rsa."ResCode" like 'NT_%'
				then rsa."WorkLoadFact" 
				else null
			end
			) as nt_wrkld,
		sum(
			case
				when rsa."ResCode" not like 'NT_%'
				then rsa."WorkLoadFact" 
				else null
			end
			) as own_wrkld
	from {{ source('spider', 'raw_spider__archive') }} as rsa
    --oup.spider.raw_spider__archive rsa
	where 
		rsa.project_type = 'проект' 
		and rsa."ResCode" is not null 
	group by 
		rsa."OperCode",
		rsa."object") as rsa 
left join (
	select 
		rsg."f_CodeIdent",
		rsg."Code",
		rsg."Name",
		rsg."c_act_AmLiz" ,
		rsg."c_act_RepMiM" ,
		rsg."c_act_FuelMiM" ,
		rsg."c_act_FnOpTr",
		rsg."object",
        rsg."Start",
        rsg."Fin"
	from {{ source('spider', 'raw_spider__gandoper') }} as rsg
    --oup.spider.raw_spider__gandoper rsg
	where rsg.project_type = 'проект') as rsg
on 
	rsa."OperCode" = rsg."Code" 
	and rsa."object" = rsg."object" 
) as tbl3
on tbl2.structure_unique_code = tbl3."f_CodeIdent" 
	and tbl2.spider_name = tbl3."object"
