{{ config(materialized="table") }}
-- Затраты на операциях из 1С

with
    t1 as (
        -- 1 шаг Ненормируемые работы из таблицы '1_c__technique_workers'
        select
            don.spider_name as "object",
            -- cw.zhufvr_id,
            ctw.work_id,
            cw.type_work_name as "Вид работ",
--            cw.enrp_type as "Тип ЕНРП",
            ctw.res_name as "Ресурс Spider",
            ctw.res_code as "Код ресурса",
            ctw.hours as "Фактическая трудоемкость",
            case
                when ctw.contragent_name = 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО'
                then 'Собственная'::varchar(11)
                when ctw.contragent_name != 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО'
                then 'Наемная'::varchar(11)
                when ctw.analytics_name like '(НТ)%'
                then 'Наемная'::varchar(11)
                when ctw.analytics_name not like '(НТ)%'
                then 'Собственная'::varchar(11)
            end as "Ресурс"

        from {{ source("1c", "1_c__technique_workers") }} as ctw
        inner join {{ source("1c", "1_c__works") }} as cw on ctw.work_id = cw.work_id
        left join {{ source("1c", "1_c__zhufvr") }} as czh on cw.zhufvr_id = czh.link
        left join
            "oup"."public"."dict__1c_objects_to_spider" as don
            on czh.territory_value = don.territory_value
        -- фильтр ниже присваивает статус ненормируемая работа всем работам, которых
        -- нет в таблице "1_c__norm_workload"
        where
        	--выбираем ненормируемые работы и ненормативные ресурсы на нормируемых работах
        	NOT EXISTS (SELECT 1
        				from {{ source("1c", "1_c__norm_workload") }} nwd
        				WHERE nwd.work_id = ctw.work_id AND nwd.analytics_value = ctw.analytics_value
                        -- analytics_value Техника не найдена часы учитываются только в таблице техника рабочие 
        				and ctw.analytics_value not in ('5195e3a3-66ff-11ec-a16c-00224dda35d0', 'e9ddfb4c-5be7-11ec-a16c-00224dda35d0'))
            -- фильтр ниже убирает удаленные и непроведенные ЖУФВР-ы
            and (czh.delete_flag is false and czh.is_done is true)
            and don.spider_name is not null

        union all

        -- 2 шаг Нормируемые работы из таблицы '1_c__norm_workload'
        select
            don.spider_name as "object",
            -- cw.zhufvr_id,                    
            cnw.work_id,
            cw.type_work_name as "Вид работ",
--            cw.enrp_type as "Тип ЕНРП",
            cnw.res_spider_name as "Ресурс Spider",
            cnw.res_spider_code as "Код ресурса",
			case 
				-- отнять разницу часов на основном рабочем на укладке а/б
				when sum(cnw.fact_workload) over (partition by cnw."work_id", cnw."analytics_value") > (ctw1.hours + 1) 
					and row_number () over (partition by cnw."work_id", cnw."analytics_value" order by cnw.res_spider_code desc) = 1 
					and cnw.res_spider_code = 'Mont'
				then cnw.fact_workload - (sum(cnw.fact_workload) over (partition by cnw."work_id", cnw."analytics_value") - ctw1.hours)
				-- присвоить 0 часов для !Техника (НЕ НАЙДЕНА), потому что их часы учтены в вехней части union all 
				when cnw."analytics_value" in ('5195e3a3-66ff-11ec-a16c-00224dda35d0', 'e9ddfb4c-5be7-11ec-a16c-00224dda35d0') then 0 
				else cnw.fact_workload 
			end as "Фактическая трудоемкость", 
            case
                when cnw.nt_res is false
                then 'Собственная'::varchar(11)
                when cnw.nt_res is true
                then 'Наемная'::varchar(11)
            end as "Ресурс"

        from {{ source("1c", "1_c__norm_workload") }} as cnw
        inner join {{ source("1c", "1_c__works") }} as cw on cnw.work_id = cw.work_id
        left join oup."1c"."1_c__technique_workers" as ctw1 
        	on cnw.work_id = ctw1.work_id and 
        	cnw.analytics_value = ctw1.analytics_value
        left join {{ source("1c", "1_c__zhufvr") }} as czh on cw.zhufvr_id = czh.link
        left join
            {{ source("dicts", "dict__1c_objects_to_spider") }} as don
            on czh.territory_value = don.territory_value
        -- фильтр ниже убирает удаленные и непроведенные ЖУФВР-ы
        where
            (czh.delete_flag is false and czh.is_done is true)
            and don.spider_name is not null

    )

select  -- noqa: ST06
--    t1."Вид работ",
--    t1."Ресурс Spider",
    replace(cw.structure_unique_code, ' ', '') as structure_unique_code,
    rsg."Name",

--    czh.zhfvr_date as "Дата",
    round(sum(t1."Фактическая трудоемкость"), 2) as FullWorkLoad_1c,
    round(sum(case 
    	 when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость"
    	 else null
    end) ::numeric, 2) as OwnWorkLoad_1c,
    round(avg (table1."FullWorkLoadFact") :: numeric,  2) as FullWorkLoad_spider,
    round(avg (table1."WorkLoadFact") :: numeric,  2) as OwnWorkLoad_spider,
    round(sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * mim."leasing" 
    	else null 
    end) ::numeric, 2) as leasing_1c,
    round(avg(rsg."c_act_AmLiz") ::numeric, 2) as leasing_spider,
/*    round((sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * mim."leasing" 
    	else null 
    end)  -
    avg(rsg."c_act_AmLiz")) ::numeric, 2) as delta_leasing,*/
    round(sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * mim."repair" 
    	else null 
    end)  ::numeric, 2) as repair_1c,
    round(avg(rsg."c_act_RepMiM") ::numeric, 2) as repair_spider,/*
    round((sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * mim."repair" 
    	else null 
    end)  -
    avg(rsg."c_act_RepMiM")) ::numeric, 2) as delta_repair,*/
    round(sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * mim."fuel" 
    	else null 
    end)  ::numeric, 2) as fuel_1c, 
    round(avg(rsg."c_act_FuelMiM") ::numeric, 2) as fuel_spider,/*
    round((sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * mim."fuel" 
    	else null 
    end)  -
    avg(rsg."c_act_FuelMiM")) ::numeric, 2) as delta_fuel,*/
    round(sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * fot."fot" * coalesce(dske."Kol_Ekip", 1) 
    	else null 
    end)  ::numeric, 2) as fot_1c,  
    round(avg(rsg."c_act_FnOpTr") ::numeric, 2) as fot_spider/*,
    round((sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" * fot."fot" * coalesce(dske."Kol_Ekip", 1) 
    	else null 
    end)  -
    avg(rsg."c_act_FnOpTr")) ::numeric, 2) as delta_fot--,*/
--	затраты
/*    round(sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" 
	    * (coalesce (fot."fot", 0) * coalesce(dske."Kol_Ekip", 1) 
	    + coalesce (mim."leasing", 0) 
	    + coalesce (mim."repair", 0)
	    + coalesce (mim."fuel", 0))
    	else null 
    end)  ::numeric, 2) as zatraty,
    round((coalesce (avg(rsg."c_act_AmLiz"), 0)
    + coalesce (avg(rsg."c_act_RepMiM") , 0) 
    + coalesce (avg(rsg."c_act_FuelMiM") , 0) 
    + coalesce (avg(rsg."c_act_FnOpTr") , 0)) ::numeric, 2) as zatraty_gantt, 
        round(sum(case 
	    when t1."Ресурс" = 'Собственная' then t1."Фактическая трудоемкость" 
	    * (coalesce (fot."fot", 0) * coalesce(dske."Kol_Ekip", 1) 
	    + coalesce (mim."leasing", 0) 
	    + coalesce (mim."repair", 0)
	    + coalesce (mim."fuel", 0))
    	else null 
    end)  ::numeric, 2) - 
    round((coalesce (avg(rsg."c_act_AmLiz"), 0)
    + coalesce (avg(rsg."c_act_RepMiM") , 0) 
    + coalesce (avg(rsg."c_act_FuelMiM") , 0) 
    + coalesce (avg(rsg."c_act_FnOpTr") , 0)) ::numeric, 2) as delta_zatraty
*/

    
from t1
-- соединение с таблицей 1с работы
left join {{ source("1c", "1_c__works") }} as cw on t1.work_id = cw.work_id
-- соединение с таблицей Гантт работ из спайдер
left join
    {{ source("spider", "raw_spider__gandoper") }} as rsg
    on replace(cw.structure_unique_code, ' ', '') = rsg."f_CodeIdent"
    and t1."object" = rsg."object"
-- соединение с таблицей 1с ЖУФВР
left join {{ source("1c", "1_c__zhufvr") }} as czh on cw.zhufvr_id = czh.link 
-- соединение со справочником МиМ и ФОТ
left join 
	{{ source('dicts', 'dict__fot') }} as fot  
	on t1."Ресурс Spider" = fot."res" 
	and (czh.zhfvr_date >= fot."start" and czh.zhfvr_date < fot."finish") 
left join 
	{{ source('dicts', 'dict__mim') }} as mim  
	on t1."Ресурс Spider" = mim."res"
	and (czh.zhfvr_date >= mim."start" and czh.zhfvr_date < mim."finish") 
-- соединение с таблицей spider
left join 	
	(select 
		rsg."f_CodeIdent" as structure_unique_code,
		sum(rsa."WorkLoadFact") as "FullWorkLoadFact",
		sum(case
			when rsa."ResCode" not like 'NT_%' then rsa."WorkLoadFact"
			else 0
		end
		) as "WorkLoadFact",
		sum(coalesce (rsa."c_aac_AmLiz", 0) 
		+ coalesce (rsa."c_aac_RepMiM", 0) 
		+ coalesce (rsa."c_aac_FuelMiM", 0) 
		+ coalesce (rsa."c_aac_FnOpTr", 0)) as zatraty
	from {{ source('spider', 'raw_spider__archive') }} as rsa 
	left join {{ source('spider', 'raw_spider__gandoper') }} as rsg 
	on rsa."OperCode" = rsg."Code" 
	and rsa."object" = rsg."object" 
	and rsa."project_type" = rsg."project_type"
	where rsa."project_type" = 'проект' and rsa."ResCode" is not null  
	group by structure_unique_code) table1 
	on replace(cw.structure_unique_code, ' ', '') = table1.structure_unique_code
-- соединение с таблицей с количесвтом экипажа на ресурсах
left join {{ source('dicts', 'dict__spider_kol_ekip') }} as dske on t1."Код ресурса" = dske."Code" 
-- фильтр ниже для выборки только целых проектов
where rsg.project_type = 'проект' 
	and t1."object" = 'АД089_М-12_км663-км729_С_(8 этап)' 
--	выбор периода
	and rsg."Start" >= '2023-01-01 00:00:00' and rsg."Fin" <= '2024-01-01 00:00:00' 
--	фильтр: турдоемкость операции = сумма трудоемксоти ресурсов 
	and round(table1."zatraty"/50) = round((
										coalesce (rsg."c_act_AmLiz", 0) 
										+ coalesce (rsg."c_act_RepMiM", 0) 
										+ coalesce (rsg."c_act_FuelMiM", 0) 
										+ coalesce (rsg."c_act_FnOpTr", 0))/50)
group by 
    cw.structure_unique_code, 
    rsg."Name"