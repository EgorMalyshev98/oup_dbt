-- проверка итоговой стоимости контракта по объектам строительства 
select 
	sp."object", -- наименование объекта строительства 
	sp."sum_smr_sp", -- стоимость проекта итог 
	vdc."sum_smr_vdc", -- стоимость проекта из ВДЦ 
	(sp."sum_smr_sp" - vdc."sum_smr_vdc") as "delta", -- разница между стоимостями в рублях 
	round(((sp."sum_smr_sp" / vdc."sum_smr_vdc" - 1) * 100), 3) as "%_delta" -- разница в стоимостях в %-ах 
from ( 
		select 
			rsg."object", 
			sum(
				round(
					(coalesce(rsg."s_act_CC_SMRFull", 0) 
					+ coalesce(rsg."s_pln_CC_SMRFull", 0))::numeric, 
				2)) as sum_smr_sp  
		from {{ source('spider', 'raw_spider__gandoper') }} as rsg -- oup.spider.raw_spider__gandoper rsg 
		where rsg.project_type = 'проект' 
		group by 
			rsg."object" 
	) as sp -- таблица Гантт Работ
left join ( 
		select 
			rvbo."object" , 
			round(sum(rvbo."СМР П")::numeric, 2) as sum_smr_vdc 
		from {{ source('excel', 'raw__vdc_by_objects') }} as rvbo -- oup.public.raw__vdc_by_objects rvbo 
		group by 
			rvbo."object" 
) as vdc -- таблица со всеми ВДЦ 
	on sp."object" = vdc."object" 
where 
    -- разница между стоимостями по модулю выше 10% 
    abs(round(((sp."sum_smr_sp" / vdc."sum_smr_vdc" - 1) * 100), 3)) > 10 