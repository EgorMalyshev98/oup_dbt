{# Проверка исходных данных spider: сравнение СМР в Гантте работ и в стоимостных составляющих #}
-- 4. Новая проверка с делением на 1000

with test as (
select
    "object",
    nz_year,
    nz_month, 
    project_type,
    "status",
    "СТОИМОСТЬ проекта [Факт]",
    "Всего [Факт]",
    "СТОИМОСТЬ проекта [План]",
    "Всего [План]"
from (

-- гантт работ
    select "object",
        nz_year,
        nz_month,
        project_type,
        "СТОИМОСТЬ проекта [Факт]",
        "СТОИМОСТЬ проекта [План]",
        "Всего [Факт]",
        "Всего [План]",
        case 
        when round("СТОИМОСТЬ проекта [Факт]"/1000) = round("Всего [Факт]"/1000) then ''
        when round("СТОИМОСТЬ проекта [План]"/1000) = round("Всего [План]"/1000) then ''
        else 'ОШИБКА'
    end "status"
	
    from 
        (select
            "object",
            nz_year,
            nz_month,
            project_type,
            concat("object", "nz_year", "nz_month", "project_type") as "link1",
            sum("s_act_CC_SMRSsI") + sum("s_act_CC_SMRSpI") as "СТОИМОСТЬ проекта [Факт]", --СТОИМОСТЬ проекта [Факт]
            sum("s_pln_CC_SMRSsI") + sum("s_pln_CC_SMRSpI") as "СТОИМОСТЬ проекта [План]" --СТОИМОСТЬ проекта [План]
        
        from raw_spider__gandoper g
        group by 
            "object",
            nz_year,
            nz_month,
            project_type,
            concat("object", "nz_year", "nz_month", "project_type")
            ) as t1

 join

 -- стоимостные составляющие 
	(select
	
        concat("object", "nz_year", "nz_month", "project_type") as "link2",
        sum("f_act_Total") as "Всего [Факт]", --Всего [Факт]
        sum("f_pln_Total") as "Всего [План]" --Всего [План]
	
	from raw_spider__cost rsc 
	where "Code" = 'SMRSsI' or "Code" = 'SMRSpI' 
	group by 
	
	    concat("object", "nz_year", "nz_month", "project_type")
	
	) as t2
	
	on t1.link1 = t2.link2) as foo
	where "status" = 'ОШИБКА'
	

)	
select 
    "object",
    nz_year,
    nz_month,
    project_type,
    "status"
from test
where status = 'ОШИБКА'