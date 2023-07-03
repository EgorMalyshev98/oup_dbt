WITH RECURSIVE cte_dates AS (
    SELECT
    "index" as gant_index,
    "Code" AS code,
    "Start" AS start_date,
    "Fin" AS end_date,
    "VolPlan" AS vol,
    EXTRACT(YEAR FROM "Start") AS start_year,
    EXTRACT(MONTH FROM "Start") AS start_month,
    project_type,
    "object" 
    
  FROM {{source('spider', 'raw_spider__gandoper')}} 
  WHERE project_type = 'проект' AND "Start" IS NOT NULL AND "VolPlan" IS NOT NULL 
  
  UNION ALL
  
  SELECT
  	gant_index,
    code,
    start_date + INTERVAL '1 MONTH',
    end_date,
    vol,
    EXTRACT(YEAR FROM start_date + INTERVAL '1 MONTH'),
    EXTRACT(MONTH FROM start_date + INTERVAL '1 MONTH'),
    project_type,
    "object"
   
  FROM cte_dates 
  WHERE date_trunc('month', start_date + INTERVAL '1 MONTH') <= date_trunc('month', end_date)
), 



tmp_dates AS (SELECT
	  gant_index,
    code,
    start_date,
    end_date,
    vol,
    start_year,
    start_month,
    project_type,
    "object",
    
    CASE

		WHEN start_date = MAX(start_date) OVER (PARTITION BY code) AND date_trunc('month', start_date) = date_trunc('month', end_date)  
			THEN date_part('day', end_date)
    	WHEN start_date = MIN(start_date) OVER (PARTITION BY code) THEN
    		CASE 
    			WHEN date_part('day', (date_trunc('month', start_date) + interval '1 month') - date_trunc('day', start_date)) = 0
    			THEN 1
    			ELSE date_part('day', (date_trunc('month', start_date) + interval '1 month') - date_trunc('day', start_date))
    		END
    	ELSE date_part('day', (date_trunc('month', start_date) + INTERVAL '1 MONTH' - date_trunc('month', start_date)))
    END AS num_days -- использовать только для расчета весов
    
FROM cte_dates),


 tmp_2 AS (SELECT
 	  gant_index,
    code,
    start_date,
    end_date,
    vol,
    project_type,
    "object",
    start_year,
    start_month,
    num_days,
    num_days/sum(num_days) OVER (PARTITION BY code) AS weight

FROM tmp_dates)

SELECT
	t.gant_index,
	t.code,
  t.vol,
  t.start_year,
  t.start_month,
  t.weight,
    
  --доп поля
  r."Name", 
  r."c_pln_SMRSsI",
  r."c_pln_SMRSpI",
  r."Ispol",
  r."IspolUch", 
  r."Real",
  r."SNT_Knstr",
  r."SNT_KnstrE",
  r."SNT_Obj"

FROM tmp_2 t
	
JOIN {{source('spider', 'raw_spider__gandoper')}} r 

{# to do: уникальный индекс в таблице исходных данных #}
ON t.gant_index = r."index" AND t.code = r."Code" AND t.project_type = r.project_type
