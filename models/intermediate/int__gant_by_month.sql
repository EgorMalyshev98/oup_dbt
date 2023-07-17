{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code'], 'type': 'hash'}]
    )
}}



WITH RECURSIVE cte_dates AS (
    SELECT
    gant_index,
    code,
    start_date,
    end_date,
    start_year,
    start_month,
    project_type,
    "object" 
    
  FROM {{ ref('int__gant_start_transform') }}
  
  UNION ALL
  
  SELECT
  	gant_index,
    code,
    start_date + INTERVAL '1 MONTH',
    end_date,
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
    start_year,
    start_month,
    project_type,
    "object",
    
    CASE

		WHEN start_date = MAX(start_date) OVER (PARTITION BY code, gant_index) AND date_trunc('month', start_date) = date_trunc('month', end_date)  
			THEN date_part('day', end_date)
    	WHEN start_date = MIN(start_date) OVER (PARTITION BY code, gant_index) THEN
    		CASE 
    			WHEN date_part('day', (date_trunc('month', start_date) + interval '1 month' - interval '1 day') - date_trunc('day', start_date)) = 0
    			THEN date_part('day', start_date)
    			ELSE date_part('day', (date_trunc('month', start_date) + interval '1 month' - interval '1 day') - date_trunc('day', start_date))
    		END
    	ELSE date_part('day', (date_trunc('month', start_date) + INTERVAL '1 MONTH'  - interval '1 day'))
    END AS num_days -- использовать только для расчета весов
    
FROM cte_dates),


 tmp_2 AS (SELECT
 	  gant_index,
    code,
    start_date,
    end_date,
    project_type,
    "object",
    start_year,
    start_month,
    num_days,
    num_days/sum(num_days) OVER (PARTITION BY code, gant_index) AS weight

FROM tmp_dates)

SELECT
	t.gant_index,
	t.code,
  t.start_year,
  t.start_month,
  'план' as smr_type,
  --доп поля

  r."object",
  
  round((weight * (coalesce(r."c_pln_SMRSsI", 0) + coalesce(r."c_pln_SMRSpI", 0)))) as "SMRFull",
  round((weight * ( - coalesce(r."c_pln_AmLiz", 0) - coalesce(r."c_pln_FnOpTr", 0) - coalesce(r."c_pln_FuelMiM", 0) - coalesce(r."c_pln_Materl", 0) - coalesce(r."c_pln_OplGpd", 0) - coalesce(r."c_pln_OpSbRb", 0) - coalesce(r."c_pln_PrMatl", 0) - coalesce(r."c_pln_ProZtr", 0) - coalesce(r."c_pln_St_Mex", 0) - coalesce(r."c_pln_StrVzn", 0) - coalesce(r."c_pln_UslStH", 0) - coalesce(r."c_pln_RepMiM", 0) - coalesce(r."c_pln_NkRuch", 0)
))) as "ZATRATY",
  round((weight * ( - coalesce(r."c_pln_AmLiz", 0) - coalesce(r."c_pln_FnOpTr", 0) - coalesce(r."c_pln_FuelMiM", 0) - coalesce(r."c_pln_Materl", 0) - coalesce(r."c_pln_OplGpd", 0) - coalesce(r."c_pln_OpSbRb", 0) - coalesce(r."c_pln_PrMatl", 0) - coalesce(r."c_pln_ProZtr", 0) - coalesce(r."c_pln_St_Mex", 0) - coalesce(r."c_pln_StrVzn", 0) - coalesce(r."c_pln_UslStH", 0) - coalesce(r."c_pln_RepMiM", 0) - coalesce(r."c_pln_NkRuch", 0) + coalesce(r."c_pln_SMRSpI", 0) + coalesce(r."c_pln_SMRSsI", 0) + coalesce(r."c_pln_UslGpd", 0)
))) as "PRIBYL"

FROM tmp_2 t
	
JOIN {{source('spider', 'raw_spider__gandoper')}} r 

{# to do: уникальный индекс в таблице исходных данных #}
ON t.gant_index = r."index" AND t.code = r."Code" AND t.project_type = r.project_type
