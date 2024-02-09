{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code'], 'type': 'hash'}]
    )
}}

{# 
  Разбивка операции по месяцам. 
  Увелечиение даты начала операции рекурсивной функцией на 1 месяц, 
  пока месяц начала операции не будет равен месяцу окночания.
#}

WITH RECURSIVE cte AS (
    SELECT
    gant_index,
    code,
    start_date,
    end_date,
    project_type,
    "object" 
    
  FROM {{ ref('int__gant_start_transform') }}
  
  UNION ALL
  
  SELECT
  	gant_index,
    code,
    start_date + INTERVAL '1 MONTH',
    end_date,
    project_type,
    "object"
   
  FROM cte 
  WHERE date_trunc('month', start_date + INTERVAL '1 MONTH') <= date_trunc('month', end_date)
), 


{# расчет даты начала и окончания для каждой части операции #}
cte_1 AS (SELECT
	  gant_index,
    code,
    start_date,
    end_date,
    project_type,
    "object",
    
    CASE

      WHEN start_date = min(start_date) OVER (PARTITION BY code, gant_index) -- первая часть
      THEN start_date
      ELSE date_trunc('month', start_date)

    END as new_start_date,

    CASE

      WHEN start_date = min(start_date) OVER (PARTITION BY code, gant_index) -- первая часть
      THEN least(end_date, date_trunc('month', start_date) + interval '1 month')
      ELSE 
        CASE
        WHEN date_trunc('month', start_date) = date_trunc('month', end_date)
        THEN end_date
        ELSE date_trunc('month', start_date) + interval '1 month'
        END

    END as new_end_date
    FROM cte),

cte_2 AS (SELECT
	  gant_index,
    code,
    new_start_date as start_date,
    new_end_date as end_date,
    EXTRACT(YEAR FROM new_start_date) AS start_year,
    EXTRACT(MONTH FROM new_start_date) AS start_month,
    project_type,
    "object",
    EXTRACT(epoch from new_end_date - new_start_date) as duration,
    count(*) OVER (PARTITION BY code, gant_index) as row_oper_count -- кол-во строк, относящихся к одной операции
    
FROM cte_1),

cte_3 AS (SELECT
 	  gant_index,
    code,
    start_date,
    end_date,
    project_type,
    "object",
    start_year,
    start_month,
    duration,
    
    CASE 
      when duration = 0
      then 1
      else duration/sum(duration) OVER (PARTITION BY code, gant_index)
    END AS weight

FROM cte_2
WHERE NOT (duration = 0 and row_oper_count > 1))

SELECT
	t.gant_index,
	t.code,
  t.start_year,
  t.start_month,
  'план' as smr_type,
  --доп поля

  r."object",
  weight * r."c_pln_SMRSsI" as smr_ss,
  weight * r."c_pln_SMRSpI" as smr_sp,
  
  round((weight * (coalesce(r."c_pln_SMRSsI", 0) + coalesce(r."c_pln_SMRSpI", 0)))) as "SMRFull",
  round((weight * ( - coalesce(r."c_pln_AmLiz", 0) - coalesce(r."c_pln_FnOpTr", 0) - coalesce(r."c_pln_FuelMiM", 0) - coalesce(r."c_pln_Materl", 0) - coalesce(r."c_pln_OplGpd", 0) - coalesce(r."c_pln_OpSbRb", 0) - coalesce(r."c_pln_PrMatl", 0) - coalesce(r."c_pln_ProZtr", 0) - coalesce(r."c_pln_St_Mex", 0) - coalesce(r."c_pln_StrVzn", 0) - coalesce(r."c_pln_UslStH", 0) - coalesce(r."c_pln_RepMiM", 0) - coalesce(r."c_pln_NkRuch", 0)
))) as "ZATRATY",
  round((weight * ( - coalesce(r."c_pln_AmLiz", 0) - coalesce(r."c_pln_FnOpTr", 0) - coalesce(r."c_pln_FuelMiM", 0) - coalesce(r."c_pln_Materl", 0) - coalesce(r."c_pln_OplGpd", 0) - coalesce(r."c_pln_OpSbRb", 0) - coalesce(r."c_pln_PrMatl", 0) - coalesce(r."c_pln_ProZtr", 0) - coalesce(r."c_pln_St_Mex", 0) - coalesce(r."c_pln_StrVzn", 0) - coalesce(r."c_pln_UslStH", 0) - coalesce(r."c_pln_RepMiM", 0) - coalesce(r."c_pln_NkRuch", 0) + coalesce(r."c_pln_SMRSpI", 0) + coalesce(r."c_pln_SMRSsI", 0) + coalesce(r."c_pln_UslGpd", 0)
))) as "PRIBYL",
round((weight * (coalesce(r."c_pln_UslGpd", 0)))) as "UslGpd"

FROM cte_3 t
	
JOIN {{source('spider', 'raw_spider__gandoper')}} r

{# to do: уникальный индекс в таблице исходных данных #}
ON t.gant_index = r."index" AND t.code = r."Code" AND t.project_type = r.project_type
