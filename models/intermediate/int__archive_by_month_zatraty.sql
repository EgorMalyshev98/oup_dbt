{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['code'], 'type': 'hash'}]
    )
}}


WITH RECURSIVE tmp1 AS 
(SELECT 
	"index" AS archive_index,
	"OperCode" AS code, 
	"Start" AS start_date,
    CASE
        WHEN date_part('day', "Fin") = 1 
            THEN "Fin" - INTERVAL '1 day'
            ELSE "Fin" 
    END AS end_date,
	"Vol" AS vol,
	EXTRACT(YEAR FROM "Start") AS start_year,
    EXTRACT(MONTH FROM "Start") AS start_month,
    project_type,
    1 AS num_of_parts

FROM {{source('spider', 'raw_spider__archive')}}
WHERE 
	"ResCode" IS NULL AND project_type = 'проект'

	
	UNION ALL
  
SELECT
	archive_index,
	code,
	start_date + INTERVAL '1 MONTH',
	end_date,
	vol,
	EXTRACT(YEAR FROM start_date + INTERVAL '1 MONTH'),
	EXTRACT(MONTH FROM start_date + INTERVAL '1 MONTH'),
    project_type,
	num_of_parts + 1
	
FROM tmp1 

WHERE 
	date_trunc('month', start_date + INTERVAL '1 MONTH') <= date_trunc('month', end_date) 
),

tmp2 AS (SELECT 
    code,
    start_date,
    end_date,
    vol,
    start_year,
    start_month,
    archive_index,
    project_type,
    
    CASE

		WHEN start_date = MAX(start_date) OVER (PARTITION BY code, archive_index) AND date_trunc('month', start_date) = date_trunc('month', end_date)  
			THEN date_part('day', end_date)
    	WHEN start_date = MIN(start_date) OVER (PARTITION BY code, archive_index) THEN
    		CASE 
    			WHEN date_part('day', (date_trunc('month', start_date) + interval '1 month') - date_trunc('day', start_date)) = 0
    			THEN 1
    			ELSE date_part('day', (date_trunc('month', start_date) + interval '1 month') - date_trunc('day', start_date))
    		END
    	ELSE date_part('day', (date_trunc('month', start_date) + INTERVAL '1 MONTH' - date_trunc('month', start_date)))
    END AS num_days -- использовать только для расчета весов

    
FROM tmp1),

tmp3 AS (SELECT
    archive_index,
	code,
    project_type,
    start_year,
    start_month,
    vol,
    num_days,
    num_days/sum(num_days) OVER(PARTITION BY code, archive_index) AS weight
	
FROM tmp2),

final as (SELECT 
    row_number() over() as id,
    t.archive_index,
    t.code,
    t.start_year,
    t.start_month,
    'факт' as ZATRATY_type,

    {# дополнительные поля #}
    r."object",
    {# sum(weight * r."WorkLoadFact") as workload, #} 
    round(sum(weight * ( - coalesce(r."c_aac_AmLiz", 0) - coalesce(r."c_aac_FnOpTr", 0) - coalesce(r."c_aac_FuelMiM", 0) - coalesce(r."c_aac_Materl", 0) - coalesce(r."c_aac_OplGpd", 0) - coalesce(r."c_aac_OpSbRb", 0) - coalesce(r."c_aac_PrMatl", 0) - coalesce(r."c_aac_ProZtr", 0) - coalesce(r."c_aac_St_Mex", 0) - coalesce(r."c_aac_StrVzn", 0) - coalesce(r."c_aac_UslStH", 0) - coalesce(r."c_aac_RepMiM", 0) - coalesce(r."c_aac_NkRuch", 0)
))) as ZATRATY,
    round(sum(weight * ( - coalesce(r."c_aac_AmLiz", 0) - coalesce(r."c_aac_FnOpTr", 0) - coalesce(r."c_aac_FuelMiM", 0) - coalesce(r."c_aac_Materl", 0) - coalesce(r."c_aac_OplGpd", 0) - coalesce(r."c_aac_OpSbRb", 0) - coalesce(r."c_aac_PrMatl", 0) - coalesce(r."c_aac_ProZtr", 0) - coalesce(r."c_aac_St_Mex", 0) - coalesce(r."c_aac_StrVzn", 0) - coalesce(r."c_aac_UslStH", 0) - coalesce(r."c_aac_RepMiM", 0) - coalesce(r."c_aac_NkRuch", 0) + coalesce(r."c_aac_SMRSpI", 0) + coalesce(r."c_aac_SMRSsI", 0) + coalesce(r."c_aac_UslGpd", 0)
))) as PRIBYL,
    round(sum(weight * (coalesce(r."c_aac_SMRSsI", 0) + coalesce(r."c_aac_SMRSpI", 0)))) as SMRFull
    
FROM tmp3 t

    JOIN {{source('spider', 'raw_spider__archive')}} r

    ON t.archive_index = r."index" AND t.code = r."OperCode" AND t.project_type = r.project_type


GROUP BY
    {# to do: уникальный индекс в таблице исходных данных #}
    t.archive_index,
    t.code,
    t.start_year,
    t.start_month,
    r."object")


{# оставляем только ключевые значения #}
SELECT * FROM final
WHERE ZATRATY IS NOT NULL or PRIBYL is not null or SMRFull is not null

