{{
    config(materialized='table')
}}

with t1 as (
            --1 шаг Ненормируемые работы из таблицы '1_c__technique_workers'
            select
                don.spider_name as "object",
--                cw.zhufvr_id,
                ctw.work_id,
                cw.type_work_name as    "Вид работ",
                cw.enrp_type as         "Тип ЕНРП",
                ctw.res_name as         "Ресурс Spider",
                ctw.res_code as         "Код ресурса",
                ctw.hours as            "Фактическая трудоемкость",
                case
                    when ctw.hours != 0 then ctw.hours
                    else 0
                end                     "Нормативная трудоемкость",
                false:: boolean as      "Нормируемая",
                case 
                    when ctw.contragent_name = 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО' then 'Собственная'::varchar(11)
                    when ctw.contragent_name != 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО' then 'Наемная'::varchar(11)
                    when ctw.analytics_name like '(НТ)%' then 'Наемная'::varchar(11)
                    when ctw.analytics_name not like '(НТ)%' then 'Собственная'::varchar(11)
                    else null
                end "Ресурс"
                

            from {{ source('1c', '1_c__technique_workers') }} ctw
            inner join {{ source('1c', '1_c__works') }} cw using (work_id)
            left join {{ source('1c', '1_c__zhufvr') }} czh on cw.zhufvr_id = czh.link
            left join {{ source('dicts', 'dict__1c_objects_to_spider') }} don on czh.territory_value = don.territory_value
            --фильтр ниже присваивает статус ненормируемая работа всем работам, которых нет в таблице "1_c__norm_workload"
            where not exists (select  
                                work_id from {{ source('1c', '1_c__norm_workload') }} cnw
                            where ctw.work_id = cnw.work_id)
                  --фильтр ниже убирает удаленные и непроведенные ЖУФВР-ы
                  and (czh.delete_flag is false and czh.is_done is true)
                  and don.spider_name is not null

            union all 

            --2 шаг Нормируемые работы из таблицы '1_c__norm_workload'
            SELECT 
                    don.spider_name as "object",
--                    cw.zhufvr_id,                    
                    cnw.work_id,
                    cw.type_work_name as    "Вид работ",
                    cw.enrp_type as         "Тип ЕНРП",
                    cnw.res_spider_name as  "Ресурс Spider",
                    cnw.res_spider_code as "Код ресурса",
                    cnw.fact_workload as    "Фактическая трудоемкость",
                    cnw.norm_workload as    "Нормативная трудоемкость",
                    true:: boolean as       "Нормируемая",
                    case 
                        when cnw.nt_res is false then 'Собственная'::varchar(11)
                        when cnw.nt_res is true then 'Наемная'::varchar(11)
                        else null
                    end "Ресурс"
                    
                FROM {{ source('1c', '1_c__norm_workload') }} cnw
                inner join {{ source('1c', '1_c__works') }} cw using(work_id)
                left join {{ source('1c', '1_c__zhufvr') }} czh on cw.zhufvr_id = czh.link
                left join {{ source('dicts', 'dict__1c_objects_to_spider') }} don on czh.territory_value = don.territory_value
                --фильтр ниже убирает удаленные и непроведенные ЖУФВР-ы
                where (czh.delete_flag is false and czh.is_done is true)
                and don.spider_name is not null

)

select 
    t1.*,
--    t1."Фактическая трудоемкость" / sum(t1."Фактическая трудоемкость") over (partition by DATE_TRUNC('day', czh.zhfvr_date), t1."Вид работ") as weight,
    cw.structure_unique_code,
--    rsg."object_1",
--    rsg."f_CodeIdent",
    case 
        when rsg."SNT_Knstr" = 'Земляное полотно' then 'Земляное полотно'
        when rsg."SNT_Knstr" = 'Дорожная одежда' then 'Дорожная одежда'
        ELSE 'Прочие работы'
    end as "Конструктив",
    rsg."SNT_KnstrE" as "Конструктивный элемент",
    vdc."Позиция из КВ",
    czh.zhfvr_date as "Дата",
    czh.work_shift_name as "Смена",
    extract(year from czh.zhfvr_date)::int as "Год",
    extract(month from czh.zhfvr_date)::int as "Месяц",
    abs(t1."Фактическая трудоемкость" / t1."Нормативная трудоемкость" * 100 - 100) as "Недопустимый % отклонения",
    case when skr."rescode" is null then false
        else true
    end ::boolean as "Ключевой ресурс"
    {# case 
        when ctw.contragent_name = 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО' then 'Собственная'::varchar(11)
        when ctw.contragent_name != 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО' then 'Наемная'::varchar(11)
        else null
    end "Техника" #}

from t1
--соединение с таблицей 1с работы
left join {{ source('1c', '1_c__works') }} cw using(work_id)
--соединение с таблицей Гантт работ из спайдер
left join {{ source('spider', 'raw_spider__gandoper') }} rsg on replace(cw.structure_unique_code,' ','') = rsg."f_CodeIdent" and t1."object" = rsg."object"
--соединение со сводной таблицей контрактных позиций
left join {{ ref('int__vdc_by_objects') }} vdc on rsg."Num_Con" = vdc."Шифр единичной расценки" and t1."object" = vdc."object"
--соединение с таблицей 1с ЖУФВР
left join {{ source('1c', '1_c__zhufvr') }} czh on cw.zhufvr_id = czh.link
--соединение с таблицей 
left join {{ source('dicts', 'dict__spider_key_res') }} skr on t1."Тип ЕНРП" = skr."f_opertype" and t1."Код ресурса" = skr."rescode"

--фильтр ниже для выборки только целых проектов
where rsg.project_type = 'проект'