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
                ctw.res_name as         "Ресурс Spider",
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
                    cnw.res_spider_name as  "Ресурс Spider",
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
    extract(month from czh.zhfvr_date)::int as "Месяц"

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
--соединени со сводной таблицей контрактных позиций
left join {{ ref('int__vdc_by_objects') }} vdc on rsg."Num_Con" = vdc."Шифр единичной расценки" and t1."object" = vdc."object"
--соединени с таблицей 1с ЖУФВР
left join {{ source('1c', '1_c__zhufvr') }} czh on cw.zhufvr_id = czh.link

--фильтр ниже для выборки только целых проектов
where rsg.project_type = 'проект'