{{ config(materialized="table") }}

with
    t1 as (
        -- 1 шаг Ненормируемые работы из таблицы '1_c__technique_workers'
        select
            don.spider_name as "object",
            -- cw.zhufvr_id,
            ctw.work_id,
            cw.type_work_name as "Вид работ",
            cw.enrp_type as "Тип ЕНРП",
            ctw.res_name as "Ресурс Spider",
            ctw.res_code as "Код ресурса",
            ctw.hours as "Фактическая трудоемкость",
            case
                when ctw.hours != 0 then ctw.hours else 0
            end as "Нормативная трудоемкость",
            false::boolean as "Нормируемая",
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
            {{ source("dicts", "dict__1c_objects_to_spider") }} as don
            on czh.territory_value = don.territory_value
        -- фильтр ниже присваивает статус ненормируемая работа всем работам, которых
        -- нет в таблице "1_c__norm_workload"
        where
            not exists (
                select ctw.work_id
                from {{ source("1c", "1_c__norm_workload") }} as cnw
                where ctw.work_id = cnw.work_id
            )
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
            cw.enrp_type as "Тип ЕНРП",
            cnw.res_spider_name as "Ресурс Spider",
            cnw.res_spider_code as "Код ресурса",
            cnw.fact_workload as "Фактическая трудоемкость",
            cnw.norm_workload as "Нормативная трудоемкость",
            true::boolean as "Нормируемая",
            case
                when cnw.nt_res is false
                then 'Собственная'::varchar(11)
                when cnw.nt_res is true
                then 'Наемная'::varchar(11)
            end as "Ресурс"

        from {{ source("1c", "1_c__norm_workload") }} as cnw
        inner join {{ source("1c", "1_c__works") }} as cw on cnw.work_id = cw.work_id
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
    t1.*,
    cw.structure_unique_code,
    case
        when rsg."SNT_Knstr" = 'Земляное полотно'
        then 'Земляное полотно'
        when rsg."SNT_Knstr" = 'Дорожная одежда'
        then 'Дорожная одежда'
        else 'Прочие работы'
    end as "Конструктив",
    rsg."SNT_KnstrE" as "Конструктивный элемент",
    vdc."Позиция из КВ",
    czh.zhfvr_date as "Дата",
    czh.work_shift_name as "Смена",
    extract(year from czh.zhfvr_date)::int as "Год",
    extract(month from czh.zhfvr_date)::int as "Месяц",
    abs(
        round(
            (t1."Фактическая трудоемкость" / t1."Нормативная трудоемкость" * 100 - 100),
            2
        )
    ) as "Недопустимый % отклонения",
    not coalesce(skr."rescode" is null, false)::boolean as "Ключевой ресурс"
{# case 
        when ctw.contragent_name = 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО' then 'Собственная'::varchar(11)
        when ctw.contragent_name != 'ТРАНССТРОЙМЕХАНИЗАЦИЯ ООО' then 'Наемная'::varchar(11)
        else null
    end "Техника" #}
from t1
-- соединение с таблицей 1с работы
left join {{ source("1c", "1_c__works") }} as cw on t1.work_id = cw.work_id
-- соединение с таблицей Гантт работ из спайдер
left join
    {{ source("spider", "raw_spider__gandoper") }} as rsg
    on replace(cw.structure_unique_code, ' ', '') = rsg."f_CodeIdent"
    and t1."object" = rsg."object"
-- соединение со сводной таблицей контрактных позиций
left join
    {{ ref("int__vdc_by_objects") }} as vdc
    on rsg."Num_Con" = vdc."Шифр единичной расценки"
    and t1."object" = vdc."object"
-- соединение с таблицей 1с ЖУФВР
left join {{ source("1c", "1_c__zhufvr") }} as czh on cw.zhufvr_id = czh.link
-- соединение с таблицей 
left join
    {{ source("dicts", "dict__spider_key_res") }} as skr
    on t1."Тип ЕНРП" = skr."f_opertype"
    and t1."Код ресурса" = skr."rescode"

-- фильтр ниже для выборки только целых проектов
where rsg.project_type = 'проект'
