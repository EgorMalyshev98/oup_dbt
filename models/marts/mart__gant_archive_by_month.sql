{{ config(materialized="table") }}


with
    tmp as (

        select
            code,
            start_year,
            start_month,
            smr_ss,
            smr_sp,
            "object",
            smr_type,
            "ZATRATY",
            "PRIBYL",
            "SMRFull",
            "UslGpd"

        from {{ ref("int__gant_by_month") }}

        union all

        select
            t1."code",
            t1."start_year",
            t1."start_month",
            t1.smr_ss,
            t1.smr_sp,
            t1."object",
            t1.smr_type,
            t1."ZATRATY",
            t1."PRIBYL",
            t1."SMRFull",
            "UslGpd"

        from {{ ref("int__archive_by_month") }} as t1

    ),

    final as (
        select
            t.*,

            case
                when length(cast(start_month as text)) = 2
                then cast(start_month as text)
                else cast(concat('0', cast(t.start_month as text)) as text)
            end as s_month,

            g."Name",
            g."Ispol",
            g."IspolUch",
            /* Временная мера для избавления от недопустимых значений поля Real*/
            case
                g."Real"
                when null
                then null
                when 'Собственные силы'
                then 'Собственными силами'
                when 'Собственными силами'
                then 'Собственными силами'
                when 'Субподряд'
                then 'Силами субподрядных организаций'
                when 'Силами субподрядных организаций'
                then 'Силами субподрядных организаций'
            end as "Силы реализации",

            {# g."Real", #}
            g."SNT_Knstr",
            g."SNT_KnstrE",
            g."SNT_TypeKnstrE",
            g."SNT_Obj",
            g."Num_Con",

            {# case
            when t.smr_ss is not null then 'Собственными силами'
            when t.smr_sp is not null then 'Силами субподрядных организаций'
            else ''
        end as "Силы реализации", #}
            case
                when g."SNT_Knstr" = 'Земляное полотно'
                then 'Земляное полотно'
                when g."SNT_Knstr" = 'Дорожная одежда'
                then 'Дорожная одежда'
                else 'Прочие работы'

            end as "Конструктив",

            vdc."Наименование работ и затрат",
            vdc."Позиция из КВ",
            vdc."СМР П"

        from tmp as t
        inner join
            {{ source("spider", "raw_spider__gandoper") }} as g
            on t.code = g."Code"
            and t."object" = g."object"
            and g.project_type = 'проект'

        left join
            {{ ref("int__vdc_by_objects") }} as vdc
            on g."Num_Con" = vdc."Шифр единичной расценки"
            and g."object" = vdc."object"
    )

select *
from final
