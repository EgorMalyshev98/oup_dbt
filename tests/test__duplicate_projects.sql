{# Проверка исходных данных spider на дублирование #}
select "object", nz_year, nz_month, project_type, status
from
    (
        select
            "object",
            "nz_year",
            "nz_month",
            "project_type",
            concat("nz_year", "nz_month", "project_type", "Code"),  -- noqa
            count(concat("nz_year", "nz_month", "project_type", "Code")),  -- noqa
            case
                when count(concat("nz_year", "nz_month", "project_type", "Code")) > 1
                then 'ДУБЛИРОВАНИЕ'
                else ''
            end as status
        from {{ source("spider", "raw_spider__gandoper") }}
        group by
            "object",
            nz_year,
            nz_month,
            project_type,
            concat(nz_year, nz_month, project_type, "Code")
    ) as foo
where status = 'ДУБЛИРОВАНИЕ'
