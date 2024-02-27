{# Проверка исходных данных spider на дублирование #}

select
    "object",
    nz_year,
    nz_month,
    project_type,
    status
from (
    select
        "object",
        "nz_year",
        "nz_month",
        "project_type",
        concat("nz_year", "nz_month", "project_type", "Code"),
        count(concat("nz_year", "nz_month", "project_type", "Code")),
        case
            when count(concat("nz_year", "nz_month", "project_type", "Code")) > 1 then 'ДУБЛИРОВАНИЕ'
            else ''
        end status
    from {{source('spider','raw_spider__gandoper')}} g
    group by
        "object",
        nz_year,
        nz_month,
        project_type,
        concat(nz_year, nz_month, project_type, "Code")
) as foo
where status = 'ДУБЛИРОВАНИЕ'
