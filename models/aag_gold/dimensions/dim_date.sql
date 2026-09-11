-- Standard date dimension. Bounds set via dim_date_start/dim_date_end vars.
-- depends_on: {{ ref('_identity_resolution') }}

with date_spine as (
    select explode(sequence(
        to_date('{{ var("dim_date_start") }}'),
        to_date('{{ var("dim_date_end") }}'),
        interval 1 day
    )) as date_day
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day,
    year(date_day)                                  as year,
    month(date_day)                                 as month,
    date_format(date_day, 'MMMM')                    as month_name,
    quarter(date_day)                                 as quarter,
    day(date_day)                                      as day_of_month,
    dayofweek(date_day)                                  as day_of_week_num,
    date_format(date_day, 'EEEE')                          as day_name,
    weekofyear(date_day)                                     as week_of_year,
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend
from date_spine