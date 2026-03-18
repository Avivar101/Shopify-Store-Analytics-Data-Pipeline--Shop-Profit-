{{
  config(
    materialized = 'table',
    )
}}

with numbers as (

    select cast(n as integer) as n
    from generate_series(0, 4017) as t(n)

),

dates as (

    select
        date '2020-01-01' + n as date_day
    from numbers

)

select
    date_day,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    strftime(date_day, '%Y-%m') as year_month,
    strftime(date_day, '%Y-Q') || cast(extract(quarter from date_day) as varchar) as year_quarter,
    extract(week from date_day) as week_of_year,
    extract(day from date_day) as day_of_month,
    extract(doy from date_day) as day_of_year,
    extract(isodow from date_day) as day_of_week,
    case extract(isodow from date_day)
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        when 7 then 'Sunday'
    end as day_name,
    case
        when extract(isodow from date_day) in (6, 7) then true
        else false
    end as is_weekend
from dates