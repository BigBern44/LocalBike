-- Dimension calendaire.
-- Grain : un jour (date_day, PK).
-- Plage volontairement large (2016 -> 2019) pour couvrir order_date / required_date / shipped_date.

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2020-01-01' as date)"
    ) }}

),

final as (

    select
        date_day,
        extract(year      from date_day)               as year,
        extract(quarter   from date_day)               as quarter,
        extract(month     from date_day)               as month,
        format_date('%B',   date_day)                  as month_name,
        extract(day       from date_day)               as day_of_month,
        extract(dayofweek from date_day)               as day_of_week,   -- 1 = dimanche
        format_date('%A',   date_day)                  as day_name,
        extract(week      from date_day)               as week_of_year,
        format_date('%Y-%m', date_day)                 as year_month,
        extract(dayofweek from date_day) in (1, 7)     as is_weekend
    from date_spine

)

select * from final
