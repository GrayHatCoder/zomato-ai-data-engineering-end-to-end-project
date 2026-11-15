{% if target.type == 'duckdb' %}
-- local (DuckDB) date spine
with spine as (
    select cast(range as date) as date_day
    from range(date '2024-01-01', date '2027-01-01', interval 1 day))
{% else %}
with spine as (
    select dateadd(day, seq4(), '2024-01-01'::date) as date_day 
    from 
    table(generator(rowcount=>1200)))
{% endif %}
select date_day, year(date_day) as year, month(date_day) as month, monthname(date_day) as month_name,
       dayname(date_day) as day_name, (dayofweekiso(date_day)>=6) as is_weekend
from spine where date_day <= '2026-12-31'
