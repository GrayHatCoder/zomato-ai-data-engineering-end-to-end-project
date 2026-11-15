{#
  Lets the SAME dbt models run on Snowflake (cloud) and DuckDB (local laptop run).

  The models use Snowflake functions (try_to_number, iff, div0, initcap, ...).
  When the target is DuckDB, this hook (called from on-run-start in dbt_project.yml)
  creates DuckDB macros with the same names and behaviour. On Snowflake it does nothing.
#}
{% macro duckdb_compat() %}
  {% if execute and target.type == 'duckdb' %}
    {% set has_number = run_query("select count(*) from duckdb_types() where lower(type_name) = 'number' and database_name = current_database()").columns[0].values()[0] %}
    {% if has_number == 0 %}
      {% do run_query("create type main.number as bigint") %}
    {% endif %}
    {% set stmts = [
      "create or replace macro main.try_to_number(x) as try_cast(x as bigint)",
      "create or replace macro main.try_to_decimal(x, p, s) as round(try_cast(x as double), s)",
      "create or replace macro main.regexp_substr(s, pattern) as nullif(regexp_extract(s, pattern), '')",
      "create or replace macro main.iff(cond, a, b) as case when cond then a else b end",
      "create or replace macro main.div0(a, b) as case when b = 0 then 0 else a / b end",
      "create or replace macro main.initcap(s) as array_to_string(list_transform(string_split(lower(s), '-'), lambda w: upper(left(w, 1)) || substr(w, 2)), '-')",
      "create or replace macro main.dayofweekiso(d) as isodow(d)",
    ] %}
    {% for s in stmts %}
      {% do run_query(s) %}
    {% endfor %}
  {% endif %}
  select 1
{% endmacro %}
