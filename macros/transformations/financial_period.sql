{# NHS financial period helpers. Financial year runs April-March; month 1 =
   April ... 12 = March. The financial year is rendered as 'YYYYYY' (start year
   + 2-digit end year, e.g. FY2025/26 -> '202526') to match the SLAM feeds.
   Used as the period-of-last-resort when a feed's stated period is absent
   (providers bill by activity date), mirroring the SLAM activity-date fallback
   (#806). #}

{# Financial month 1-12 from a date (April -> 1, March -> 12). #}
{% macro fin_month_from_date(d) %}
    mod(month({{ d }}) + 8, 12) + 1
{% endmacro %}

{# Financial year 'YYYYYY' from a date (e.g. 2024-12-10 -> '202425',
   2025-02-10 -> '202425'). #}
{% macro fin_year_from_date(d) %}
    case when {{ d }} is not null then
        to_varchar(iff(month({{ d }}) >= 4, year({{ d }}), year({{ d }}) - 1))
        || lpad(to_varchar(mod(iff(month({{ d }}) >= 4, year({{ d }}), year({{ d }}) - 1) + 1, 100)), 2, '0')
    end
{% endmacro %}

{# Financial year 'YYYYYY' from a bare 4-digit start year string
   (e.g. '2019' -> '201920'); non-4-digit-year input -> NULL. #}
{% macro fin_year_from_start_year(y) %}
    case when trim({{ y }}) rlike '^20[0-9]{2}$'
        then trim({{ y }}) || lpad(to_varchar(mod(cast(trim({{ y }}) as int) + 1, 100)), 2, '0')
    end
{% endmacro %}
