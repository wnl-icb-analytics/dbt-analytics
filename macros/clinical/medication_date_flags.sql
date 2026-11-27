{%- macro medication_date_flags(date_column, months=[3, 6, 12], include_future_flag=true, reference_date='current_date()') -%}
{# Recent means after the calendar-month lower bound and on or before the reference date. #}
{%- set flags = [] -%}
{%- if include_future_flag -%}
    {%- do flags.append('coalesce(' ~ date_column ~ ' > ' ~ reference_date ~ ', false) as is_future_dated') -%}
{%- endif -%}
{%- for n in months -%}
    {%- do flags.append('coalesce(' ~ date_column ~ ' > dateadd(month, -' ~ n ~ ', ' ~ reference_date ~ ') and '
        ~ date_column ~ ' <= ' ~ reference_date ~ ', false) as is_recent_' ~ n ~ 'm') -%}
{%- endfor -%}
{{ flags | join(',\n    ') }}
{%- endmacro -%}
