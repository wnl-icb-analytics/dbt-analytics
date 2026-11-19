{% macro mhsds_one_row_per_record(record_id, alias) -%}
    {#- The LDS share can deliver the same source row more than once (every row twice
        from 30 Oct 2026); keep one row per record identifier, preferring the latest load. -#}
    qualify row_number() over (
        partition by {{ record_id }}
        order by {{ alias }}.effective_from desc, {{ alias }}.dmic_date_added desc
    ) = 1
{%- endmacro %}
