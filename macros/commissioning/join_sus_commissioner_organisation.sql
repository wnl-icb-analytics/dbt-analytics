{% macro join_sus_commissioner_organisation(code_expression, alias) %}
left join {{ ref('organisation') }} as {{ alias }}_exact
    on {{ alias }}_exact.organisation_code = case
        when nullif(upper(trim({{ code_expression }})), '') = 'VPP' then 'VPP00'
        else nullif(upper(trim({{ code_expression }})), '')
    end

left join {{ ref('organisation') }} as {{ alias }}_fallback
    on {{ alias }}_exact.organisation_code is null
    and length(nullif(upper(trim({{ code_expression }})), '')) = 5
    and right(nullif(upper(trim({{ code_expression }})), ''), 2) = '00'
    and left(nullif(upper(trim({{ code_expression }})), ''), 3)
        = {{ alias }}_fallback.organisation_code
{% endmacro %}

{% macro sus_commissioner_organisation_name(alias) %}
coalesce(
    {{ alias }}_exact.organisation_name
    , {{ alias }}_fallback.organisation_name
)
{% endmacro %}
