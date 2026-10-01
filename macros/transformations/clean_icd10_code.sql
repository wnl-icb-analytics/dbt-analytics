{% macro clean_icd10_code(code_column) %}
case 
    when {{ code_column }} is null then null
    when {{ code_column }} = 'Code deprecated' then 'Code deprecated'
    when length(trim(regexp_replace(replace({{ code_column }}, '.', ''), '[-][A-Z0-9]$|[X-]+$|X+$', ''))) > 3 then 
        left(trim(regexp_replace(replace({{ code_column }}, '.', ''), '[-][A-Z0-9]$|[X-]+$|X+$', '')), 3) || '.' || substr(trim(regexp_replace(replace({{ code_column }}, '.', ''), '[-][A-Z0-9]$|[X-]+$|X+$', '')), 4)
    else 
        trim(regexp_replace(replace({{ code_column }}, '.', ''), '[-][A-Z0-9]$|[X-]+$|X+$', ''))
end
{% endmacro %}