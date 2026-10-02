{% macro nice_ref(model_name, reference='current') %}
{# Only pairs from one calculation macro with identical physical columns. #}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{{ return(ref(model_name if reference == 'current' else model_name ~ '_by_month')) }}
{% endmacro %}
