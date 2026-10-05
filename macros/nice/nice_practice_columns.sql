{% macro nice_practice_columns(alias, reference) %}
{#-
    Emit practice columns under the current or monthly interface names.
    Args: alias is a SQL alias with practice_code/name, or none for a union input
          already using the interface names; reference is current or by_month.
    Returns: two SELECT expressions, current_practice_code/name or practice_code/name.
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{% set prefix = 'current_' if reference == 'current' else '' %}
{% if alias %}
{{ alias }}.practice_code AS {{ prefix }}practice_code,
{{ alias }}.practice_name AS {{ prefix }}practice_name
{% else %}
{{ prefix }}practice_code,
{{ prefix }}practice_name
{% endif %}
{% endmacro %}
