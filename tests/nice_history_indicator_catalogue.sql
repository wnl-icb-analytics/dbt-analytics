{{ config(tags=['monthly-full', 'nice-history']) }}

{% set ids = [] %}
{% if execute %}
    {% for node in graph.nodes.values() %}
        {% set indicator = node.config.get('meta', {}).get('indicator', {}) %}
        {% if node.resource_type == 'model'
            and node.original_file_path.startswith('models/reporting/olids/measures/nice/')
            and not node.name.endswith('_by_month')
            and indicator.get('type') == 'MEASURE'
            and indicator.get('id', '').startswith('IND') %}
            {% do ids.append(indicator.id) %}
        {% endif %}
    {% endfor %}
    {% if ids | unique | list | length != 105 %}
        {{ exceptions.raise_compiler_error('Expected 105 current NICE measure definitions.') }}
    {% endif %}
{% endif %}

WITH expected AS (
    {% for id in ids | unique | sort %}
    SELECT '{{ id }}' AS indicator_id{% if not loop.last %} UNION ALL{% endif %}
    {% endfor %}
), actual AS (
    SELECT DISTINCT indicator_id
    FROM {{ ref('fct_person_nice_indicator_status_by_month') }}
)
(
    SELECT indicator_id FROM expected
    EXCEPT SELECT indicator_id FROM actual
)
UNION ALL
(
    SELECT indicator_id FROM actual
    EXCEPT SELECT indicator_id FROM expected
)
