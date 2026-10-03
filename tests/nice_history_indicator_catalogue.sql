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
    {% if ids | length != 146 or ids | unique | list | length != 146 %}
        {{ exceptions.raise_compiler_error('Expected 146 unique current NICE measure definitions.') }}
    {% endif %}
{% endif %}

WITH expected AS (
    {% for id in ids | unique | sort %}
    SELECT '{{ id }}' AS indicator_id{% if not loop.last %} UNION ALL{% endif %}
    {% endfor %}
), expected_dates AS (
    SELECT expected.indicator_id, dates.reporting_date
    FROM expected
    CROSS JOIN ({{ nice_reference_dates('by_month') }}) AS dates
    -- IND318's fixed diagnosis cohort starts on 1 April 2026.
    WHERE expected.indicator_id <> 'IND318'
        OR dates.reporting_date >= '2026-04-01'::DATE
), actual AS (
    SELECT DISTINCT indicator_id, reporting_date
    FROM {{ ref('fct_person_nice_indicator_status_by_month') }}
)
(
    SELECT indicator_id, reporting_date FROM expected_dates
    EXCEPT SELECT indicator_id, reporting_date FROM actual
)
UNION ALL
(
    SELECT indicator_id, reporting_date FROM actual
    EXCEPT SELECT indicator_id, reporting_date FROM expected_dates
)
