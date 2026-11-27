{{ config(
    materialized='table',
    tags=['indicator_definitions'],
    post_hook="{{ archive_definitions() }}"
) }}

/*
Main indicators reference table.
Extracts indicator definitions from model and column YAML meta properties.
One row per indicator with core metadata for dashboards and reporting.
The post-hook stores daily-grain history and repairs duplicate current versions.
*/

{%- set metadata = extract_indicator_metadata() -%}
{%- set validation_results = {
    'matches': 'Matches',
    'difference_explained': 'Matches once a known difference is allowed for',
    'difference_not_tested': 'Plausible difference, not tested',
    'related_figure_only': 'Only a related published figure',
    'no_comparator': 'No published figure'
} -%}

WITH indicators AS (
    {% if metadata.indicators %}
        {% for ind in metadata.indicators %}
        SELECT
            '{{ ind.indicator_id }}' AS indicator_id,
            '{{ ind.indicator_type }}' AS indicator_type,
            '{{ ind.category | default("") | replace("'", "''") }}' AS category,
            '{{ ind.name_short | replace("'", "''") }}' AS name_short,
            '{{ ind.description_short | replace("'", "''") }}' AS description_short,
            '{{ ind.description_long | replace("'", "''") }}' AS description_long,
            '{{ ind.source_model }}' AS source_model,
            '{{ ind.source_column }}' AS source_column,
            {% if ind.is_qof is not none %}
                {{ ind.is_qof }} AS is_qof,
            {% else %}
                NULL AS is_qof,
            {% endif %}
            {% if ind.qof_indicator is not none %}
                '{{ ind.qof_indicator }}' AS qof_indicator,
            {% else %}
                NULL AS qof_indicator,
            {% endif %}
            '{{ ind.sort_order | replace("'", "''") }}' AS sort_order,
            {% if ind.validation_status %}
                {{ 'TRUE' if ind.validation_status == 'validated' else 'FALSE' }} AS is_externally_validated,
                '{{ validation_results.get(ind.validation_result, ind.validation_result) }}' AS external_validation_result,
                {% if ind.validation_comparator %}'{{ ind.validation_comparator | replace("'", "''") }}'{% else %}NULL{% endif %} AS external_validation_comparator,
                {% if ind.validation_note %}'{{ ind.validation_note | replace("'", "''") }}'{% else %}NULL{% endif %} AS external_validation_note,
            {% else %}
                NULL AS is_externally_validated,
                NULL AS external_validation_result,
                NULL AS external_validation_comparator,
                NULL AS external_validation_note,
            {% endif %}
            CURRENT_TIMESTAMP() AS metadata_extracted_at
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    {% else %}
        SELECT 
            NULL AS indicator_id,
            NULL AS indicator_type,
            NULL AS category,
            NULL AS name_short,
            NULL AS description_short,
            NULL AS description_long,
            NULL AS source_model,
            NULL AS source_column,
            NULL AS is_qof,
            NULL AS qof_indicator,
            NULL AS sort_order,
            NULL AS is_externally_validated,
            NULL AS external_validation_result,
            NULL AS external_validation_comparator,
            NULL AS external_validation_note,
            CURRENT_TIMESTAMP() AS metadata_extracted_at
        WHERE FALSE  -- Empty result set
    {% endif %}
)

SELECT
    i.indicator_id,
    i.indicator_type,
    c.programme,
    COALESCE(ltc.clinical_domain, c.clinical_domain) AS clinical_domain,
    COALESCE(ltc.condition_name, c.clinical_subdomain) AS clinical_subdomain,
    i.category,
    i.name_short,
    i.description_short,
    i.description_long,
    i.source_model,
    i.source_column,
    i.is_qof,
    i.qof_indicator,
    i.sort_order,
    i.is_externally_validated::BOOLEAN AS is_externally_validated,
    i.external_validation_result::VARCHAR AS external_validation_result,
    i.external_validation_comparator::VARCHAR AS external_validation_comparator,
    i.external_validation_note::VARCHAR AS external_validation_note,
    i.metadata_extracted_at
FROM indicators i
LEFT JOIN {{ ref('indicator_classification') }} c ON i.indicator_id = c.indicator_id
LEFT JOIN {{ ref('ltc_register_denominator_rules') }} ltc ON c.condition_code = ltc.condition_code
ORDER BY i.sort_order, i.indicator_id
