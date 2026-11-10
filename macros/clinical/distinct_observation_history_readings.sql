{% macro distinct_observation_history_readings(model, value, unit, condition, extra_lookup_cluster=none) -%}
    SELECT reading.*
    FROM {{ ref(model) }} AS reading
    {% if extra_lookup_cluster is not none %}
    LEFT JOIN (
        SELECT id, mapped_concept_code, result_value, patient_id, date_recorded
        FROM {{ ref('stg_olids_observation') }}
        WHERE mapped_concept_code IN (
            SELECT concept_code FROM {{ ref(model) }}
            UNION
            -- Calculated BMI retains the weight observation id.
            SELECT code FROM {{ ref('stg_reference_combined_codesets') }}
            WHERE UPPER(cluster_id) = '{{ extra_lookup_cluster }}'
        )
    ) AS source_observation
        ON reading.id = source_observation.id
    {% else %}
    LEFT JOIN {{ ref('stg_olids_observation') }} AS source_observation
        ON reading.id = source_observation.id
        -- The concept join prunes the source clustered by mapped concept.
        AND reading.concept_code = source_observation.mapped_concept_code
    {% endif %}
    WHERE {{ condition }}
        AND reading.clinical_effective_date <= CURRENT_DATE
    -- Each caller supplies one observation type. Rank before the large history union.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY reading.person_id,
            COALESCE(source_observation.mapped_concept_code, reading.id::VARCHAR),
            reading.clinical_effective_date::DATE, source_observation.result_value,
            {{ value }}, {{ unit }}, source_observation.patient_id,
            IFF({{ is_unknown_clinical_day('reading.clinical_effective_date') }},
                reading.id::VARCHAR, NULL)
        ORDER BY reading.clinical_effective_date DESC,
            source_observation.date_recorded DESC NULLS LAST, reading.id DESC
    ) = 1
{%- endmacro %}
