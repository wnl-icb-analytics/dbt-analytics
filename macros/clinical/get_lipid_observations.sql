{% macro get_lipid_observations(cluster_id, value_column, triglycerides=false, ratio=false) %}
WITH observations AS (
    {{ get_observations("'" ~ cluster_id ~ "'", source='PCD') }}
),

recorded AS (
    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.clinical_effective_date_raw,
        obs.date_recorded,
        obs.result_value AS original_result_value,
        COALESCE(NULLIF(TRIM(obs.result_unit_code), ''), NULLIF(TRIM(units.code), '')) AS original_result_unit_code,
        COALESCE(NULLIF(TRIM(obs.result_unit_display), ''), NULLIF(TRIM(units.display), '')) AS original_result_unit_display,
        -- OLIDS often retains the source unit concept without a mapped unit.
        LOWER(REPLACE(COALESCE(
            NULLIF(TRIM(obs.result_unit_display), ''),
            NULLIF(TRIM(obs.result_unit_code), ''),
            NULLIF(TRIM(units.display), ''),
            NULLIF(TRIM(units.code), '')
        ), ' ', '')) AS unit_label,
        IFF(unit_label IN ('.', 'unknown', '(unknown)', 'unknownunits', 'unkuom', 'n/a', '(nouom)'),
            NULL, unit_label) AS recorded_unit,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        CASE
            WHEN LOWER(obs.code_description) LIKE '%non-fasting%' THEN 'Non-fasting'
            WHEN LOWER(obs.code_description) LIKE '%fasting%' THEN 'Fasting'
            WHEN LOWER(obs.code_description) LIKE '%random%' THEN 'Random'
            ELSE 'Not specified'
        END AS sampling_context
    FROM observations obs
    LEFT JOIN {{ ref('stg_olids_concept') }} units
        ON obs.result_units_source_concept_id = units.concept_id
    WHERE obs.clinical_effective_date_raw IS NOT NULL
        AND obs.clinical_effective_date_raw::DATE <= CURRENT_DATE()
        AND obs.clinical_effective_date::DATE <= CURRENT_DATE()
),

conversion AS (
    SELECT
        *,
        CASE
            {% if ratio %}
            WHEN recorded_unit IN ('ratio', '1', ':1', '1/1', 'mmol/mmol', 'mol/mol', 'totalcholesterol:hdlratio') THEN 1.0
            -- The PCD observable defines a dimensionless ratio when no unit is supplied.
            WHEN recorded_unit IS NULL THEN 1.0
            {% else %}
            WHEN recorded_unit = 'mmol/l' THEN 1.0
            WHEN recorded_unit IN ('umol/l', 'µmol/l', 'μmol/l') THEN 0.001
            -- Labcorp SI factors: cholesterol 0.0259; triglycerides 0.0113 per mg/dL.
            -- https://www.labcorp.com/test-menu/resources/si-unit-conversion-table
            WHEN recorded_unit IN ('mg/dl', 'mg/100ml') THEN {{ 0.0113 if triglycerides else 0.0259 }}
            WHEN recorded_unit = 'mg/l' THEN {{ 0.00113 if triglycerides else 0.00259 }}
            WHEN recorded_unit = 'g/l' THEN {{ 1.13 if triglycerides else 2.59 }}
            {% endif %}
        END AS conversion_factor
    FROM recorded
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    clinical_effective_date_raw,
    date_recorded,
    original_result_value * conversion_factor AS {{ value_column }},
    IFF(conversion_factor IS NOT NULL, '{{ 'ratio' if ratio else 'mmol/L' }}', NULL) AS result_unit_display,
    original_result_value,
    original_result_unit_code,
    original_result_unit_display,
    CASE
        WHEN recorded_unit IS NULL THEN '{{ 'Inferred from ratio observable' if ratio else 'Missing unit' }}'
        WHEN conversion_factor IS NULL THEN 'Unsupported unit'
        WHEN conversion_factor = 1 THEN 'Recorded standard unit'
        ELSE 'Converted'
    END AS unit_status,
    concept_code,
    concept_display,
    source_cluster_id,
    sampling_context
FROM conversion
{% endmacro %}
