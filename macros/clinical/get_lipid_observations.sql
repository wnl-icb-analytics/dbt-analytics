{% macro get_lipid_observations(cluster_id, value_column, triglycerides=false, ratio=false) %}
{#- Inclusive validity ranges. A converted value outside its range is invalid and cannot be
    selected as a latest result. Ratios below 1 are arithmetically impossible. -#}
{%- set validity_ranges = {
    'CHOL2_COD': [0.5, 20],
    'LDLCCHOL_COD': [0.1, 20],
    'HDLCCHOL_COD': [0.1, 5],
    'NONHDLCCHOL_COD': [0.1, 20],
    'TRIGLYC_COD': [0.1, 50],
    'TCHOLHDL_COD': [1, 20]
} -%}
{%- set validity_range = validity_ranges[cluster_id] -%}
{#- Labels that mean "no unit supplied" rather than a unit. -#}
{%- set unknown_unit_labels = "'.', 'unknown', '(unknown)', 'unknownunits', 'unkuom', 'n/a', '(nouom)'" -%}
{%- set ratio_unit_labels = "'ratio', '1', ':1', '1/1', 'mmol/mmol', 'mol/mol', 'totalcholesterol:hdlratio'" -%}
{#- Labels whose recorded values profile as mmol/L in OLIDS despite the label: mg/100ml results
    have a median of 5.6 for total cholesterol and 1.5 for HDL; the others are typos or
    impossible units for a lipid concentration. Treated as mmol/L and flagged for review. -#}
{%- set mislabelled_standard_unit_labels = "'mg/100ml', 'g', 'mmo1/l', 'nmol/l', '(calculated)'" -%}
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
        obs.result_value AS recorded_value,
        NULLIF(TRIM(units.code), '') AS source_result_unit_code,
        NULLIF(TRIM(units.display), '') AS source_result_unit_display,
        NULLIF(TRIM(obs.result_unit_code), '') AS mapped_result_unit_code,
        NULLIF(TRIM(obs.result_unit_display), '') AS mapped_result_unit_display,
        {% for prefix in ['source', 'mapped'] %}
        LOWER(REPLACE(COALESCE({{ prefix }}_result_unit_display, {{ prefix }}_result_unit_code), ' ', '')) AS {{ prefix }}_unit_label,
        CASE
            WHEN {{ prefix }}_unit_label IN ({{ unknown_unit_labels }}) THEN NULL
            WHEN {{ prefix }}_unit_label IN ('µmol/l', 'μmol/l') THEN 'umol/l'
            WHEN {{ prefix }}_unit_label IN ({{ ratio_unit_labels }}) THEN 'ratio'
            ELSE {{ prefix }}_unit_label
        END AS {{ prefix }}_canonical_unit,
        {% endfor %}
        COALESCE(source_canonical_unit <> mapped_canonical_unit, FALSE) AS is_unit_metadata_conflict,
        -- The mapped unit takes precedence. An unknown-unit placeholder in the mapped
        -- field falls through to the source unit concept, which OLIDS often retains
        -- without a mapped unit.
        COALESCE(mapped_canonical_unit, source_canonical_unit) AS recorded_unit,
        CASE
            WHEN mapped_canonical_unit IS NOT NULL THEN 'Mapped unit'
            WHEN source_canonical_unit IS NOT NULL THEN 'Source unit'
            ELSE 'No recorded unit'
        END AS conversion_unit_basis,
        COALESCE(NULLIF(TRIM(obs.result_unit_code), ''), NULLIF(TRIM(units.code), '')) AS original_result_unit_code,
        COALESCE(NULLIF(TRIM(obs.result_unit_display), ''), NULLIF(TRIM(units.display), '')) AS original_result_unit_display,
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
            WHEN recorded_unit = 'ratio' THEN 'Recorded standard unit'
            -- The PCD observable defines a dimensionless ratio when no unit is supplied.
            WHEN recorded_unit IS NULL THEN 'Inferred from ratio observable'
            -- Any other label on a dimensionless observable is noise: recorded values
            -- labelled mmol/L, % or mg/mmol profile identically to labelled ratios.
            ELSE 'Mislabelled standard unit'
            {% else %}
            WHEN recorded_unit = 'mmol/l' THEN 'Recorded standard unit'
            -- A result with no recorded unit is retained as mmol/L, the UK reporting
            -- standard for lipids, and flagged for review rather than discarded.
            WHEN recorded_unit IS NULL THEN 'Missing unit'
            WHEN recorded_unit IN ({{ mislabelled_standard_unit_labels }}) THEN 'Mislabelled standard unit'
            WHEN recorded_unit IN ('umol/l', 'mg/dl', 'mg/l', 'g/l') THEN 'Converted'
            ELSE 'Unsupported unit'
            {% endif %}
        END AS unit_status,
        CASE
            WHEN unit_status IN ('Recorded standard unit', 'Missing unit', 'Inferred from ratio observable', 'Mislabelled standard unit') THEN 1.0
            WHEN recorded_unit = 'umol/l' THEN 0.001
            -- Labcorp SI factors: cholesterol 0.0259; triglycerides 0.0113 per mg/dL.
            -- https://www.labcorp.com/test-menu/resources/si-unit-conversion-table
            WHEN recorded_unit = 'mg/dl' THEN {{ 0.0113 if triglycerides else 0.0259 }}
            WHEN recorded_unit = 'mg/l' THEN {{ 0.00113 if triglycerides else 0.00259 }}
            WHEN recorded_unit = 'g/l' THEN {{ 1.13 if triglycerides else 2.59 }}
        END AS conversion_factor
    FROM recorded
)

SELECT
    id,
    person_id,
    clinical_effective_date,
    clinical_effective_date_raw,
    date_recorded,
    recorded_value,
    {% if not ratio %}
    recorded_value * conversion_factor AS converted_value_mmol_l,
    -- Keep the established analyte field for existing consumers.
    converted_value_mmol_l AS {{ value_column }},
    {% else %}
    recorded_value * conversion_factor AS {{ value_column }},
    {% endif %}
    IFF(conversion_factor IS NOT NULL, '{{ 'ratio' if ratio else 'mmol/L' }}', NULL) AS result_unit_display,
    recorded_value AS original_result_value,
    source_result_unit_code,
    source_result_unit_display,
    mapped_result_unit_code,
    mapped_result_unit_display,
    conversion_unit_basis,
    conversion_factor,
    is_unit_metadata_conflict,
    original_result_unit_code,
    original_result_unit_display,
    unit_status,
    CASE
        WHEN conversion_factor IS NULL THEN 'Unit cannot be converted'
        WHEN recorded_value IS NULL THEN 'Missing numeric result'
        WHEN NOT ({{ value_column }} > 0 AND {{ value_column }} < 'inf'::FLOAT)
            THEN 'Non-positive or non-finite result'
        WHEN {{ value_column }} < {{ validity_range[0] }} THEN 'Below valid range'
        WHEN {{ value_column }} > {{ validity_range[1] }} THEN 'Above valid range'
        ELSE 'Within valid range'
    END AS plausibility_status,
    -- Recorded unit labels can be wrong even when conversion yields a plausible number.
    (unit_status IN ('Missing unit', 'Unsupported unit', 'Converted', 'Mislabelled standard unit')
        OR is_unit_metadata_conflict
        OR plausibility_status <> 'Within valid range') AS is_lipid_review_required,
    concept_code,
    concept_display,
    source_cluster_id,
    sampling_context
FROM conversion
{% endmacro %}
