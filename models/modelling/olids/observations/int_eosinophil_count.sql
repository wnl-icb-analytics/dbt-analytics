{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All blood eosinophil count observations with unit standardisation and data quality flags.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Uses the standardise_count_observation macro: values inside the absolute range are kept
as-is; conversions are attempted only when a value is implausibly high; nothing is dropped
(out-of-range values pass through, carried by flags).
Standard unit: 10*9/L (billion per liter).
*/

WITH raw_observations AS (
    SELECT *
    FROM ({{ get_observations("'EOS_COUNT'") }})
),

deduplicated AS (
    {{ deduplicate_table(
        table='raw_observations',
        partition_cols=['person_id', 'clinical_effective_date', 'result_value', 'result_unit_code', 'mapped_concept_code'],
        order_cols=['date_recorded', 'id']
    ) }}
),

base_observations AS (
    SELECT
        id,
        person_id,
        clinical_effective_date,
        result_value,
        result_unit_code,
        result_unit_display,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS code_description,
        cluster_id AS source_cluster_id
    FROM deduplicated
    WHERE clinical_effective_date IS NOT NULL
      AND clinical_effective_date <= CURRENT_DATE()
      AND result_value IS NOT NULL
),

{{ standardise_count_observation(
    base_cte='base_observations',
    measurement='eosinophil_count',
    value_column='result_value'
) }}

SELECT
    id,
    person_id,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,
    original_result_value,
    original_result_unit_display,
    original_result_unit_code,
    'COUNT' AS expected_measurement_type,
    inferred_unit,
    inferred_value,
    value_was_converted,
    CASE WHEN inferred_unit IS NULL THEN NULL
         ELSE inferred_unit IS DISTINCT FROM original_result_unit_code
    END AS unit_was_changed,
    conversion_reason,
    confidence,
    is_negative,
    is_extreme_outlier,
    CASE
        WHEN inferred_value IS NULL THEN 'Abnormal'
        WHEN inferred_value < 0 THEN 'Abnormal'
        WHEN inferred_value < 0.04 THEN 'Eosinopenia'
        WHEN inferred_value <= 0.5 THEN 'Normal'
        WHEN inferred_value <= 1.5 THEN 'Eosinophilia'
        WHEN inferred_value <= 5.0 THEN 'Hypereosinophilia'
        WHEN inferred_value <= 100 THEN 'Severe Hypereosinophilia'
        ELSE 'Abnormal'
        -- bounds taken from:
            -- https://b-s-h.org.uk/guidelines/guidelines/investigation-and-management-of-eosinophilia
            -- https://www.rightdecisions.scot.nhs.uk/tam-treatments-and-medicines-nhs-highland/adult-therapeutic-guidelines/haematology/eosinophilia-guidelines/
    END AS eosinophil_category
FROM flagged
