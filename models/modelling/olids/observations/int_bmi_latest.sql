{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid BMI measurement per person with ethnicity-adjusted categorisation.
Uses the int_bmi_all model and filters to most recent valid BMI.
Same-day ties: latest entry time, then higher value (entries saved together are
usually near-duplicates), then id so the row is stable between builds. Invalid
readings are removed before ranking, so the higher value is always valid.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    date_recorded,
    bmi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    result_value,
    bmi_source,

    -- Age information
    age,

    -- Ethnicity information
    requires_lower_bmi_thresholds,
    cardiometabolic_risk_ethnicity_group,

    -- Validation
    is_valid_bmi,

    -- BMI categorisation (ethnicity-adjusted)
    bmi_category,

    -- BMI risk sort key (ethnicity-adjusted)
    bmi_risk_sort_key

FROM {{ ref('int_bmi_all') }}
WHERE is_valid_bmi = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, date_recorded DESC, bmi_value DESC, ID DESC
) = 1
