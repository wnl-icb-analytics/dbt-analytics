{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid waist circumference measurement per person.
The latest circumference may have no paired height or assessable ratio band.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    date_recorded,
    waist_circumference_value,
    concept_code,
    concept_display,
    source_cluster_id,
    height_cm,
    height_measurement_date,
    bmi_value_at_waist,
    bmi_measurement_date,
    waist_to_height_ratio,
    waist_to_height_ratio_category,
    original_result_value,
    original_result_unit_display

FROM {{ ref('int_waist_circumference_all') }}

WHERE is_valid_waist_circumference = TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date::DATE DESC, date_recorded DESC NULLS LAST, id DESC
) = 1
