{{ config(materialized='view') }}
SELECT person_id, indicator_id, current_practice_code, latest_record_date, is_in_numerator, indicator_status,
    latest_asm_date, latest_oral_patch_date, latest_ehc_date, latest_non_specific_advice_date, latest_written_advice_date, latest_verbal_advice_date, has_unknown_modality_advice
FROM {{ ref('fct_person_contraception_nice_indicators') }}
