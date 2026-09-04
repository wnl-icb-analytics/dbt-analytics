{{
    config(
        materialized='view',
        tags=['qof', 'pit', 'reporting']
    )
}}

/*
COPD Register - Point in Time (PIT)

QOF v51 Business Logic (via calculate_copd_register macro):
- Disorder evidence counts at any date; administrative evidence counts only in the preceding two years
- Rule 1: EUNRESCOPD_DAT < 01/04/2023 → automatic inclusion
- Rule 2: EUNRESCOPD_DAT >= 01/04/2023 + spirometry <0.7 within timeframe
- Rule 3: Newly registered + spirometry within timeframe
- Rule 4: All remaining post-April 2023 patients (no SPIRPU_COD required)
*/

WITH register_data AS (
    {{ calculate_copd_register(reference_date_expr=get_reference_date()) }}
)

SELECT
    person_id,
    register_name,
    is_on_register,
    {{ get_reference_date() }} AS reference_date
FROM register_data
