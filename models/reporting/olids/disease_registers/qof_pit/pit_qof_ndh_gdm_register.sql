{{
    config(
        materialized='view',
        tags=['qof', 'pit', 'reporting']
    )
}}

/*
NDH register as of reference date.

Reference date logic (via get_reference_date() macro):
- Defaults to CURRENT_DATE()
- Can be overridden with --vars '{"qof_reference_date": "2024-03-31"}'

Output: person_id, register_name, is_on_register, reference_date
*/

WITH register_data AS (
    {{ calculate_qof_ndh_gdm_register(reference_date_expr=get_reference_date()) }}
)

SELECT
    person_id,
    register_name,
    is_on_register,
    {{ get_reference_date() }} AS reference_date
FROM register_data
WHERE is_on_register = TRUE
