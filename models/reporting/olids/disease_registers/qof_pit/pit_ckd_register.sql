{{
    config(
        materialized='view',
        tags=['qof', 'pit', 'reporting']
    )
}}

/*
CKD Register - Point in Time (PIT)

QOF v51 Business Logic (via calculate_ckd_register macro):
- Age ≥18 at reference date
- Has CKD Stage 3-5 diagnosis (CKD_COD)
- NOT downstaged to Stage 1-2 (CKD1AND2_COD) after latest Stage 3-5
- NOT resolved (CKDRES_COD) after latest Stage 3-5
*/

WITH register_data AS (
    {{ calculate_ckd_register(reference_date_expr=get_reference_date()) }}
)

SELECT
    person_id,
    register_name,
    is_on_register,
    {{ get_reference_date() }} AS reference_date
FROM register_data
