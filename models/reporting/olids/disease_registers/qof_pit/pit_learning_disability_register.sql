{{
    config(
        materialized='view',
        tags=['qof', 'pit', 'reporting']
    )
}}

/*
Learning Disability Register - Point in Time (PIT)

QOF v51 Business Logic (via calculate_learning_disability_register macro):
- Has learning disability diagnosis (LD_COD)
- NOT excluded (LDREM_COD) after latest diagnosis
- No age restriction (all ages included per QOF v51)
*/

WITH register_data AS (
    {{ calculate_learning_disability_register(reference_date_expr=get_reference_date()) }}
)

SELECT
    person_id,
    register_name,
    is_on_register,
    {{ get_reference_date() }} AS reference_date
FROM register_data
