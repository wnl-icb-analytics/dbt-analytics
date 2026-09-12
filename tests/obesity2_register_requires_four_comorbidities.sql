SELECT
    person_id,
    comorbidity_count
FROM {{ ref('fct_person_obesity2_register') }}
WHERE comorbidity_count < 4 OR comorbidity_count > 5
