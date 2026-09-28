-- NICE IND229 and IND274 explicitly exclude people aged 24 and under or 85 and over.
SELECT person_id, indicator_id, age
FROM {{ ref('fct_person_lipid_lowering_therapy_ind229') }}
WHERE age IS NULL OR age NOT BETWEEN 25 AND 84

UNION ALL

SELECT person_id, indicator_id, age
FROM {{ ref('fct_person_lipid_lowering_therapy_ind274') }}
WHERE age IS NULL OR age NOT BETWEEN 25 AND 84
