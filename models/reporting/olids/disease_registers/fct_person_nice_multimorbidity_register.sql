{{ config(materialized='table', cluster_by=['person_id']) }}

WITH population AS (
    {{ nice_reference_population('current') }}
), categories AS (
    SELECT person_id, reporting_date,
        COUNT(DISTINCT category_code) AS category_count,
        BOOLOR_AGG(category_code = 'CANCER') AS has_cancer,
        BOOLOR_AGG(category_code = 'CHRONIC_PAIN') AS has_chronic_pain,
        BOOLOR_AGG(category_code = 'CIRCULATORY') AS has_circulatory,
        BOOLOR_AGG(category_code = 'DIABETES') AS has_diabetes,
        BOOLOR_AGG(category_code = 'DIGESTIVE') AS has_digestive,
        BOOLOR_AGG(category_code = 'LEARNING_DISABILITY') AS has_learning_disability,
        BOOLOR_AGG(category_code = 'MENTAL_HEALTH') AS has_mental_health,
        BOOLOR_AGG(category_code = 'MUSCULOSKELETAL') AS has_musculoskeletal,
        BOOLOR_AGG(category_code = 'NEUROLOGICAL') AS has_neurological,
        BOOLOR_AGG(category_code = 'RENAL') AS has_renal,
        BOOLOR_AGG(category_code = 'RESPIRATORY') AS has_respiratory
    FROM {{ ref('int_nice_multimorbidity_categories') }}
    GROUP BY person_id, reporting_date
)
SELECT population.person_id, population.reporting_date, population.age, population.practice_code,
    TRUE AS is_on_register, categories.category_count,
    categories.has_cancer, categories.has_chronic_pain, categories.has_circulatory,
    categories.has_diabetes, categories.has_digestive, categories.has_learning_disability,
    categories.has_mental_health, categories.has_musculoskeletal, categories.has_neurological,
    categories.has_renal, categories.has_respiratory
FROM population
INNER JOIN categories ON population.person_id = categories.person_id
    AND population.reporting_date = categories.reporting_date
WHERE population.age >= 18 AND categories.category_count >= 4
