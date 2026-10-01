-- The family and snapshot input retain IND226's primary and booster evidence.
WITH family AS (
    SELECT person_id, indicator_id, primary_doses_by_12_months,
        booster_doses_12_to_18_months
    FROM {{ ref('fct_person_childhood_immunisation_nice_indicators') }}
),
snapshot_input AS (
    SELECT person_id, indicator_id, primary_doses_by_12_months,
        booster_doses_12_to_18_months
    FROM {{ ref('fct_person_childhood_immunisation_nice_indicators_snapshot_input') }}
),
measure AS (
    SELECT person_id, indicator_id, primary_doses_by_12_months,
        booster_doses_12_to_18_months
    FROM {{ ref('fct_person_childhood_immunisation_ind226') }}
)

SELECT family.person_id, family.indicator_id, 'family' AS failed_interface
FROM family
LEFT JOIN measure
    ON family.person_id = measure.person_id
    AND family.indicator_id = measure.indicator_id
WHERE family.primary_doses_by_12_months IS DISTINCT FROM measure.primary_doses_by_12_months
    OR family.booster_doses_12_to_18_months IS DISTINCT FROM measure.booster_doses_12_to_18_months

UNION ALL

SELECT family.person_id, family.indicator_id, 'snapshot_input' AS failed_interface
FROM family
LEFT JOIN snapshot_input
    ON family.person_id = snapshot_input.person_id
    AND family.indicator_id = snapshot_input.indicator_id
WHERE snapshot_input.person_id IS NULL
    OR family.primary_doses_by_12_months IS DISTINCT FROM snapshot_input.primary_doses_by_12_months
    OR family.booster_doses_12_to_18_months IS DISTINCT FROM snapshot_input.booster_doses_12_to_18_months
