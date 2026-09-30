-- Each row is a distinct medication count period, so a period must not run
-- straight on from one with the same count.
SELECT
    person_id,
    valid_from
FROM {{ ref('int_polypharmacy_history') }}
QUALIFY medication_count = LAG(medication_count) OVER (PARTITION BY person_id ORDER BY valid_from)
    AND valid_from = DATEADD(day, 1, LAG(valid_to) OVER (PARTITION BY person_id ORDER BY valid_from))
