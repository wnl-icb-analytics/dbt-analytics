-- The expanded observation feed copies each referral request under a new ID.
-- Only the referral-request row should remain when both are present.
SELECT
    e.person_id,
    e.araf_referral_ID
FROM {{ ref('int_valproate_araf_referral_events') }} AS e
INNER JOIN {{ ref('stg_olids_referral_request') }} AS r
    ON e.araf_referral_ID = r.observation_id
INNER JOIN {{ ref('int_valproate_araf_referral_events') }} AS kept
    ON kept.araf_referral_ID = r.id
