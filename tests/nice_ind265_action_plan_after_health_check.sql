-- NICE IND265 and IND266 count a learning disability health action plan only
-- when it is dated on or after the health check (NCD HI03 rule). A numerator
-- row whose plan precedes its check, or has no check, breaks that rule.
SELECT person_id, indicator_id, latest_review_date, latest_health_action_plan_date
FROM {{ ref('fct_person_ltc_review_nice_indicators') }}
WHERE indicator_id IN ('IND265', 'IND266')
    AND is_in_numerator
    AND (latest_health_action_plan_date < latest_review_date OR latest_review_date IS NULL)
