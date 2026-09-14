-- NICE counts delivered intervention within three calendar months after the positive screen.
SELECT screening_observation_id, source_cluster_id, screen_date, latest_intervention_date
FROM {{ ref('int_nice_alcohol_screen_intervention') }}
WHERE latest_intervention_date < screen_date
    OR latest_intervention_date > DATEADD(month, 3, screen_date)
    OR latest_intervention_date > CURRENT_DATE()
