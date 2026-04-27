{{
    config(
        materialized='table',
        cluster_by=['person_id', 'episode_start'])
}}

/*
Pregnancy episodes constructed from int_pregnancy_observations_all.

Episode logic:
  - episode_start = first pregnancy code that is not preceded by an open episode.
  - A new episode begins when a pregnancy code follows either a delivery/loss code
    or a gap longer than pregnancy_episode_max_weeks from the prior observation.
  - episode_end = earliest subsequent delivery/loss code, else
    episode_start + pregnancy_episode_max_weeks (fallback).
  - episode_end is capped at episode_start + pregnancy_episode_max_weeks even when
    an outcome code exists, guarding against mis-dated outcome records.

One row per pregnancy episode per person. Used by exclusion-window and cohort models.
*/

WITH preg_obs AS (

    SELECT
        person_id,
        clinical_effective_date,
        CASE
            WHEN is_pregnancy_code THEN 'pregnancy'
            WHEN is_delivery_outcome_code AND code_type = 'delivery' THEN 'delivery'
            WHEN is_delivery_outcome_code AND code_type = 'pregnancy_loss' THEN 'pregnancy_loss'
        END AS event_type
    FROM {{ ref('int_pregnancy_observations_all') }}
    WHERE is_pregnancy_code OR is_delivery_outcome_code

),

preg_obs_dedup AS (

    SELECT DISTINCT
        person_id,
        clinical_effective_date,
        event_type
    FROM preg_obs
    WHERE event_type IS NOT NULL

),

with_context AS (

    SELECT
        person_id,
        clinical_effective_date,
        event_type,
        LAG(clinical_effective_date) OVER (
            PARTITION BY person_id ORDER BY clinical_effective_date, event_type
        ) AS prev_date,
        LAG(event_type) OVER (
            PARTITION BY person_id ORDER BY clinical_effective_date, event_type
        ) AS prev_event_type
    FROM preg_obs_dedup

),

episode_flags AS (

    SELECT
        person_id,
        clinical_effective_date,
        event_type,
        CASE
            WHEN event_type = 'pregnancy' AND (
                prev_date IS NULL
                OR prev_event_type IN ('delivery', 'pregnancy_loss')
                OR DATEDIFF(
                    'day', prev_date, clinical_effective_date
                ) > {{ pregnancy_episode_max_weeks() }} * 7
            ) THEN 1
            ELSE 0
        END AS is_new_episode
    FROM with_context

),

episode_assignment AS (

    SELECT
        person_id,
        clinical_effective_date,
        event_type,
        SUM(is_new_episode) OVER (
            PARTITION BY person_id
            ORDER BY clinical_effective_date, event_type
            ROWS UNBOUNDED PRECEDING
        ) AS episode_number
    FROM episode_flags

),

episodes_agg AS (

    SELECT
        person_id,
        episode_number,
        MIN(clinical_effective_date) AS episode_start,
        MIN(
            CASE WHEN event_type = 'delivery' THEN clinical_effective_date END
        ) AS delivery_date,
        MIN(
            CASE WHEN event_type = 'pregnancy_loss' THEN clinical_effective_date END
        ) AS loss_date
    FROM episode_assignment
    WHERE episode_number >= 1
    GROUP BY person_id, episode_number

),

episodes_resolved AS (

    SELECT
        person_id,
        episode_number,
        episode_start,
        delivery_date,
        loss_date,
        CASE
            WHEN delivery_date IS NULL AND loss_date IS NULL THEN NULL
            WHEN loss_date IS NULL THEN delivery_date
            WHEN delivery_date IS NULL THEN loss_date
            WHEN delivery_date <= loss_date THEN delivery_date
            ELSE loss_date
        END AS outcome_date,
        CASE
            WHEN delivery_date IS NULL AND loss_date IS NULL
                THEN 'fallback_max_duration'
            WHEN loss_date IS NULL OR delivery_date <= loss_date
                THEN 'delivery'
            ELSE 'pregnancy_loss'
        END AS outcome_type,
        DATEADD(
            'week',
            {{ pregnancy_episode_max_weeks() }},
            episode_start
        ) AS max_episode_end
    FROM episodes_agg

)

SELECT
    person_id,
    episode_number AS episode_id,
    episode_start,
    LEAST(
        COALESCE(outcome_date, max_episode_end),
        max_episode_end
    ) AS episode_end,
    outcome_type,
    delivery_date,
    loss_date

FROM episodes_resolved

ORDER BY person_id, episode_start
