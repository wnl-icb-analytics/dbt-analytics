{{
    config(
        materialized='table',
        cluster_by=['person_id', 'episode_start'])
}}

/*
Pregnancy episodes constructed from int_pregnancy_observations_all.

Episode logic:
  - Observation dates ignore time of day. Same-day events are ordered pregnancy,
    delivery, then pregnancy loss.
  - A pregnancy code starts a new episode if not preceded by an open episode (no
    prior code, prior code was an outcome, or the episode's first pregnancy code
    was more than pregnancy_episode_max_weeks ago). The maximum date is inclusive.
  - A delivery / pregnancy_loss code starts an orphan episode only when no
    pregnancy code is recorded in the preceding pregnancy_episode_max_weeks.
    Repeated outcomes within that period join the episode; its outcome stays
    the earliest outcome. This catches outcomes where antenatal care was
    delivered outside primary care and only the delivery / loss notification
    reached the GP record. For these episodes, episode_start is back-dated
    by pregnancy_episode_max_weeks from the outcome date (estimated conception).
  - episode_end = earliest subsequent delivery / loss code in the episode, else
    episode_start + pregnancy_episode_max_weeks (fallback).
  - episode_end is capped at episode_start + pregnancy_episode_max_weeks even
    when an outcome code exists, guarding against mis-dated outcome records.

One row per pregnancy episode per person. Used by exclusion-window and cohort models.
*/

WITH RECURSIVE preg_obs AS (

    SELECT
        person_id,
        TO_DATE(clinical_effective_date) AS clinical_effective_date,
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
        event_type,
        CASE event_type WHEN 'pregnancy' THEN 1 WHEN 'delivery' THEN 2 WHEN 'pregnancy_loss' THEN 3 END AS event_priority
    FROM preg_obs
    WHERE event_type IS NOT NULL

),

with_context AS (

    SELECT
        person_id,
        clinical_effective_date,
        event_type,
        event_priority,
        ROW_NUMBER() OVER (
            PARTITION BY person_id ORDER BY clinical_effective_date, event_priority
        ) AS event_number,
        LAG(event_type) OVER (
            PARTITION BY person_id ORDER BY clinical_effective_date, event_priority
        ) AS prev_event_type,
        MAX(CASE WHEN event_type = 'pregnancy' THEN clinical_effective_date END) OVER (
            PARTITION BY person_id ORDER BY clinical_effective_date, event_priority
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_pregnancy_date
    FROM preg_obs_dedup

),

episode_flags AS (

    SELECT
        person_id,
        clinical_effective_date,
        event_type,
        event_number,
        CASE
            -- The duration limit is applied during assignment from the episode's first code.
            WHEN event_type = 'pregnancy' AND (
                prev_event_type IS NULL
                OR prev_event_type IN ('delivery', 'pregnancy_loss')
            ) THEN 1
            -- An outcome code starts an "orphan" episode when no recent pregnancy
            -- code precedes it (otherwise it just closes an existing episode)
            WHEN event_type IN ('delivery', 'pregnancy_loss') AND (
                prev_pregnancy_date IS NULL
                OR DATEDIFF(
                    'day', prev_pregnancy_date, clinical_effective_date
                ) > {{ pregnancy_episode_max_weeks() }} * 7
            ) THEN 1
            ELSE 0
        END AS is_new_episode
    FROM with_context

),

-- Carry the episode's first date: a chain of short gaps must not outlive its window.
episode_assignment (
    person_id, clinical_effective_date, event_type, event_number,
    episode_number, episode_first_event_date
) AS (

    SELECT
        person_id,
        clinical_effective_date,
        event_type,
        event_number,
        1 AS episode_number,
        clinical_effective_date AS episode_first_event_date
    FROM episode_flags
    WHERE event_number = 1

    UNION ALL

    SELECT
        e.person_id,
        e.clinical_effective_date,
        e.event_type,
        e.event_number,
        CASE
            WHEN e.is_new_episode = 1 OR (
                e.event_type = 'pregnancy'
                AND e.clinical_effective_date > DATEADD(
                    'week', {{ pregnancy_episode_max_weeks() }}, a.episode_first_event_date
                )
            ) THEN a.episode_number + 1
            ELSE a.episode_number
        END AS episode_number,
        CASE
            WHEN e.is_new_episode = 1 OR (
                e.event_type = 'pregnancy'
                AND e.clinical_effective_date > DATEADD(
                    'week', {{ pregnancy_episode_max_weeks() }}, a.episode_first_event_date
                )
            ) THEN e.clinical_effective_date
            ELSE a.episode_first_event_date
        END AS episode_first_event_date
    FROM episode_assignment a
    INNER JOIN episode_flags e
        ON e.person_id = a.person_id
        AND e.event_number = a.event_number + 1

),

episodes_agg AS (

    SELECT
        person_id,
        episode_number,
        MIN(
            CASE WHEN event_type = 'pregnancy' THEN clinical_effective_date END
        ) AS first_pregnancy_date,
        MIN(
            CASE WHEN event_type = 'delivery' THEN clinical_effective_date END
        ) AS delivery_date,
        MIN(
            CASE WHEN event_type = 'pregnancy_loss' THEN clinical_effective_date END
        ) AS loss_date,
        MIN(clinical_effective_date) AS earliest_event_date
    FROM episode_assignment
    WHERE episode_number >= 1
    GROUP BY person_id, episode_number

),

episodes_resolved AS (

    SELECT
        person_id,
        episode_number,
        first_pregnancy_date,
        delivery_date,
        loss_date,
        -- Earliest outcome (delivery or loss), if any
        CASE
            WHEN delivery_date IS NULL AND loss_date IS NULL THEN NULL
            WHEN loss_date IS NULL THEN delivery_date
            WHEN delivery_date IS NULL THEN loss_date
            WHEN delivery_date <= loss_date THEN delivery_date
            ELSE loss_date
        END AS outcome_date,
        CASE
            WHEN first_pregnancy_date IS NOT NULL
                 AND delivery_date IS NULL AND loss_date IS NULL
                THEN 'fallback_max_duration'
            WHEN first_pregnancy_date IS NULL
                 AND delivery_date IS NULL AND loss_date IS NULL
                THEN 'fallback_max_duration'
            WHEN loss_date IS NULL OR delivery_date <= loss_date
                THEN 'delivery'
            ELSE 'pregnancy_loss'
        END AS outcome_type,
        -- Episode anchor for orphan outcomes: back-date from the outcome
        COALESCE(
            first_pregnancy_date,
            DATEADD(
                'week',
                -1 * {{ pregnancy_episode_max_weeks() }},
                earliest_event_date
            )
        ) AS episode_start_raw
    FROM episodes_agg

)

SELECT
    person_id,
    episode_number AS episode_id,
    episode_start_raw AS episode_start,
    LEAST(
        COALESCE(outcome_date, DATEADD(
            'week',
            {{ pregnancy_episode_max_weeks() }},
            episode_start_raw
        )),
        DATEADD(
            'week',
            {{ pregnancy_episode_max_weeks() }},
            episode_start_raw
        )
    ) AS episode_end,
    outcome_type,
    delivery_date,
    loss_date,
    first_pregnancy_date IS NULL AS is_orphan_outcome_episode

FROM episodes_resolved

ORDER BY person_id, episode_start
