{{
    config(
        materialized='table',
        cluster_by=['person_id', 'window_start'])
}}

/*
Unified exclusion windows for hypertensive-disorders-of-pregnancy (HDP) research use.
Combines pregnancy episodes and HDP diagnosis codes into one window table so downstream
models can filter observations (e.g. BP readings) with a single join.

Window sources:
  - pregnancy: an episode from int_pregnancy_episodes_all with no HDP code inside it.
  - pregnancy_hdp: an episode that contains at least one HDP code; episode_end extended
    by hdp_postpartum_extension_weeks to cover persistent HDP-related BP after delivery.
  - hdp_standalone: an HDP code with no linked pregnancy episode; window runs from the
    code date forward by hdp_postpartum_extension_weeks.

One row per window per person.
*/

WITH episodes AS (

    SELECT
        person_id,
        episode_id,
        episode_start,
        episode_end,
        outcome_type
    FROM {{ ref('int_pregnancy_episodes_all') }}

),

hdp_codes AS (

    SELECT
        person_id,
        clinical_effective_date AS hdp_code_date
    FROM {{ ref('int_hypertensive_disorders_of_pregnancy_all') }}

),

-- Flag each episode as having an HDP code inside it
episode_hdp_flag AS (

    SELECT
        e.person_id,
        e.episode_id,
        e.episode_start,
        e.episode_end,
        e.outcome_type,
        MAX(
            CASE
                WHEN h.hdp_code_date BETWEEN e.episode_start AND e.episode_end
                    THEN 1 ELSE 0
            END
        ) AS has_hdp_code
    FROM episodes e
    LEFT JOIN hdp_codes h
        ON e.person_id = h.person_id
    GROUP BY
        e.person_id, e.episode_id, e.episode_start, e.episode_end, e.outcome_type

),

pregnancy_windows AS (

    SELECT
        person_id,
        episode_start AS window_start,
        CASE
            WHEN has_hdp_code = 1
                THEN DATEADD(
                    'week',
                    {{ hdp_postpartum_extension_weeks() }},
                    episode_end
                )
            ELSE episode_end
        END AS window_end,
        CASE
            WHEN has_hdp_code = 1 THEN 'pregnancy_hdp'
            ELSE 'pregnancy'
        END AS reason
    FROM episode_hdp_flag

),

-- HDP codes not already inside any pregnancy episode
standalone_hdp_codes AS (

    SELECT h.person_id, h.hdp_code_date
    FROM hdp_codes h
    LEFT JOIN episodes e
        ON h.person_id = e.person_id
        AND h.hdp_code_date BETWEEN e.episode_start AND e.episode_end
    WHERE e.episode_id IS NULL

),

standalone_windows AS (

    SELECT
        person_id,
        hdp_code_date AS window_start,
        DATEADD(
            'week',
            {{ hdp_postpartum_extension_weeks() }},
            hdp_code_date
        ) AS window_end,
        'hdp_standalone' AS reason
    FROM standalone_hdp_codes

),

all_windows AS (

    SELECT person_id, window_start, window_end, reason FROM pregnancy_windows
    UNION ALL
    SELECT person_id, window_start, window_end, reason FROM standalone_windows

)

SELECT
    person_id,
    ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY window_start, window_end
    ) AS window_id,
    window_start,
    window_end,
    reason

FROM all_windows

ORDER BY person_id, window_start
