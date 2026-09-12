{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Per-person assessment of the BP spacing inclusion criterion:
"BP recorded on >= 4 separate dates >= 4 weeks apart over at least 36 consecutive months."

Method:
  - Rank each person's eligible readings by date.
  - Precompute next_rank for every reading: the rank of the earliest subsequent
    reading at least min_reading_gap_weeks later. Non-recursive; uses a
    non-equi self-join with MIN aggregation.
  - Recursive CTE then walks the chain by jumping reading_rank -> next_rank;
    the recursive term does no window aggregation (Snowflake restriction).
  - A person meets the criterion when the chain reaches
    min_qualifying_readings (default 4) and the span from first to last
    reading in the chain is at least min_span_months (default 36).

One row per person in the base population that has at least one eligible reading.
*/

WITH RECURSIVE eligible AS (

    SELECT
        person_id,
        effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id ORDER BY effective_date
        ) AS reading_rank
    FROM {{ ref('int_qmul_gp_bp_registry_bp_readings_eligible') }}

),

next_jump AS (

    -- For every reading, find the rank of the earliest next reading
    -- that is at least min_reading_gap_weeks later. NULL when no such reading exists.
    SELECT
        c.person_id,
        c.reading_rank AS current_rank,
        MIN(n.reading_rank) AS next_rank
    FROM eligible c
    LEFT JOIN eligible n
        ON n.person_id = c.person_id
        AND n.reading_rank > c.reading_rank
        AND n.effective_date >= DATEADD(
            'week',
            {{ qmul_gp_bp_registry_min_reading_gap_weeks() }},
            c.effective_date
        )
    GROUP BY c.person_id, c.reading_rank

),

chain AS (

    -- Anchor: earliest reading per person (rank 1)
    SELECT
        person_id,
        reading_rank,
        effective_date,
        1 AS chain_position
    FROM eligible
    WHERE reading_rank = 1

    UNION ALL

    -- Recursive step: jump to next_rank via the precomputed next_jump table.
    -- No window functions here - Snowflake requirement.
    SELECT
        e.person_id,
        e.reading_rank,
        e.effective_date,
        c.chain_position + 1
    FROM chain c
    INNER JOIN next_jump nj
        ON nj.person_id = c.person_id
        AND nj.current_rank = c.reading_rank
    INNER JOIN eligible e
        ON e.person_id = c.person_id
        AND e.reading_rank = nj.next_rank
    WHERE nj.next_rank IS NOT NULL

),

chain_with_anchor AS (

    -- Tag every chain step with the chain's first reading date so we can
    -- pick out the earliest step that satisfies the full inclusion rule
    -- (>= min_qualifying_readings AND span from chain start >= min_span_months).
    SELECT
        c.person_id,
        c.reading_rank,
        c.effective_date,
        c.chain_position,
        MIN(c.effective_date) OVER (PARTITION BY c.person_id) AS chain_anchor_date
    FROM chain c

),

summary AS (

    SELECT
        person_id,
        MAX(chain_position) AS qualifying_reading_count,
        MIN(effective_date) AS span_start,
        MAX(effective_date) AS span_end,
        DATEDIFF('day', MIN(effective_date), MAX(effective_date)) AS span_days,
        MIN(
            CASE
                WHEN chain_position >= {{ qmul_gp_bp_registry_min_qualifying_readings() }}
                 AND DATEDIFF('day', chain_anchor_date, effective_date)
                     >= {{ qmul_gp_bp_registry_min_span_months() }} * 30
                THEN effective_date
            END
        ) AS inclusion_first_met_date
    FROM chain_with_anchor
    GROUP BY person_id

)

SELECT
    person_id,
    qualifying_reading_count,
    span_start,
    span_end,
    span_days,
    inclusion_first_met_date,
    inclusion_first_met_date IS NOT NULL AS meets_bp_criteria
FROM summary
