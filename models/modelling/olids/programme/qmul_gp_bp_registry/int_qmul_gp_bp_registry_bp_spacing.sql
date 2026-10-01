{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

{#- A zero gap lets the ASOF join match a reading to itself and the chain never ends;
    the final-reading rule needs a reading before the last one. -#}
{%- if qmul_gp_bp_registry_min_reading_gap_weeks() | trim | int < 1
    or qmul_gp_bp_registry_min_qualifying_readings() | trim | int < 2 -%}
    {{ exceptions.raise_compiler_error(
        'qmul_gp_bp_registry_min_reading_gap_weeks must be >= 1 and qmul_gp_bp_registry_min_qualifying_readings >= 2'
    ) }}
{%- endif %}

/*
Per-person assessment of the BP spacing inclusion criterion:
"BP recorded on >= 4 separate dates >= 4 weeks apart over at least 36 consecutive months."

Method:
  - Rank each person's distinct eligible reading dates.
  - Precompute next_rank for every reading: the rank of the earliest subsequent
    reading at least min_reading_gap_weeks later. Non-recursive; uses an
    ASOF join.
  - Recursive CTE then walks the chain by jumping reading_rank -> next_rank;
    the recursive term does no window aggregation (Snowflake restriction).
  - The greedy chain gives the earliest possible date for each position, so a
    qualifying set exists exactly when some reading falls on or after both
    the first reading + min_span_months (default 36) calendar months and the
    chain's reading min_qualifying_readings - 1 (default 3rd) + min_reading_gap_weeks.
    That reading need not be on the greedy chain: a chain reading just short
    of the span can block a later reading that completes it.
  - qualifying_reading_count and span_* describe the whole greedy chain.

One row per person in the base population that has at least one eligible reading.
*/

WITH RECURSIVE eligible AS (

    SELECT
        person_id,
        effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id ORDER BY effective_date
        ) AS reading_rank
    FROM (
        SELECT DISTINCT person_id, effective_date
        FROM {{ ref('int_qmul_gp_bp_registry_bp_readings_eligible') }}
    )

),

next_jump AS (

    -- For every reading, find the rank of the earliest next reading
    -- that is at least min_reading_gap_weeks later. NULL when no such reading exists.
    SELECT
        c.person_id,
        c.reading_rank AS current_rank,
        n.reading_rank AS next_rank
    FROM (
        SELECT
            person_id,
            reading_rank,
            DATEADD('week', {{ qmul_gp_bp_registry_min_reading_gap_weeks() }}, effective_date) AS min_next_date
        FROM eligible
    ) c
    ASOF JOIN eligible n
        MATCH_CONDITION (c.min_next_date <= n.effective_date)
        ON c.person_id = n.person_id

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

summary AS (

    SELECT
        person_id,
        MAX(chain_position) AS qualifying_reading_count,
        MIN(effective_date) AS span_start,
        MAX(effective_date) AS span_end,
        DATEDIFF('day', MIN(effective_date), MAX(effective_date)) AS span_days,
        -- The final reading must be min_span_months after the first reading and
        -- min_reading_gap_weeks after the reading before it. NULL when the chain
        -- is too short to supply that reading.
        GREATEST(
            DATEADD('month', {{ qmul_gp_bp_registry_min_span_months() }}, MIN(effective_date)),
            DATEADD(
                'week',
                {{ qmul_gp_bp_registry_min_reading_gap_weeks() }},
                MAX(
                    CASE
                        WHEN chain_position = {{ qmul_gp_bp_registry_min_qualifying_readings() }} - 1
                            THEN effective_date
                    END
                )
            )
        ) AS earliest_final_reading_date
    FROM chain
    GROUP BY person_id

),

final_reading AS (

    SELECT
        s.person_id,
        MIN(e.effective_date) AS inclusion_first_met_date
    FROM summary s
    INNER JOIN eligible e
        ON e.person_id = s.person_id
        AND e.effective_date >= s.earliest_final_reading_date
    GROUP BY s.person_id

)

SELECT
    s.person_id,
    s.qualifying_reading_count,
    s.span_start,
    s.span_end,
    s.span_days,
    f.inclusion_first_met_date,
    f.inclusion_first_met_date IS NOT NULL AS meets_bp_criteria
FROM summary s
LEFT JOIN final_reading f
    ON s.person_id = f.person_id
