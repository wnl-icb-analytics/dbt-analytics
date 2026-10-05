{{ config(materialized='view') }}

-- Core segment fields for prospective SCD2 change tracking: the segment
-- assignment, population state and cohort membership.
-- Age, general demographics and the drivers behind each cohort (LTC counts
-- and criterion flags) are excluded so they do not open extra versions.
-- Cohort membership can still change because a rolling 12-month activity
-- window moved rather than because new evidence was recorded.

SELECT
    person_id,
    segment_number,
    segment_name,
    is_active,
    is_deceased,
    is_end_of_life,
    is_complex_adult,
    has_multiple_ltcs,
    has_single_ltc,
    is_complex_child,
    has_child_health_needs
FROM {{ ref('fct_person_segment') }}
