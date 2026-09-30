{{
    config(
        materialized='table',
        cluster_by=['person_id', 'valid_from'],
        tags=['polypharmacy']
    )
}}

/*
Historical polypharmacy medication counts as SCD Type 2 (last 5 years).

Event-based slowly changing dimension tracking polypharmacy medication burden
over time. Only creates new rows when medication count changes, providing
efficient storage and accurate historical tracking.

Example output for a patient:
- 2020-01-15 to 2021-08-10: 4 medications (not polypharmacy)
- 2021-08-11 to 2023-03-05: 5 medications (polypharmacy starts)
- 2023-03-06 to 2026-11-02: 6 medications (current; valid_to is always
  set, and periods can start in the future as current issues run out)

Grain: One row per person per distinct medication count period
*/

WITH repeat_prescription_codes AS (
    -- Get SNOMED codes for repeat prescriptions from cluster
    SELECT DISTINCT code
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE cluster_id = 'REPEAT_PRESCRIPTION'
),

medication_periods AS (
    -- Get all repeat medications in polypharmacy scope for last 5 years
    SELECT
        pp.person_id,
        mo.mapped_concept_code,
        mo.clinical_effective_date AS start_date,
        DATEADD(day, COALESCE(mo.duration_days, 28), mo.clinical_effective_date) AS end_date
    FROM {{ ref('stg_olids_medication_order') }} mo
    INNER JOIN {{ ref('stg_olids_medication_statement') }} ms
        ON mo.medication_statement_id = ms.id
    INNER JOIN repeat_prescription_codes rpc
        ON ms.authorisation_type_code = rpc.code
    INNER JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    INNER JOIN {{ ref('int_polypharmacy_medications_list') }} bnf
        ON mo.mapped_concept_code = bnf.snomed_code
    WHERE mo.clinical_effective_date >= DATEADD(year, -5, CURRENT_DATE())
        AND mo.clinical_effective_date IS NOT NULL
        AND mo.mapped_concept_code IS NOT NULL
),

medication_events AS (
    -- Generate start and end events for each medication period
    SELECT
        person_id,
        mapped_concept_code,
        start_date AS event_date
    FROM medication_periods

    UNION ALL

    SELECT
        person_id,
        mapped_concept_code,
        DATEADD(day, 1, end_date) AS event_date
    FROM medication_periods
),

distinct_event_dates AS (
    -- Get unique dates where medication count could have changed
    SELECT DISTINCT
        person_id,
        event_date
    FROM medication_events
    WHERE event_date >= DATEADD(year, -5, CURRENT_DATE())
),

medication_counts_by_date AS (
    -- Count distinct medications active on each event date
    SELECT
        ded.person_id,
        ded.event_date,
        COUNT(DISTINCT mp.mapped_concept_code) AS medication_count
    FROM distinct_event_dates ded
    LEFT JOIN medication_periods mp
        ON ded.person_id = mp.person_id
        AND ded.event_date BETWEEN mp.start_date AND mp.end_date
    GROUP BY ded.person_id, ded.event_date
),

scd_with_lag AS (
    -- Add LAG to detect changes
    SELECT
        person_id,
        event_date,
        medication_count,
        LAG(medication_count) OVER (
            PARTITION BY person_id
            ORDER BY event_date
        ) AS prev_medication_count,
        LAG(event_date) OVER (
            PARTITION BY person_id
            ORDER BY event_date
        ) AS prev_event_date
    FROM medication_counts_by_date
),

scd_smoothed AS (
    -- Only keep changes that last 3+ days or are the first/last event
    SELECT
        person_id,
        event_date,
        medication_count,
        prev_medication_count,
        LEAD(event_date) OVER (
            PARTITION BY person_id
            ORDER BY event_date
        ) AS next_event_date
    FROM scd_with_lag
    WHERE medication_count != prev_medication_count
        OR prev_medication_count IS NULL
),

scd_stable AS (
    -- Stable change points (3+ days or final period)
    SELECT
        person_id,
        event_date,
        medication_count
    FROM scd_smoothed
    WHERE DATEDIFF(day, event_date, COALESCE(next_event_date, CURRENT_DATE())) >= 3
        OR next_event_date IS NULL  -- Keep the final period
),

scd_merged AS (
    -- Dropping a short dip leaves the points either side with the same count;
    -- keep the first of each run so every row is a distinct count period.
    SELECT
        person_id,
        event_date,
        medication_count
    FROM scd_stable
    QUALIFY medication_count IS DISTINCT FROM LAG(medication_count) OVER (
        PARTITION BY person_id
        ORDER BY event_date
    )
),

scd_compressed AS (
    SELECT
        person_id,
        event_date AS valid_from,
        LEAD(DATEADD(day, -1, event_date)) OVER (
            PARTITION BY person_id
            ORDER BY event_date
        ) AS valid_to,
        medication_count,
        medication_count >= 5 AS is_polypharmacy_5plus,
        medication_count >= 10 AS is_polypharmacy_10plus
    FROM scd_merged
)

SELECT
    person_id,
    medication_count,
    valid_from,
    valid_to,
    is_polypharmacy_5plus,
    is_polypharmacy_10plus,

    -- Current record flag (period overlaps with today)
    CASE
        WHEN valid_from <= CURRENT_DATE()
            AND (valid_to IS NULL OR valid_to >= CURRENT_DATE())
        THEN TRUE
        ELSE FALSE
    END AS is_current,

    -- Duration of this period in days
    DATEDIFF(
        day,
        valid_from,
        COALESCE(valid_to, CURRENT_DATE())
    ) AS period_duration_days

FROM scd_compressed
WHERE medication_count > 0  -- Only include periods with at least one medication
ORDER BY person_id, valid_from
