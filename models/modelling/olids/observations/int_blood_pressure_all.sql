{{
    config(
        materialized='table',
        tags=['intermediate', 'clinical', 'blood_pressure'],
        cluster_by=['person_id', 'effective_date'])
}}

/*
Blood Pressure Events - Event-based consolidation (one row per person per date)
===============================================================================
Aggregates from int_blood_pressure_observations_base.

Pairing (within-reading, not by date):
- Systolic and diastolic are separate observation rows. They are paired within
  a single reading via PARENT_OBSERVATION_ID (one parent holds both children in
  ~99.7% of cases), NOT by taking the max systolic and max diastolic of the
  date. The old date-level MAX/MAX approach fabricated a (max systolic, max
  diastolic) pair from two different readings on multi-reading days (~13% of
  person-dates), which pessimistically overstated uncontrolled BP downstream.
- Rows without a parent_observation_id (~7%) fall back to a single date-level
  pseudo-reading per context group (max systolic / max diastolic within that group).

Representative reading per date (lowest-of-day):
- clinical_effective_date carries no usable time-of-day (all OLIDS observations
  are stored at midnight/01:00), so "latest reading of the day" is impossible.
- One reading per person per date is selected by the lowest-systolic rule
  (lowest systolic, then lowest diastolic) - the conventional choice for BP
  control, discounting initial/white-coat elevated readings that are re-checked.
  systolic_value and diastolic_value are therefore a real, paired reading.

Flags:
- is_hypertensive_range is a date-level OR across ALL readings (any reading in
  hypertensive range on its own clinic/home threshold). Semantics are unchanged
  and independent of the representative selection.
- is_home_bp_event / is_abpm_bp_event describe the REPRESENTATIVE reading, so
  the value and its measurement context are on the same scale for downstream
  threshold selection.

Clinical validation:
- Both systolic and diastolic values present
- Systolic: 40-350 mmHg, Diastolic: 20-200 mmHg; systolic must exceed diastolic
*/

WITH valid_observations AS (
    SELECT *
    FROM {{ ref('int_blood_pressure_observations_base') }}
    WHERE effective_date IS NOT NULL
      -- Broad plausible value filter (inclusive, matching the typed ranges
      -- enforced in the readings HAVING clause: 40-350 / 20-200)
      AND result_value >= 20
      AND result_value <= 350
      AND effective_date <= CURRENT_DATE() -- Exclude future dates
),

-- Reading grain: parent_observation_id links the systolic + diastolic of one
-- reading; unparented rows form a pseudo-reading per date and context group.
reading_rows AS (
    SELECT
        *,
        COALESCE(
            parent_observation_id::VARCHAR,
            'NOPARENT:' || person_id::VARCHAR || ':' || effective_date::VARCHAR || ':' || bp_context_group
        ) AS reading_id
    FROM valid_observations
),

readings AS (
    SELECT
        person_id,
        effective_date,
        reading_id,
        CASE WHEN BOOLOR_AGG(bp_context_group = 'ABPM') THEN 'ABPM'
            WHEN BOOLOR_AGG(bp_context_group = 'HOME') THEN 'HOME'
            ELSE 'CLINIC'
        END AS bp_context_group,
        MAX(CASE WHEN is_systolic_row THEN result_value END) AS systolic_value,
        MAX(CASE WHEN is_diastolic_row THEN result_value END) AS diastolic_value,
        BOOLOR_AGG(is_home_bp_row) AS is_home_bp_reading,
        BOOLOR_AGG(is_abpm_bp_row) AS is_abpm_bp_reading,
        MAX(CASE WHEN is_systolic_row THEN id::VARCHAR END) AS systolic_observation_id,
        MAX(CASE WHEN is_diastolic_row THEN id::VARCHAR END) AS diastolic_observation_id
    FROM reading_rows
    GROUP BY person_id, effective_date, reading_id
    -- Valid paired reading within plausible ranges
    HAVING MAX(CASE WHEN is_systolic_row THEN result_value END) IS NOT NULL
       AND MAX(CASE WHEN is_diastolic_row THEN result_value END) IS NOT NULL
       AND MAX(CASE WHEN is_systolic_row THEN result_value END) >= 40
       AND MAX(CASE WHEN is_systolic_row THEN result_value END) <= 350
       AND MAX(CASE WHEN is_diastolic_row THEN result_value END) >= 20
       AND MAX(CASE WHEN is_diastolic_row THEN result_value END) <= 200
       AND MAX(CASE WHEN is_systolic_row THEN result_value END) > MAX(CASE WHEN is_diastolic_row THEN result_value END)
),

readings_flagged AS (
    SELECT
        *,
        -- Hypertensive on this reading's own measurement context
        CASE
            WHEN is_home_bp_reading OR is_abpm_bp_reading
                THEN (systolic_value >= 135 OR diastolic_value >= 85)
            ELSE (systolic_value >= 140 OR diastolic_value >= 90)
        END AS is_hypertensive_reading
    FROM readings
),

-- One representative reading per person/date: lowest systolic, then lowest
-- diastolic (time-of-day is not usable for ordering). The date-level
-- hypertensive flag is any reading in hypertensive range that date.
representative AS (
    SELECT
        *,
        BOOLOR_AGG(is_hypertensive_reading) OVER (
            PARTITION BY person_id, effective_date
        ) AS is_hypertensive_range
    FROM readings_flagged
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, effective_date
        ORDER BY systolic_value ASC, diastolic_value ASC, reading_id ASC
    ) = 1
),

-- Date-level traceability arrays across all observations on the date.
date_concepts AS (
    SELECT
        person_id,
        effective_date,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
        ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids
    FROM valid_observations
    GROUP BY person_id, effective_date
)

-- One row per person per date: representative paired reading + date-level flags
SELECT
    r.person_id,
    r.effective_date,
    r.effective_date AS clinical_effective_date,  -- Backward compatibility alias

    -- Representative (lowest-of-day) paired reading
    r.systolic_value,
    r.diastolic_value,

    -- Measurement context of the representative reading
    r.is_home_bp_reading AS is_home_bp_event,
    r.is_abpm_bp_reading AS is_abpm_bp_event,
    r.bp_context_group,

    -- Date-level hypertensive flag (any reading in range), unchanged semantics
    r.is_hypertensive_range,

    -- Traceability metadata
    'mmHg' AS result_unit_display,
    r.systolic_observation_id,
    r.diastolic_observation_id,
    c.all_concept_codes,
    c.all_concept_displays,
    c.all_source_cluster_ids

FROM representative AS r
INNER JOIN date_concepts AS c
    ON r.person_id = c.person_id
    AND r.effective_date = c.effective_date

ORDER BY r.person_id, r.effective_date DESC
