{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medications']
    )
}}

/*
Medications prescribed within the last 30 days and last year per person.

Person-level snapshot of recent medication prescriptions with counts.
Includes all medication orders (not just repeat prescriptions) from the last 30 days and last year.
Also splits last-year medicines into current repeats and non-repeats (see medications_repeat / medications_non_repeat).

Grain: One row per person with medications in last 30 days or last year
*/

WITH repeat_medicines_list as (
    select rcm.person_id
        , rcm.mapped_concept_code
        , rcm.latest_order_date
        , 1 as repeat_flag
    from {{ref('int_medication_orders_repeat_current')}} rcm
),

medication_orders_base AS (
    -- Get all medication orders from the last year
    SELECT
        pp.person_id,
        COALESCE(bnf.bnf_name, mo.medication_name, mo.statement_medication_name) AS medication_name,
        bnf.vtm AS vtm_code,
        bnf.vtm_name AS vtm_name,
        mo.clinical_effective_date AS order_date,
        CASE 
            WHEN mo.clinical_effective_date >= DATEADD(day, -30, CURRENT_DATE()) THEN 1 
            ELSE 0 
        END AS is_last_30d,
        zeroifnull(rm.repeat_flag) as is_repeat_order
    FROM {{ ref('stg_olids_medication_order') }} mo
    INNER JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    LEFT JOIN {{ ref('stg_reference_bnf_latest') }} bnf
        ON mo.mapped_concept_code = bnf.snomed_code
    LEFT JOIN repeat_medicines_list rm
        ON mo.mapped_concept_code = rm.mapped_concept_code 
        and pp.person_id = rm.person_id 
        and mo.clinical_effective_date = rm.latest_order_date
    WHERE mo.clinical_effective_date >= DATEADD(day, -365, CURRENT_DATE())
        AND mo.clinical_effective_date <= CURRENT_DATE()
        AND mo.clinical_effective_date IS NOT NULL
        AND COALESCE(bnf.bnf_name, mo.medication_name, mo.statement_medication_name) IS NOT NULL
),

person_spine as (
    select distinct person_id
    FROM medication_orders_base
),


repeat_medicines as (
    -- Distinct medicines whose latest order is a currently active repeat
    SELECT DISTINCT
        person_id,
        medication_name
    FROM medication_orders_base
    WHERE is_repeat_order = 1
),

medications_30d AS (
    -- Get all medication orders from the last 30 days
    SELECT
        person_id,
        medication_name,
        COUNT(*) AS prescription_count
    FROM medication_orders_base
    WHERE is_last_30d = 1
    GROUP BY person_id, medication_name
),

medications_12mo AS (
    -- Get all medication orders from the last year
    SELECT
        person_id,
        medication_name,
        COUNT(*) AS prescription_count
    FROM medication_orders_base
    GROUP BY person_id, medication_name
),

repeat_medication_arrays AS (
    -- Create arrays of medication objects that are current repeats
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT medication_name)WITHIN GROUP (ORDER BY medication_name) AS medications_repeat, -- TO DO: add dose
        COUNT(*) AS unique_repeat_medication_count
    FROM repeat_medicines
    GROUP BY person_id
),

non_repeat_medication_arrays AS (
    -- 12 month medications excluding those that are current repeats (matched on medication name)
    SELECT
        m.person_id,
        ARRAY_COMPACT(ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'medication_name', m.medication_name,
                'prescription_count', m.prescription_count
            )
        ) WITHIN GROUP (ORDER BY m.medication_name)) AS medications_non_repeat
    FROM medications_12mo m
    LEFT JOIN repeat_medicines r
        ON m.person_id = r.person_id
        AND m.medication_name = r.medication_name
    WHERE r.medication_name IS NULL
    GROUP BY m.person_id
),

medication_arrays_30d AS (
    -- Create arrays of medication objects with counts for last 30 days
    SELECT
        person_id,
        ARRAY_COMPACT(ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'medication_name', medication_name,
                'prescription_count', prescription_count
            )
        ) WITHIN GROUP (ORDER BY medication_name)) AS medications_recent_30d,
        SUM(prescription_count) AS total_prescriptions_30d,
        COUNT(DISTINCT medication_name) AS unique_medication_count_30d
    FROM medications_30d
    GROUP BY person_id
),

active_ingredients_30d AS (
    -- Count unique active ingredients (VTMs) for last 30 days
    SELECT
        person_id,
        COUNT(DISTINCT vtm_code) AS unique_active_ingredient_count_30d
    FROM medication_orders_base
    WHERE is_last_30d = 1
        AND vtm_code IS NOT NULL
    GROUP BY person_id
),

medication_arrays_12mo AS (
    -- Create arrays of medication objects with counts for last year
    SELECT
        person_id,
        ARRAY_COMPACT(ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'medication_name', medication_name,
                'prescription_count', prescription_count
            )
        ) WITHIN GROUP (ORDER BY medication_name)) AS medications_recent_12mo,
        SUM(prescription_count) AS total_prescriptions_12mo,
        COUNT(DISTINCT medication_name) AS unique_medication_count_12mo
    FROM medications_12mo
    GROUP BY person_id
),

active_ingredients_12mo AS (
    -- Count unique active ingredients (VTMs) for last year
    SELECT
        person_id,
        COUNT(DISTINCT vtm_code) AS unique_active_ingredient_count_12mo
    FROM medication_orders_base
    WHERE vtm_code IS NOT NULL
    GROUP BY person_id
)

-- Combine both time periods
SELECT
    ps.person_id,
    COALESCE(rm.medications_repeat, ARRAY_CONSTRUCT()) AS medications_repeat,
    COALESCE(nrm.medications_non_repeat, ARRAY_CONSTRUCT()) AS medications_non_repeat,
    COALESCE(m30d.medications_recent_30d, ARRAY_CONSTRUCT()) AS medications_recent_30d,
    COALESCE(m30d.total_prescriptions_30d, 0) AS total_prescriptions_30d,
    COALESCE(m30d.unique_medication_count_30d, 0) AS unique_medication_count_30d,
    COALESCE(ai30d.unique_active_ingredient_count_30d, 0) AS unique_active_ingredient_count_30d,
    COALESCE(m12mo.medications_recent_12mo, ARRAY_CONSTRUCT()) AS medications_recent_12mo,
    COALESCE(m12mo.total_prescriptions_12mo, 0) AS total_prescriptions_12mo,
    COALESCE(m12mo.unique_medication_count_12mo, 0) AS unique_medication_count_12mo,
    COALESCE(ai12mo.unique_active_ingredient_count_12mo, 0) AS unique_active_ingredient_count_12mo,
    COALESCE(rm.unique_repeat_medication_count, 0) AS unique_repeat_medication_count
FROM person_spine ps
left join repeat_medication_arrays rm
    on ps.person_id = rm.person_id
left join non_repeat_medication_arrays nrm
    on ps.person_id = nrm.person_id
left join medication_arrays_30d m30d
    on ps.person_id = m30d.person_id
left join medication_arrays_12mo m12mo
    on ps.person_id =  m12mo.person_id
left join active_ingredients_30d ai30d
    on ps.person_id =  ai30d.person_id
left join active_ingredients_12mo ai12mo
    on ps.person_id =  ai12mo.person_id