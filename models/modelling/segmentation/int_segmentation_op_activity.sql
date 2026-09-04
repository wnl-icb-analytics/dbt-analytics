{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'])
}}

-- Outpatient activity blocks for segmentation, rolling 12 months ending on
-- the latest attended appointment date (lag-aware). Grain: one row per
-- sk_patient_id with any qualifying attended activity; sk_patient_id '1'
-- is a shared junk key and is excluded.
--
-- paediatric_op_appointments_12mo counts attended appointments under a
-- paediatric treatment function (paediatric_treatment_function_codes seed).
--
-- outpatient_specialties_12mo counts distinct main specialties, aligned to
-- NWL: excludes trauma & orthopaedics (110), ENT (120), ophthalmology (130)
-- and A&E (180) - the high-volume, low-complexity childhood attendances
-- (fractures, grommets, squints), 20% of children's outpatient activity
-- here. Paediatric T&O (214), ENT (215) and ophthalmology (216) treatment
-- functions are also excluded: some providers (Chelsea and Westminster,
-- GOSH) record them under main specialty 420 paediatrics, which would
-- otherwise let the same activity through. Main specialty loses paediatric
-- sub-specialty granularity, which the treatment function coding kept;
-- accepted as the cost of matching the previously clinically agreed NWL
-- cohort, and the paediatric criterion still counts paediatric treatment
-- functions. On this population the two codings differ by under 1% once
-- the exclusions are applied.
--
-- outpatient_specialties_excluding_maternity_12mo also excludes records
-- coded to obstetrics (501) or midwifery (560) in either main specialty or
-- treatment function. This adjusted count supports the child complexity
-- criterion without changing the general outpatient activity measure.

WITH op_max_date AS (
    SELECT MAX(start_date) AS max_date
    FROM {{ ref('int_sus_op_appointment') }}
    WHERE
        appointment_attended_or_dna IN ('5', '6')
        AND start_date <= CURRENT_DATE()
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
),

paediatric_op AS (
    SELECT
        op.sk_patient_id,
        COUNT(DISTINCT op.visit_occurrence_id) AS paediatric_op_appointments_12mo
    FROM {{ ref('int_sus_op_appointment') }} AS op
    INNER JOIN {{ ref('paediatric_treatment_function_codes') }} AS tfc
        ON op.treatment_function_code = tfc.treatment_function_code
    CROSS JOIN op_max_date AS m
    WHERE
        op.appointment_attended_or_dna IN ('5', '6')
        AND op.start_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
        AND op.sk_patient_id IS NOT NULL
        AND op.sk_patient_id != '1'
    GROUP BY op.sk_patient_id
),

op_specialties AS (
    SELECT
        op.sk_patient_id,
        COUNT(DISTINCT op.main_specialty_code) AS outpatient_specialties_12mo,
        COUNT(DISTINCT CASE
            WHEN
                COALESCE(op.main_specialty_code, '') NOT IN ('501', '560')
                AND COALESCE(op.treatment_function_code, '')
                NOT IN ('501', '560')
                THEN op.main_specialty_code
        END) AS outpatient_specialties_excluding_maternity_12mo
    FROM {{ ref('int_sus_op_appointment') }} AS op
    CROSS JOIN op_max_date AS m
    WHERE
        op.appointment_attended_or_dna IN ('5', '6')
        AND op.start_date BETWEEN DATEADD(MONTH, -12, m.max_date) AND m.max_date
        AND op.main_specialty_code NOT IN ('110', '120', '130', '180')
        AND COALESCE(op.treatment_function_code, '')
        NOT IN ('214', '215', '216')
        AND op.sk_patient_id IS NOT NULL
        AND op.sk_patient_id != '1'
    GROUP BY op.sk_patient_id
)

SELECT
    COALESCE(p.sk_patient_id, s.sk_patient_id) AS sk_patient_id,
    ZEROIFNULL(p.paediatric_op_appointments_12mo)
        AS paediatric_op_appointments_12mo,
    ZEROIFNULL(s.outpatient_specialties_12mo) AS outpatient_specialties_12mo,
    ZEROIFNULL(s.outpatient_specialties_excluding_maternity_12mo)
        AS outpatient_specialties_excluding_maternity_12mo
FROM paediatric_op AS p
FULL OUTER JOIN op_specialties AS s
    ON p.sk_patient_id = s.sk_patient_id
