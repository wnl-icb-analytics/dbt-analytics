{{
    config(
        materialized='table')
}}

/*
    biomarker_trajectories_input_features
    -------------------------------------
    HbA1c series read by the biomarker trajectories job, which fits a Gaussian
    process to each person's readings in Snowpark Container Services and writes
    trajectory readouts back to this schema.

    Grain
      One row per person, clinical effective date and IFCC value.

    Population and time
      Currently registered, living people, excluding test patient records
      (dim_person_active_patients), with a valid HbA1c in the last 5 years up to
      the build date.

    Rules carried over from cltcs_measurements, so the series match the C-LTCS
    measurements the job read before
      - Valid results only (is_valid_hba1c), in IFCC mmol/mol. DCCT percent
        results are converted upstream in int_hba1c_all.
      - Where a person has both IFCC and DCCT results on one date, only the
        IFCC results are kept, so one test recorded on both scales is not read
        as two readings.
      - One row per person, date and value. This also removes the duplicate rows
        int_hba1c_all holds when an observation maps to both HbA1c clusters.

    The job also removes repeated identical values within 7 days, so that rule is
    not applied here.
*/

SELECT
    hb.person_id,
    hb.clinical_effective_date,
    hb.hba1c_ifcc,
    hb.measurement_type,
    hb.id AS observation_id
FROM {{ ref('int_hba1c_all') }} AS hb
INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
    ON hb.person_id = ap.person_id
WHERE hb.is_valid_hba1c = TRUE
    AND hb.clinical_effective_date >= DATEADD(year, -5, CURRENT_DATE())
-- Valid results are always IFCC or DCCT, so NOT is_ifcc means DCCT.
QUALIFY (
        hb.is_ifcc
        OR NOT BOOLOR_AGG(hb.is_ifcc) OVER (
            PARTITION BY hb.person_id, hb.clinical_effective_date)
    )
    -- IFCC first: when an IFCC result and a converted DCCT result have the same
    -- value, ordering by id alone could rank the DCCT row first, and the scale
    -- rule above would then drop both. cltcs_measurements loses these readings.
    AND ROW_NUMBER() OVER (
        PARTITION BY hb.person_id, hb.clinical_effective_date, hb.hba1c_ifcc
        ORDER BY hb.is_ifcc DESC, hb.id
    ) = 1
