{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['qadmissions', 'risk_scores', 'monthly-full'],
        meta={
            'custom_message': 'Emergency admission outcomes for validating QAdmissions scores built from qadmissions_input_features_history. Holds no risk scores. Includes OLIDS and SUS data - for secondary use, INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED ON person_id to apply National Data Opt-Out and Type 1 opt-out filtering.'
        }
    )
}}

/*
    qadmissions_outcomes_history
    ----------------------------
    Follow-up of each person in qadmissions_input_features_history after each
    index date: the first emergency admission, or the date and reason follow-up
    ended without one. Pair it with scores from the same index date to
    validate QAdmissions (calibration, discrimination, sensitivity and PPV).

    Grain
      One row per person and index date, for every row of
      qadmissions_input_features_history.

    Horizon
      2 years, the horizon the QAdmissions paper validated, from
      qadmissions_history_horizon_years(). qadmissions_input_features_history
      passes the same value to the model as surv.

    Event
      The first SUS admitted patient care spell admitted after the index
      date, up to the end of the horizon, with an emergency admission method:
      every method starting 2 except 2C, the definition hes_admitprior_cat
      uses. Open spells count.

    End of follow-up
      The earliest of: the event, death, deregistration, the end of complete
      SUS data and the end of the horizon. An admission after any of the
      others is not an event: SUS is unlikely to hold admissions for people
      who have left the registered population.
      - Death: the date from int_patient_deceased_status where the person
        links to it by sk_patient_id, otherwise the mid-month approximation
        from dim_person_birth_death.
      - Deregistration: as in the deregistration CTEs below; a deregistration
        followed by another OLIDS registration before the end of the horizon
        doesn't end follow-up.
      - Complete SUS data: up to the end of the third month before the build
        month. Rows whose horizon ends later have has_complete_follow_up =
        FALSE.

    Eligibility flags (rows are kept, so exclusions can be counted)
      has_sus_link: sk_patient_id can link to SUS. Without it no admission
        can be found, so these rows must be excluded from validation.
      is_registered_12m: registered at the index date with a practice whose
        registration started at least 12 months earlier, the QAdmissions
        paper's entry rule for a complete GP record.
*/

{%- set horizon_years = qadmissions_history_horizon_years() -%}
{%- set horizon_months = 12 * horizon_years %}

WITH index_dates AS (
    SELECT reference_date AS end_date
    FROM (
        {{ qadmissions_history_index_dates() }}
    )
),

-- The span of outcome dates the index dates need, and the last month of
-- complete SUS data, allowing for submission lag.
follow_up_window AS (
    SELECT
        MIN(end_date)                                                AS first_end_date,
        MAX(end_date)                                                AS last_end_date,
        LAST_DAY(DATEADD('month', {{ horizon_months }}, MAX(end_date))) AS last_horizon_end_date,
        LAST_DAY(DATEADD('month', -3, CURRENT_DATE()))               AS sus_complete_through_date
    FROM index_dates
),

base_spine AS (
    SELECT
        end_date,
        LAST_DAY(DATEADD('month', {{ horizon_months }}, end_date)) AS horizon_end_date,
        person_id,
        sk_patient_id,
        sk_patient_id IS NOT NULL AND sk_patient_id <> '1'         AS has_sus_link
    FROM {{ ref('qadmissions_input_features_history') }}
),

-- Emergency admissions that could follow any index date. sk_patient_id '1' is
-- a placeholder, not a patient.
emergency_admissions AS (
    SELECT
        e.sk_patient_id,
        e.visit_occurrence_id,
        e.start_date::DATE AS admission_date,
        e.spell_admission_method
    FROM {{ ref('int_sus_apc_encounter') }} AS e
    CROSS JOIN follow_up_window AS w
    WHERE e.sk_patient_id IS NOT NULL
        AND e.sk_patient_id <> '1'
        AND LEFT(e.spell_admission_method, 1) = '2'
        AND e.spell_admission_method <> '2C'
        AND e.start_date::DATE > w.first_end_date
        AND e.start_date::DATE <= w.last_horizon_end_date
),

-- First emergency admission after each index date, within the horizon. The
-- final SELECT decides whether it came before follow-up ended.
first_admission AS (
    SELECT
        b.end_date,
        b.person_id,
        a.admission_date,
        a.spell_admission_method
    FROM base_spine AS b
    INNER JOIN emergency_admissions AS a
        ON b.sk_patient_id = a.sk_patient_id
        AND a.admission_date > b.end_date
        AND a.admission_date <= b.horizon_end_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.end_date, b.person_id
        ORDER BY a.admission_date, a.spell_admission_method, a.visit_occurrence_id
    ) = 1
),

-- Death dates per sk_patient_id. int_patient_deceased_status has one row per
-- OLIDS patient record, and one sk_patient_id can have several.
linked_deaths AS (
    SELECT
        sk_patient_id,
        MIN(final_date_of_death) AS linked_death_date
    FROM {{ ref('int_patient_deceased_status') }}
    WHERE is_deceased
        AND final_date_of_death IS NOT NULL
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id <> '1'
    GROUP BY sk_patient_id
),

-- One row per person.
birth_death AS (
    SELECT
        person_id,
        is_deceased,
        death_date_approx
    FROM {{ ref('dim_person_birth_death') }}
),

-- Deregistrations from OLIDS, one row per person and deregistration date.
-- Excludes registrations ended by death, including deaths up to 31 days after
-- the end date (death dates are mid-month approximations, so these are likely
-- deaths processed late). next_registered_date is the earliest start of any
-- other OLIDS registration still open after the deregistration; an overlapping
-- registration counts, so it can fall before the deregistration date.
deregistration_events AS (
    SELECT
        r.person_id,
        r.registration_end_date AS deregistration_date,
        MIN(o.registration_start_date) AS next_registered_date
    FROM {{ ref('dim_person_historical_practice') }} AS r
    LEFT JOIN birth_death AS bd
        ON r.person_id = bd.person_id
    LEFT JOIN {{ ref('dim_person_historical_practice') }} AS o
        ON r.person_id = o.person_id
        AND r.registration_sequence <> o.registration_sequence
        AND COALESCE(o.effective_end_date::date, '9999-12-31') > r.registration_end_date
    -- Inverted end dates are treated as still open upstream
    WHERE r.registration_end_date >= r.registration_start_date
        -- Not already ended by a death recorded before the end date
        AND r.effective_end_date::date = r.registration_end_date
        AND NOT (
            COALESCE(bd.is_deceased, FALSE)
            AND bd.death_date_approx <= DATEADD('day', 31, r.registration_end_date)
        )
    GROUP BY
        r.person_id,
        r.registration_end_date
),

-- First deregistration after the index date and within the horizon with no
-- other OLIDS registration starting before the horizon ends.
deregistration AS (
    SELECT
        b.end_date,
        b.person_id,
        MIN(de.deregistration_date) AS deregistration_date
    FROM base_spine AS b
    INNER JOIN deregistration_events AS de
        ON b.person_id = de.person_id
        AND de.deregistration_date > b.end_date
        AND de.deregistration_date <= b.horizon_end_date
        AND (
            de.next_registered_date IS NULL
            OR de.next_registered_date > b.horizon_end_date
        )
    GROUP BY
        b.end_date,
        b.person_id
),

-- Registrations that could be open at some index date, narrowed before the
-- join to index dates. An end date before the start is treated as open, as in
-- int_segmentation_person_month_spine.
registrations AS (
    SELECT
        r.person_id,
        r.registration_start_date::DATE AS registration_start_date,
        r.registration_end_date::DATE   AS registration_end_date
    FROM {{ ref('dim_person_historical_practice') }} AS r
    CROSS JOIN follow_up_window AS w
    WHERE r.registration_start_date::DATE <= w.last_end_date
        AND (
            r.registration_end_date IS NULL
            OR r.registration_end_date::DATE > w.first_end_date
            OR r.registration_end_date < r.registration_start_date
        )
),

-- Registered at the index date with a practice for at least the 12 months
-- before it. One row per person and index date with a registration open then.
registration_12m AS (
    SELECT
        b.end_date,
        b.person_id,
        BOOLOR_AGG(r.registration_start_date <= DATEADD('month', -12, b.end_date)) AS is_registered_12m
    FROM base_spine AS b
    INNER JOIN registrations AS r
        ON b.person_id = r.person_id
        AND r.registration_start_date <= b.end_date
        AND (
            r.registration_end_date IS NULL
            OR r.registration_end_date > b.end_date
            OR r.registration_end_date < r.registration_start_date
        )
    GROUP BY
        b.end_date,
        b.person_id
),

-- One row per person and index date, with every candidate end of follow-up
-- inside the horizon. censoring_date is the end of follow-up if there is no
-- event, never before the index date. Each joined CTE has at most one row per
-- (end_date, person_id) or per join key.
follow_up AS (
    SELECT
        b.end_date,
        b.person_id,
        b.sk_patient_id,
        b.horizon_end_date,
        b.has_sus_link,
        COALESCE(reg.is_registered_12m, FALSE)                         AS is_registered_12m,
        w.sus_complete_through_date,
        fa.admission_date,
        fa.spell_admission_method,
        COALESCE(ld.linked_death_date, IFF(bd.is_deceased, bd.death_date_approx, NULL)) AS any_death_date,
        IFF(any_death_date > b.end_date AND any_death_date <= b.horizon_end_date, any_death_date, NULL) AS death_date,
        dr.deregistration_date,
        GREATEST(
            b.end_date,
            LEAST(
                COALESCE(death_date, b.horizon_end_date),
                COALESCE(dr.deregistration_date, b.horizon_end_date),
                b.horizon_end_date,
                w.sus_complete_through_date
            )
        )                                                              AS censoring_date
    FROM base_spine AS b
    CROSS JOIN follow_up_window AS w
    LEFT JOIN first_admission AS fa
        ON b.end_date = fa.end_date
        AND b.person_id = fa.person_id
    LEFT JOIN linked_deaths AS ld
        ON b.sk_patient_id = ld.sk_patient_id
    LEFT JOIN birth_death AS bd
        ON b.person_id = bd.person_id
    LEFT JOIN deregistration AS dr
        ON b.end_date = dr.end_date
        AND b.person_id = dr.person_id
    LEFT JOIN registration_12m AS reg
        ON b.end_date = reg.end_date
        AND b.person_id = reg.person_id
)

SELECT
    end_date,
    person_id,
    sk_patient_id,
    {{ horizon_years }}                                                AS horizon_years,
    horizon_end_date,
    has_sus_link,
    is_registered_12m,
    horizon_end_date <= sus_complete_through_date                      AS has_complete_follow_up,
    COALESCE(admission_date <= censoring_date, FALSE)                  AS is_event,
    IFF(is_event, admission_date, NULL)                                AS emergency_admission_date,
    IFF(is_event, spell_admission_method, NULL)                        AS emergency_admission_method,
    death_date,
    deregistration_date,
    IFF(is_event, admission_date, censoring_date)                      AS follow_up_end_date,
    -- On a tie the event wins, then death, deregistration and end of data.
    CASE
        WHEN is_event                                         THEN 'emergency_admission'
        WHEN death_date = censoring_date                      THEN 'death'
        WHEN deregistration_date = censoring_date             THEN 'deregistration'
        WHEN sus_complete_through_date < horizon_end_date     THEN 'end_of_data'
        ELSE 'end_of_horizon'
    END                                                                AS follow_up_end_reason,
    DATEDIFF('day', end_date, follow_up_end_date)                      AS follow_up_days
FROM follow_up
