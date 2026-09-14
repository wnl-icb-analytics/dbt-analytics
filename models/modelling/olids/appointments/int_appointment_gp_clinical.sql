{{
    config(
        materialized='table',
        cluster_by=['start_date'],
        tags=['intermediate', 'appointment', 'gp']
    )
}}

/*
Cleaned GP appointments from OLIDS.

Filters to legitimate patient-facing clinical appointments:
- Care Related Encounters only (excludes admin, care related activity, none)
- Excludes future/available slots (status code 0)
- Excludes admin practitioner roles (receptionists, clerks etc. — workflow
  logging, not clinical)

Resolves practitioner roles via the sds_role_groups seed, cleans durations,
and classifies contact modes and slot categories for downstream analysis.

Duration methodology:
- For untimed/list schedules (open-book triage, duty doctor, eConsult lists),
  planned_duration_mins is inherited from the whole session — duration_minutes is NULL
- For timed schedules:
  - Use actual_duration_mins if recorded and shorter than planned (GP finished early)
  - Otherwise use planned_duration_mins (the booked slot length)
  - Cap at 60 minutes (anything above is a session/day-length data quality issue)
  - Default NULLs and 0s to 10 minutes (PSSRU-convention GP consultation length)
*/

with appointments as (
    select
        a.id as appointment_id,
        a.person_id,
        a.patient_id,
        a.provider_organisation_id,
        a.practitioner_in_role_id,
        a.schedule_id,
        a.start_date,
        a.datetime_booked,

        -- Raw durations (cleaned below after joining schedule)
        a.planned_duration_mins,
        a.actual_duration_mins,

        -- Status
        a.appointment_status_source_code,
        a.appointment_status_source_display,
        a.appointment_status_code,
        CASE
            WHEN a.appointment_status_code = '5' THEN TRUE
            ELSE FALSE
        END as is_attended,
        CASE
            WHEN a.appointment_status_code = '3' THEN TRUE
            ELSE FALSE
        END as is_dna,

        -- Contact mode (simplified)
        a.contact_mode_source_code,
        -- 'Not an Appointment' is a practice data-entry label rather than a
        -- genuine non-appointment — profiling (2026-04) showed all 44k such
        -- rows have a populated datetime_booked, sit on real schedules,
        -- and span the full range of slot categories including 13.5k
        -- Urgent and 10.4k Routine consultations that feed the access
        -- KPIs. They're real clinical activity where the contact mode
        -- just wasn't properly recorded, so we collapse them into
        -- 'Unknown' alongside the ELSE fallback. The raw value is still
        -- available via contact_mode_source_code for anyone who needs it.
        CASE
            WHEN a.contact_mode_source_code = 'Face to Face (Surgery)' THEN 'Face-to-face'
            WHEN a.contact_mode_source_code = 'Telephone/Audio' THEN 'Telephone'
            WHEN a.contact_mode_source_code = 'Written (Including Online)' THEN 'Online'
            WHEN a.contact_mode_source_code = 'Face to Face (Home Visit)' THEN 'Home Visit'
            WHEN a.contact_mode_source_code = 'Video with Audio' THEN 'Video'
            ELSE 'Unknown'
        END as contact_mode,

        -- Slot category
        a.national_slot_category_name,
        CASE
            WHEN a.national_slot_category_name IN (
                'General Consultation Routine', 'General Consultation Planned'
            ) THEN 'Routine'
            WHEN a.national_slot_category_name = 'General Consultation Acute'
                THEN 'Acute'
            WHEN a.national_slot_category_name IN ('Clinical Triage', 'Triage')
                THEN 'Triage'
            WHEN a.national_slot_category_name IN (
                'Planned Clinics', 'Scheduled/Planned Clinical Activity'
            ) THEN 'Planned Clinic'
            WHEN a.national_slot_category_name IN (
                'Planned Clinical Procedure', 'Scheduled/Planned Clinical Procedure'
            ) THEN 'Clinical Procedure'
            WHEN a.national_slot_category_name IN (
                'Unplanned Clinical Activity', 'Unscheduled/Unplanned Clinical Activity'
            ) THEN 'Unplanned'
            WHEN a.national_slot_category_name IN (
                'Home Visit', 'Care Home Visit',
                'Home Visit and Care Home Visit',
                'Patient contact during Care Home Round'
            ) THEN 'Home/Care Home Visit'
            WHEN a.national_slot_category_name = 'Structured Medication Review'
                THEN 'Medication Review'
            WHEN a.national_slot_category_name = 'Walk-in'
                THEN 'Walk-in'
            WHEN a.national_slot_category_name = 'Social Prescribing Service'
                THEN 'Social Prescribing'
            WHEN a.national_slot_category_name IN (
                'Care Home Needs Assessment & Personalised Care and Support Planning'
            ) THEN 'Care Home Assessment'
            WHEN a.national_slot_category_name = 'Non-contractual chargeable work'
                THEN 'Non-NHS Chargeable'
            WHEN a.national_slot_category_name = 'Service provided by organisation external to the practice'
                THEN 'External Service'
            WHEN a.national_slot_category_name = 'Group Consultation and Group Education'
                THEN 'Group Consultation'
            ELSE 'Other'
        END as slot_category,

        -- Urgency classification (NHSE GP contract 2026/27 definition)
        -- Only 'General Consultation Acute' maps to urgent — this is the
        -- national category practices are instructed to use for clinically
        -- urgent patients. Triage/Unplanned/Walk-in are not inherently urgent.
        CASE
            WHEN a.national_slot_category_name = 'General Consultation Acute'
                THEN 'Urgent'
            WHEN a.national_slot_category_name IN (
                'General Consultation Routine', 'General Consultation Planned',
                'Planned Clinics', 'Planned Clinical Procedure',
                'Scheduled/Planned Clinical Procedure',
                'Scheduled/Planned Clinical Activity',
                'Structured Medication Review'
            ) THEN 'Routine'
            ELSE 'Other'
        END as urgency,
        -- Same-day: booked and seen on the same day (NHSE standard definition)
        CASE
            WHEN a.datetime_booked IS NOT NULL
                AND DATE(a.datetime_booked) = DATE(a.start_date)
                THEN TRUE
            ELSE FALSE
        END as is_same_day,
        -- Calendar days from booking to the appointment slot time
        -- (0 = same day; NULL when booking time is not recorded)
        CASE
            WHEN a.datetime_booked IS NOT NULL
                THEN DATEDIFF('day', DATE(a.datetime_booked), DATE(a.start_date))
        END as booking_to_slot_days,

        -- UK fiscal year start (Apr-Mar) — used for costing and any
        -- year-on-year rollups that should align to NHS financial years
        CASE
            WHEN MONTH(a.start_date) >= 4 THEN YEAR(a.start_date)
            ELSE YEAR(a.start_date) - 1
        END as fiscal_year_start,

        -- Patient experience
        a.patient_wait_mins,
        a.patient_delay_mins,

        -- Booking
        a.booking_method_source_code as booking_method,

        -- Context
        a.appointment_type as local_slot_type,
        a.context_type,
        a.service_setting,
        a.age_at_event,
        a.publisher_organisation_code

    from {{ ref('stg_olids_appointment') }} as a
    where a.context_type = 'Care Related Encounter'
      and a.appointment_status_code != '0'  -- exclude future/available slots
),

practitioner_roles as (
    -- Join practitioner_in_role to:
    --   - stg_olids_practitioner for the clinician's name
    --   - sds_role_groups seed for official SDS group + our analytical
    --     grouping + ARRS flag (replaces the old hard-coded CASE block)
    -- Codes not present in the seed fall through to 'Other' / FALSE via
    -- COALESCE below, so new SDS codes never cause rows to be dropped.
    select
        pir.id as practitioner_in_role_id,
        pir.practitioner_id,
        pir.employer_organisation_id as practitioner_org_id,
        pir.role_code,
        pir.role as role_name,
        p.name as practitioner_name,
        COALESCE(sds.sds_role_group, 'Unknown') as sds_role_group,
        COALESCE(sds.practitioner_role_group, 'Other') as practitioner_role_group,
        COALESCE(sds.is_arrs_role, FALSE) as is_arrs_role
    from {{ ref('stg_olids_practitioner_in_role') }} as pir
    left join {{ ref('stg_olids_practitioner') }} as p
        on pir.practitioner_id = p.id
    left join {{ ref('sds_role_groups') }} as sds
        on pir.role_code = sds.role_code
),

schedules as (
    -- Schedule is the container for appointments; appointment_type indicates whether
    -- the schedule is timed (normal bookable slots) or untimed (open books
    -- like duty doctor / triage sessions where planned_duration_mins is meaningless)
    select
        s.id as schedule_id,
        s.type as schedule_type,
        CASE
            WHEN s.type IN ('Untimed Appointments', 'List') THEN TRUE
            ELSE FALSE
        END as is_untimed_session
    from {{ ref('stg_olids_schedule') }} as s
),

pssru_base_deflator as (
    -- GDP deflator for the PSSRU base fiscal year. The 2025 manual reports
    -- 2024-25 prices, so fiscal_year_start = 2024. Bump this and the
    -- pssru_unit_costs_2025 seed when PSSRU publishes a newer manual.
    select gdp_deflator as pssru_base_gdp_deflator
    from {{ ref('uk_cost_indices') }}
    where fiscal_year_start = 2024
),

cleaned as (
    -- Core cleaned appointment row with duration_minutes computed. Wrapped
    -- in its own CTE so the downstream cost calculation can reference
    -- duration_minutes (Snowflake can't use a SELECT-level alias in the
    -- same SELECT).
    select
    a.appointment_id,
    a.person_id,
    a.patient_id,
    a.provider_organisation_id,
    a.schedule_id,
    a.start_date,
    a.datetime_booked,

    -- Raw durations
    a.planned_duration_mins,
    a.actual_duration_mins,

    -- Cleaned duration (timed schedules only)
    -- For untimed/list schedules, planned_duration_mins is meaningless (inherited from
    -- the whole session), so duration_minutes is NULL. Downstream aggregations
    -- will correctly skip these rather than using a fabricated default.
    -- For timed schedules: use actual if reliably shorter than planned, else
    -- planned. Capped at 60 minutes, defaults to 10 if null/zero.
    CASE
        WHEN COALESCE(s.is_untimed_session, FALSE) THEN NULL
        ELSE LEAST(
            COALESCE(
                CASE
                    -- Actual is reliable when shorter than planned (GP finished early)
                    WHEN a.actual_duration_mins > 0
                         AND a.planned_duration_mins > 0
                         AND a.actual_duration_mins < a.planned_duration_mins
                        THEN a.actual_duration_mins
                    -- Otherwise prefer planned slot length
                    WHEN a.planned_duration_mins > 0
                        THEN a.planned_duration_mins
                    -- If only actual is recorded (planned NULL/0), use it
                    WHEN a.actual_duration_mins > 0
                        THEN a.actual_duration_mins
                    ELSE 10
                END,
                10
            ),
            60
        )
    END as duration_minutes,

    -- Schedule context
    s.schedule_type,
    s.is_untimed_session,

    -- Status
    a.appointment_status_source_code,
    a.appointment_status_source_display,
    a.is_attended,
    a.is_dna,

    -- Contact mode
    a.contact_mode_source_code,
    a.contact_mode,

    -- Slot category
    a.national_slot_category_name,
    a.slot_category,

    -- Practitioner
    -- Wrap the seed-derived columns in COALESCE so appointments with no
    -- matching practitioner_in_role record (NULL practitioner_in_role_id
    -- or missing pir row) still get an explicit 'Unknown' value rather
    -- than NULL — protects downstream not_null tests and analyst queries.
    pr.practitioner_id,
    pr.practitioner_name,
    pr.role_code,
    pr.role_name,
    COALESCE(pr.sds_role_group, 'Unknown') as sds_role_group,
    COALESCE(pr.practitioner_role_group, 'Unknown') as practitioner_role_group,
    COALESCE(pr.is_arrs_role, FALSE) as is_arrs_role,

    -- Urgency
    a.urgency,
    a.is_same_day,
    a.booking_to_slot_days,

    -- Patient experience
    a.patient_wait_mins,
    a.patient_delay_mins,

    -- Fiscal year (for costing and financial-year rollups)
    a.fiscal_year_start,

    -- Booking and context
    a.booking_method,
    a.local_slot_type,
    a.service_setting,
    a.age_at_event,
    a.publisher_organisation_code

    from appointments as a
    left join practitioner_roles as pr
        on a.practitioner_in_role_id = pr.practitioner_in_role_id
    left join schedules as s
        on a.schedule_id = s.schedule_id
    where COALESCE(pr.practitioner_role_group, 'Unknown') != 'Admin'
)

select
    c.*,

    -- PSSRU unit cost at PSSRU base year prices (2024-25 for the 2025 manual).
    -- Sourced from the pssru_unit_costs_2025 seed. Rows flagged cost_is_proxy
    -- use a band-matched rate from another role (see pssru_unit_costs_2025.yml
    -- for methodology and the seed's notes column for per-row rationale).
    -- NULL for non-clinical role groups (Other / Unknown).
    costs.cost_per_minute_gbp as pssru_cost_per_minute_gbp,
    COALESCE(costs.is_proxy, FALSE) as cost_is_proxy,
    costs.proxy_source as cost_proxy_source,

    -- Cost in PSSRU base year prices (constant real terms across FYs)
    CASE
        WHEN c.duration_minutes IS NULL THEN NULL
        WHEN costs.cost_per_minute_gbp IS NULL THEN NULL
        ELSE ROUND(c.duration_minutes * costs.cost_per_minute_gbp, 2)
    END as appointment_cost_gbp_base_prices,

    -- Cost in the appointment's own fiscal year prices (nominal / contemporaneous)
    -- = base cost * (appointment-year deflator / PSSRU base-year deflator)
    CASE
        WHEN c.duration_minutes IS NULL THEN NULL
        WHEN costs.cost_per_minute_gbp IS NULL THEN NULL
        WHEN idx.gdp_deflator IS NULL THEN NULL
        WHEN pssru.pssru_base_gdp_deflator IS NULL THEN NULL
        ELSE ROUND(
            c.duration_minutes
            * costs.cost_per_minute_gbp
            * (idx.gdp_deflator / pssru.pssru_base_gdp_deflator),
            2
        )
    END as appointment_cost_gbp_nominal

from cleaned as c
cross join pssru_base_deflator as pssru
left join {{ ref('pssru_unit_costs_2025') }} as costs
    on c.practitioner_role_group = costs.practitioner_role_group
left join {{ ref('uk_cost_indices') }} as idx
    on c.fiscal_year_start = idx.fiscal_year_start
