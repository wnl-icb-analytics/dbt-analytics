{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS GP Appointments Semantic View
    ====================================

    Appointment-level semantic model combining GP access data with current
    patient demographics, publisher-practice details, and PSSRU unit costs.
    OLIDS is the One London Integrated Data Set — primary care data from system
    suppliers (currently EMIS Web, with TPP to follow), unified by the One London team.

    Grain: One row per appointment

    Use Cases:
    - Derived access measures (urgent same-day, routine 7/14 day)
    - DNA rates by deprivation, ethnicity, age band
    - Contact mode trends (F2F vs telephone vs online)
    - Workforce mix analysis (GP vs nurse vs pharmacist)
    - Appointment costing using PSSRU unit costs
    - Equity analysis using current IMD and borough
    - Publisher-practice access comparison

    Practice Attribution:
    - practice_code / practice_name / pcn_name / borough come from dim_practice
      joined via publisher_organisation_code. This is the practice that
      published or owns the appointment record, not a guaranteed delivery
      location.
    - Patient demographics and residence come from the current snapshot.
      Only age_at_event records an event-time demographic.
#}

TABLES(
    appt AS {{ ref('int_appointment_gp_clinical_recent') }}
        PRIMARY KEY (appointment_id)
        COMMENT = 'Cleaned GP appointments — Care Related Encounters only, restricted to the latest 60 source months, anchored to maximum start_date',
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics core (current snapshot). Condition and vulnerability cohorts come from sem_olids_population via a person_id CTE join.',
    practice AS {{ ref('dim_practice') }}
        PRIMARY KEY (practice_code)
        COMMENT = 'Practice details for the appointment publisher or record owner (name, PCN, borough)',
    costs AS {{ ref('pssru_unit_costs_2025') }}
        PRIMARY KEY (practitioner_role_group)
        COMMENT = 'PSSRU unit costs per practitioner role group (2024/2025 prices)'
)

RELATIONSHIPS(
    appt (person_id) REFERENCES demographics,
    appt (publisher_organisation_code) REFERENCES practice (practice_code),
    appt (practitioner_role_group) REFERENCES costs
)

FACTS(
    appt.duration_minutes AS duration_minutes COMMENT = 'Cleaned slot-duration estimate in minutes. NULL for untimed/list schedules. For timed schedules: actual when shorter than planned, otherwise planned, otherwise actual, otherwise 10; capped at 60. This is not an observed consultation duration.',
    appt.planned_duration_mins AS planned_duration_mins COMMENT = 'Raw planned slot duration in minutes; unreliable for untimed/list schedules',
    appt.actual_duration_mins AS actual_duration_mins COMMENT = 'Raw recorded duration in minutes. Values longer than planned are unreliable because workflow buttons can be completed after the consultation; prefer duration_minutes for costing and averages.',
    appt.booking_to_slot_days AS booking_to_slot_days COMMENT = 'Calendar days from booking to the appointment slot time (0 = same day)',
    appt.patient_wait_mins AS patient_wait_mins COMMENT = 'Minutes patient waited beyond scheduled time',
    appt.patient_delay_mins AS patient_delay_mins COMMENT = 'Minutes patient arrived late',
    appt.age_at_event AS age_at_event COMMENT = 'Patient age at appointment (event-time, stable for historical analysis)',
    appt.pssru_cost_per_minute_gbp AS pssru_cost_per_minute_gbp COMMENT = 'PSSRU 2025 cost per minute for the appointment practitioner role group (2024/25 prices)',
    appt.appointment_cost_gbp_base_prices AS appointment_cost_gbp_base_prices COMMENT = 'Appointment cost in PSSRU base year prices (2024/25), in real terms. Use for cross-year comparisons.',
    appt.appointment_cost_gbp_nominal AS appointment_cost_gbp_nominal COMMENT = 'Appointment cost in contemporaneous fiscal year prices (GDP deflator adjusted from the PSSRU 2024/25 base). NULL for fiscal years outside uk_cost_indices seed coverage (pre 2000-01).',
    costs.cost_per_minute_gbp AS cost_per_minute_gbp COMMENT = 'Legacy: PSSRU cost per minute for this role group (same as pssru_cost_per_minute_gbp on appt, retained for backwards compatibility)'
)

DIMENSIONS(
    -- Person linkage key (on appt so it can be selected alongside appointment facts)
    appt.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Appointment time
    appt.start_date AS start_date WITH SYNONYMS = ('appointment date', 'date') COMMENT = 'Scheduled appointment start date and time; this does not prove attendance',
    appt.datetime_booked AS datetime_booked WITH SYNONYMS = ('booking date', 'booked at') COMMENT = 'Date and time when the appointment was booked; null when not recorded',
    appt.fiscal_year_start AS fiscal_year_start WITH SYNONYMS = ('financial year start') COMMENT = 'Calendar year in which the appointment financial year starts; for example 2024 represents 2024/25',

    -- Appointment status
    appt.is_attended AS is_attended COMMENT = 'TRUE if patient attended',
    appt.is_dna AS is_dna WITH SYNONYMS = ('did not attend', 'no show') COMMENT = 'TRUE if did not attend',
    appt.appointment_status_source_display AS appointment_status_source_display WITH SYNONYMS = ('appointment status', 'status') COMMENT = 'Raw appointment status label recorded by the source system. Values are not standardised; use is_attended and is_dna for those two canonical groupings.',
    appt.appointment_status_source_code AS appointment_status_source_code COMMENT = 'Raw source-system appointment status code',

    -- Urgency and access
    appt.urgency AS urgency WITH SYNONYMS = ('urgent', 'routine') COMMENT = 'Appointment urgency (Urgent, Routine, Other). Only General Consultation Acute maps to Urgent per NHSE GP contract 2026/27.',
    appt.is_same_day AS is_same_day WITH SYNONYMS = ('same day') COMMENT = 'Booked for the same calendar day as the scheduled appointment. This does not imply attendance; combine with is_attended for seen activity.',

    -- Contact mode
    appt.contact_mode AS contact_mode WITH SYNONYMS = ('mode', 'delivery mode') COMMENT = 'Simplified contact mode: Face-to-face, Telephone, Online, Home Visit, Video, or Unknown.',
    appt.contact_mode_source_code AS contact_mode_source_code COMMENT = 'Raw contact mode code from source system',

    -- Appointment categories
    appt.national_slot_category_name AS national_slot_category_name WITH SYNONYMS = ('national category', 'national slot category', 'appointment category') COMMENT = 'Recorded national slot category. Prefer this for comparisons and national-category reporting.',
    appt.local_slot_type AS local_slot_type WITH SYNONYMS = ('slot type', 'local appointment type') COMMENT = 'Practice-defined free-text appointment type. Values are not standardised across practices; use national_slot_category_name for comparison.',
    appt.slot_category AS slot_category COMMENT = 'Legacy locally-derived summary of national_slot_category_name. This is not the official GPAD National Slot Type Group; prefer national_slot_category_name.',

    -- Practitioner
    appt.practitioner_role_group AS practitioner_role_group WITH SYNONYMS = ('HCP type', 'staff type', 'role') COMMENT = 'Analytical role grouping (GP, Nurse, Pharmacist, HCA, Physician Associate, Paramedic, Physiotherapist, Care Navigator, Counsellor, Mental Health Practitioner, Health & Wellbeing Coach, Social Prescriber, Dietitian, Podiatrist, Occupational Therapist, Other Direct Patient Care, Admin/Non-Clinical, Unknown)',
    appt.sds_role_group AS sds_role_group WITH SYNONYMS = ('SDS group', 'NHS role group') COMMENT = 'Official NHS Digital SDS role group (GP, Nurses, Other Direct Patient Care, Admin/Data Quality, Unknown) — aligns with national GPAD publications',
    appt.role_name AS role_name COMMENT = 'Raw practitioner role name as recorded by the practice',
    appt.is_arrs_role AS is_arrs_role WITH SYNONYMS = ('ARRS', 'additional roles') COMMENT = 'TRUE where the SDS code unambiguously identifies an ARRS-scheme role',
    appt.schedule_type AS schedule_type COMMENT = 'Raw schedule type from OLIDS',
    appt.is_untimed_session AS is_untimed_session COMMENT = 'TRUE if parent schedule is an open/untimed session (duty doctor, eConsult list). Duration is NULL for these.',
    appt.service_setting AS service_setting COMMENT = 'Recorded GPAD provision and funding setting, such as General Practice, PCN, Extended Access Provision, or Other. It is not a contact mode or physical location; do not infer it from role, time, or venue.',

    -- Appointment publisher / record owner
    appt.appointment_practice_code AS publisher_organisation_code WITH SYNONYMS = ('publisher practice code', 'record owner practice', 'ODS code') COMMENT = 'ODS code of the practice that published or owns the appointment record; not guaranteed to identify the delivery location',
    practice.appointment_practice_name AS practice_name COMMENT = 'Name of the appointment publisher or record-owner practice',
    practice.appointment_pcn_code AS pcn_code COMMENT = 'PCN code of the publisher practice',
    practice.appointment_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the publisher practice',
    practice.appointment_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Publisher-practice PCN name with borough prefix',
    practice.appointment_borough AS borough_registered WITH SYNONYMS = ('borough') COMMENT = 'Borough of the publisher practice',
    practice.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the publisher practice: 93C = NHS North Central London (Camden, Islington, Barnet, Enfield, Haringey); W2U3Z = NHS North West London (Brent, Ealing, Hammersmith and Fulham, Harrow, Hillingdon, Hounslow, Kensington and Chelsea, Westminster). NULL outside the WNL footprint.',
    practice.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name of the publisher practice. NULL outside the WNL footprint.',

    -- Booking
    appt.booking_method AS booking_method COMMENT = 'Recorded booking route, such as Practice, EMIS Access, or External Organisation',

    -- Patient demographics core (current snapshot; richer demographics and
    -- condition cohorts come from sem_olids_population via person_id)
    demographics.gender AS gender COMMENT = 'Patient current gender (Male, Female, Unknown), not necessarily the value at appointment time',
    demographics.age_band_5y AS age_band_5y COMMENT = 'Current 5-year age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.age_band_10y AS age_band_10y COMMENT = 'Current 10-year age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'Current NHS standard age band (drifts — use age_at_event for cohorting historical appointments)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Patient current ethnicity category, not necessarily the value recorded at appointment time',
    demographics.main_language AS main_language COMMENT = 'Patient current main spoken language (Not Recorded if unknown), not necessarily the value at appointment time',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Patient current interpreter requirement, not necessarily the value at appointment time',
    demographics.is_active AS is_active COMMENT = 'Patient currently registered with an NCL GP practice',

    -- Patient geography and deprivation (residence-based)
    demographics.borough_resident AS borough_resident COMMENT = 'Patient current borough of residence, not residence at appointment time',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Patient current NCL neighbourhood of residence, not residence at appointment time',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'Current-residence IMD 2025 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'Current-residence IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Cost reference
    costs.afc_band AS afc_band COMMENT = 'Agenda for Change band for role group',
    appt.cost_is_proxy AS cost_is_proxy COMMENT = 'TRUE when the PSSRU rate is borrowed from another role. FALSE can also mean no rate was available, so check the cost fact for null.',
    appt.cost_proxy_source AS cost_proxy_source COMMENT = 'Practitioner role group whose PSSRU rate was borrowed; null when the rate is direct or unavailable'
)

METRICS(
    -- Volume
    appt.appointment_count AS COUNT(appt.appointment_id) COMMENT = 'All retained clinical appointment records except available slots; includes attended, DNA, cancelled, future booked and other statuses',
    appt.attended_count AS COUNT(CASE WHEN appt.is_attended THEN appt.appointment_id END) COMMENT = 'Appointments recorded as attended; use this for delivered activity',
    appt.dna_count AS COUNT(CASE WHEN appt.is_dna THEN appt.appointment_id END) COMMENT = 'Appointments recorded as DNA',
    appt.patient_count AS COUNT(DISTINCT appt.person_id) COMMENT = 'Distinct people with any retained appointment status; filter is_attended for people receiving delivered activity',

    -- DNA rate
    appt.dna_rate AS COUNT(CASE WHEN appt.is_dna THEN appt.appointment_id END) / NULLIF(COUNT(appt.appointment_id), 0) COMMENT = 'DNA records divided by all retained appointment records (0-1), including cancelled and other statuses in the denominator',

    -- Access KPIs
    appt.urgent_same_day_count AS COUNT(CASE WHEN appt.urgency = 'Urgent' AND appt.is_attended AND appt.is_same_day THEN appt.appointment_id END) COMMENT = 'Urgent seen same day',
    appt.urgent_attended_count AS COUNT(CASE WHEN appt.urgency = 'Urgent' AND appt.is_attended THEN appt.appointment_id END) COMMENT = 'All urgent attended',
    appt.routine_within_7d_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended AND appt.booking_to_slot_days <= 7 THEN appt.appointment_id END) COMMENT = 'Routine within 7 days',
    appt.routine_within_14d_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended AND appt.booking_to_slot_days <= 14 THEN appt.appointment_id END) COMMENT = 'Routine within 14 days',
    appt.routine_attended_count AS COUNT(CASE WHEN appt.urgency = 'Routine' AND appt.is_attended THEN appt.appointment_id END) COMMENT = 'All routine attended',

    -- Duration and wait
    appt.avg_duration AS AVG(appt.duration_minutes) COMMENT = 'Average cleaned slot-duration estimate in minutes; not observed consultation time',
    appt.total_duration AS SUM(appt.duration_minutes) COMMENT = 'Total cleaned slot-duration estimate in minutes; not delivered clinical time unless filtered to attended appointments',
    appt.avg_booking_to_slot_days AS AVG(appt.booking_to_slot_days) COMMENT = 'Average days from booking to the appointment slot',
    appt.avg_patient_wait AS AVG(appt.patient_wait_mins) COMMENT = 'Average wait beyond scheduled time (minutes)'
)

COMMENT = 'OLIDS GP Appointments with publisher-practice attribution, current patient demographics, and PSSRU costs. Grain: one row per appointment record. Scope is Care Related Encounters, not all workload or appointment capacity. The latest 60-month window is anchored to the maximum appointment start date in the source, not CURRENT_DATE. Publisher-practice dimensions identify the data-producing or record-owning practice and are not guaranteed delivery-location fields. Condition cohorts come from sem_olids_population via person_id CTE joins.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. This view is appointment-record grain and contains only Care Related Encounters; it is not all workload or appointment capacity. Filter start_date before linkage, and apply start_date <= CURRENT_TIMESTAMP for observed past activity because future booked appointments can remain. The window contains the latest 60 source months and is anchored to MAX(start_date), not today. appointment_count and patient_count include all retained statuses; use attended_count or filter is_attended = TRUE for delivered activity. appointment_status_source_display is raw; use is_attended and is_dna for those canonical groups. DNA rate uses all retained appointment records as its denominator. Prefer national_slot_category_name for category reporting. local_slot_type is practice-defined free text; slot_category is a legacy local summary, not the official GPAD National Slot Type Group. service_setting records provision/funding context, not contact mode or physical location. Example: SELECT national_slot_category_name, AGG(attended_count) FROM SEM_OLIDS_APPOINTMENTS WHERE is_attended = TRUE AND start_date <= CURRENT_TIMESTAMP GROUP BY national_slot_category_name. Publisher-practice dimensions identify the record-owning practice, not a guaranteed delivery location; registered practice is in sem_olids_population. Demographic and residence fields are current snapshots; only age_at_event is event-time. Avoid interpreting current demographic differences as historical appointment-time inequalities. The urgent same-day and routine 7/14-day metrics are derived attended-appointment measures using this model''s urgency grouping; they do not exactly reproduce the current national definitions. is_same_day means booked for the appointment date and does not itself prove attendance. Cost: SUM(appointment_cost_gbp_base_prices) for real-terms comparisons (PSSRU 2024/25 prices), SUM(appointment_cost_gbp_nominal) for contemporaneous cost; never derive cost from total_duration * cost_per_minute_gbp because that ignores the per-row deflator. cost_is_proxy = FALSE can include rows with no available cost rate; check the cost fact for null.'
AI_QUESTION_CATEGORIZATION 'Use this view for: GP appointment records, derived access measures, wait times, DNA rates by current deprivation/ethnicity, recorded national slot categories, contact mode, service setting, workforce mix, attended utilisation by condition, publisher-practice comparison, and appointment costing. For current population snapshots without appointment data use sem_olids_population. For clinical biomarkers use sem_olids_observations. For time-series condition trends use sem_olids_trends. Questions needing cohorts from TWO domains are answerable by joining this view to the other sem_olids_* views on person_id in CTEs, with aggregate-only output.'
