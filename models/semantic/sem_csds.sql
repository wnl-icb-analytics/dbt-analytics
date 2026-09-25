{{ config(materialized='semantic_view', schema='SEMANTIC') }}

TABLES(
    people AS {{ ref('fct_csds_person_summary') }}
        PRIMARY KEY (person_id) COMMENT = 'Summary of the CSDS evidence for each identifiable person. Each row is one CSDS person ID found in any modelled evidence. Demographics come from the newest period record across providers. as_of_date is the latest accepted dataset period; providers can be older. National ID changes can split one person across rows.',
    referrals AS {{ ref('fct_csds_referral_summary') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Care and access measures for each latest recorded community referral. A referral absent from its provider''s latest submission counts as closed. Contact, activity, team and clock measures are aggregated before joining. Time to first attendance is recorded evidence, not a national waiting-time measure.',
    contacts AS {{ ref('fct_csds_care_contact') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Latest referral/contact pairs in all attendance states, including cancellations and non-attendance. Team type is the delivering team the contact names; read the attribution basis before comparing teams across providers. Practice is the GP registration on the contact date.',
    activities AS {{ ref('fct_csds_care_activity') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Accepted care activity occurrences within contacts, one per submission and activity identifier. Repeated monthly submissions remain separate. Activities are detail of contacts, not separate encounters.',
    clinical AS {{ ref('fct_csds_clinical_record') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Retained clinical items: coded immunisations, referral and activity assessment responses, and procedures, findings and observations recorded on activities. Assessment rows are responses, not completed questionnaires.',
    referral_periods AS {{ ref('fct_csds_referral_period') }}
        PRIMARY KEY (referral_period_id) COMMENT = 'Referral state in each accepted submission. Open at period end when submitted that month, received, not discharged, and at least one submitted team still open. A referral or team no longer submitted counts as closed. Select one reporting period for a snapshot. Health visiting and school nursing referrals legitimately stay open for years.',
    caseload_referrals AS {{ ref('fct_csds_current_caseload_referral') }}
        PRIMARY KEY (referral_period_id) COMMENT = 'Open referrals in the dataset''s latest accepted month, including those awaiting a first attended contact. Providers whose latest submission is older are excluded, which does not mean discharge. Recorded caseload, not confirmed active treatment.',
    caseload_people AS {{ ref('fct_csds_current_caseload_person') }}
        PRIMARY KEY (caseload_person_provider_id) COMMENT = 'People with at least one open referral in the dataset''s latest month, one row per person and provider. Count distinct person_id across providers.',
    rtt AS {{ ref('fct_csds_referral_to_treatment_period') }}
        PRIMARY KEY (referral_to_treatment_period_id) COMMENT = 'Accepted referral-to-treatment and response clock records. A clock repeats each month it is reported; filter rtt_is_latest_clock_record to count clocks once. Type 05 is the two-hour urgent community response and type 06 the two-day standard. The view does not decide target compliance.',
    teams AS {{ ref('fct_csds_referral_service') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Latest service or team relationship for each referral and local team. A referral can have several teams, each with its own closure or rejection; a team absent from the referral''s latest submission is treated as ended.',
    demographics AS {{ ref('dim_csds_person_provider_period') }}
        PRIMARY KEY (person_provider_period_id) COMMENT = 'Demographics and residence reported for a person by a provider in a reporting period. Use for historical breakdowns through the declared relationships from contacts, referral periods, caseload and clocks. Differences between providers stay separate.',
    submissions AS {{ ref('dq_csds_provider_submission') }}
        PRIMARY KEY (provider_organisation_code, reporting_period_end_date) COMMENT = 'Submission dates and record counts for each provider and accepted reporting period. Check provider freshness before comparing activity or caseload. Several providers stopped submitting CSDS years ago.'
)

RELATIONSHIPS(
    referrals (person_id) REFERENCES people,
    contacts (person_id) REFERENCES people,
    contacts (person_provider_period_id) REFERENCES demographics,
    activities (person_id) REFERENCES people,
    clinical (person_id) REFERENCES people,
    referral_periods (person_id) REFERENCES people,
    referral_periods (person_provider_period_id) REFERENCES demographics,
    caseload_referrals (person_id) REFERENCES people,
    caseload_referrals (person_provider_period_id) REFERENCES demographics,
    caseload_people (person_id) REFERENCES people,
    caseload_people (person_provider_period_id) REFERENCES demographics,
    rtt (person_id) REFERENCES people,
    rtt (person_provider_period_id) REFERENCES demographics,
    teams (person_id) REFERENCES people
)

DIMENSIONS(
    people.person_id AS person_id COMMENT = 'CSDS person identifier for linking aggregate cohorts. Never return person identifiers in query results.',
    people.as_of_date AS as_of_date COMMENT = 'Latest accepted dataset reporting-period end, the observation date for current person measures. Not the run date.',
    people.person_gender AS person_stated_gender_name COMMENT = 'Latest reported person stated gender description.',
    people.person_ethnicity_group AS ethnicity_2001_broad_group COMMENT = 'Broad 2001 ethnic group from the latest patient record; null when unknown or unmatched.',
    people.person_residence_local_authority AS residence_local_authority_name COMMENT = 'Local authority of residence from the latest patient record. Current evidence, not residence at earlier care.',
    people.person_imd_2025_decile AS residence_imd_2025_decile COMMENT = 'IMD 2025 decile of the latest reported residence; 1 is most deprived.',
    people.person_is_wnl_resident AS is_wnl_resident COMMENT = 'The latest patient record places residence in West and North London.',
    people.has_current_recorded_caseload AS has_current_recorded_caseload COMMENT = 'True when the person has an open referral in the dataset latest month.',
    people.is_looked_after_child AS is_looked_after_child COMMENT = 'Latest reported looked-after-child indicator: Y true, N false, otherwise null. Null must not be treated as no.',
    referrals.referrals_provider_code AS provider_organisation_code COMMENT = 'Code of the provider reporting these referrals.',
    referrals.referrals_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting these referrals.',
    referrals.referrals_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The referral is commissioned by West and North London. Filter true for WNL figures.',
    referrals.referral_received_date AS referral_received_date COMMENT = 'Recorded referral received date. Use for intake trends.',
    referrals.referral_reason AS primary_reason_for_referral_name COMMENT = 'Primary reason for referral description; null when unmatched.',
    referrals.referral_priority AS priority_type_name COMMENT = 'Referral priority description.',
    referrals.referral_source AS source_of_referral_name COMMENT = 'Source of referral description.',
    referrals.referral_team_type AS service_or_team_type_name COMMENT = 'Team type when all the referral''s teams in its latest submission share one; null when they differ or none were submitted. Musculoskeletal and physiotherapy services are separate types.',
    referrals.referral_access_state AS latest_recorded_access_state COMMENT = 'no_longer_submitted, discharged, all teams ended, attended contact recorded, or no attended contact recorded, in the provider''s latest submission.',
    referrals.referral_is_open AS is_recorded_open COMMENT = 'Open in the provider''s latest submission. Referrals no longer submitted count as closed.',
    contacts.contacts_provider_code AS provider_organisation_code COMMENT = 'Code of the provider reporting these contacts.',
    contacts.contacts_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting these contacts.',
    contacts.contacts_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The contact is commissioned by West and North London. Filter true for WNL figures; about a quarter of recent contacts are commissioned elsewhere.',
    contacts.contact_date AS care_contact_date COMMENT = 'Recorded contact date; the scheduled date for cancellations and non-attendance. Use for activity trends.',
    contacts.contact_attendance AS attendance_name COMMENT = 'Attendance description from the attended-or-did-not-attend field.',
    contacts.contact_mechanism AS consultation_mechanism_name COMMENT = 'Consultation mechanism, such as face to face, telephone or video.',
    contacts.contact_location_type AS activity_location_type_name COMMENT = 'Type of location where the contact took place.',
    contacts.contact_team_type AS service_or_team_type_name COMMENT = 'Delivering service or team type. Musculoskeletal and physiotherapy services are separate types. Read with contact_team_attribution_basis.',
    contacts.contact_team_attribution_basis AS service_or_team_attribution_basis COMMENT = 'contact_team, contact_team_other_submission or contact_team_submission_map when the contact''s own team was resolved; referral_team_* when the referral''s single team type was used; unresolved otherwise.',
    contacts.contact_practice_code AS practice_code COMMENT = 'GP practice registered on the contact date, else the person''s latest known practice.',
    contacts.is_attended AS is_attended COMMENT = 'Attendance code 5 or 6, from the attended-or-did-not-attend field or else the newer attendance status.',
    activities.activity_type AS activity_type_name COMMENT = 'Community care activity type description.',
    activities.activity_contact_date AS care_contact_date COMMENT = 'Date of the contact the activity belongs to; activities have no separate date.',
    clinical.clinical_record_type AS clinical_record_type COMMENT = 'immunisation, referral_assessment, activity_assessment, procedure, finding or observation.',
    clinical.clinical_code_system AS clinical_code_system COMMENT = 'Coding system of the submitted clinical code.',
    clinical.clinical_date AS clinical_date COMMENT = 'Recorded or inherited date of the clinical item.',
    clinical.clinical_label_status AS clinical_label_status COMMENT = 'Whether the submitted code found a label. Reference availability is not clinical validity.',
    referral_periods.referral_periods_provider_code AS provider_organisation_code COMMENT = 'Provider reporting the referral in this period.',
    referral_periods.referral_periods_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting the referral in this period.',
    referral_periods.referral_periods_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The referral is commissioned by West and North London in this period.',
    referral_periods.referral_periods_period_end AS reporting_period_end_date COMMENT = 'Reporting period the state is evaluated at. Filter to one period for a snapshot.',
    referral_periods.referral_periods_access_state AS recorded_access_state COMMENT = 'Access state at period end.',
    referral_periods.referral_periods_open_at_period_end AS is_recorded_open_at_period_end COMMENT = 'Open at period end: submitted, received, not discharged, and at least one submitted team still open.',
    referral_periods.referral_periods_waiting_for_first_attendance AS is_open_without_recorded_attendance COMMENT = 'Open at period end with no attended contact evidenced by then. Evidence of waiting, not waiting-list eligibility.',
    referral_periods.referral_periods_team_type AS service_or_team_type_name COMMENT = 'Team type when all the referral''s teams in that submission share one; null otherwise.',
    caseload_referrals.caseload_referrals_provider_code AS provider_organisation_code COMMENT = 'Provider reporting the open referral.',
    caseload_referrals.caseload_referrals_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting the open referral.',
    caseload_referrals.caseload_referrals_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The open referral is commissioned by West and North London. Filter true for the WNL caseload.',
    caseload_referrals.caseload_referrals_team_type AS service_or_team_type_name COMMENT = 'Team type when all the referral''s teams share one; null otherwise.',
    caseload_referrals.caseload_referrals_contact_state AS recorded_caseload_contact_state COMMENT = 'Whether attendance was evidenced in the latest period, only earlier, or not at all.',
    caseload_people.caseload_people_provider_code AS provider_organisation_code COMMENT = 'Provider of the person''s open referrals.',
    caseload_people.caseload_people_provider_name AS provider_organisation_name COMMENT = 'Name of the provider of the person''s open referrals.',
    rtt.rtt_provider_code AS provider_organisation_code COMMENT = 'Provider reporting the clock.',
    rtt.rtt_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting the clock.',
    rtt.rtt_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The clock''s referral is commissioned by West and North London in that submission.',
    rtt.rtt_measurement_type_code AS waiting_time_measurement_type_code COMMENT = 'Waiting time measurement type code; 05 is the two-hour urgent community response, 06 the two-day standard.',
    rtt.rtt_measurement_type AS waiting_time_measurement_type_name COMMENT = 'Community-care waiting time measurement type description.',
    rtt.rtt_status AS rtt_status_name COMMENT = 'Referral-to-treatment period status description.',
    rtt.rtt_start_date AS rtt_start_date COMMENT = 'Clock start date. Group two-hour response clocks by start month.',
    rtt.rtt_period_end AS reporting_period_end_date COMMENT = 'Reporting period carrying the clock record.',
    rtt.rtt_is_latest_clock_record AS is_latest_clock_record COMMENT = 'Newest report of the clock. Filter true to count each clock once.',
    teams.team_type AS service_type_name COMMENT = 'Service or team type of the relationship.',
    teams.team_provider_code AS provider_organisation_code COMMENT = 'Provider reporting the relationship.',
    teams.team_in_latest_referral_submission AS is_in_latest_referral_submission COMMENT = 'Reported in the referral''s latest submission; otherwise treated as ended.',
    demographics.demographics_provider_name AS provider_organisation_name COMMENT = 'Provider reporting the demographics.',
    demographics.demographics_period_end AS reporting_period_end_date COMMENT = 'Reporting period of the demographics.',
    demographics.demographics_gender AS person_stated_gender_name COMMENT = 'Person stated gender reported in that provider period.',
    demographics.demographics_ethnicity_group AS ethnicity_2001_broad_group COMMENT = 'Broad 2001 ethnic group reported in that provider period.',
    demographics.demographics_residence_local_authority AS residence_local_authority_name COMMENT = 'Local authority of residence reported in that provider period.',
    demographics.demographics_is_wnl_resident AS is_wnl_resident COMMENT = 'Residence in that provider period is in West and North London.',
    demographics.demographics_imd_2025_decile AS residence_imd_2025_decile COMMENT = 'IMD 2025 decile of the residence reported in that period; 1 is most deprived.',
    demographics.demographics_age_at_period_end AS source_age_at_period_end COMMENT = 'Source-derived age in years at the period end.',
    submissions.submissions_provider_code AS provider_organisation_code COMMENT = 'Provider organisation code.',
    submissions.submissions_provider_name AS provider_organisation_name COMMENT = 'Provider organisation name.',
    submissions.submission_period_end AS reporting_period_end_date COMMENT = 'Accepted reporting period.',
    submissions.is_latest_provider_period AS is_latest_provider_period COMMENT = 'True on each provider''s latest accepted period.',
    submissions.provider_months_behind_dataset AS provider_months_behind_dataset COMMENT = 'Months between the provider''s latest period and the dataset''s latest period.'
)

METRICS(
    people.person_count AS COUNT(people.person_id) COMMENT = 'Identifiable CSDS person IDs in all modelled evidence. One individual can have several national IDs over time.',
    people.current_caseload_person_count AS COUNT_IF(people.has_current_recorded_caseload) COMMENT = 'People with an open referral in the dataset latest month, counted once across providers.',
    referrals.referral_count AS COUNT(referrals.source_record_id) COMMENT = 'Latest recorded referrals, including those without a person ID. Use referral_periods for historical state.',
    referrals.open_referral_count AS COUNT_IF(referrals.is_recorded_open) COMMENT = 'Referrals open in their provider''s latest submission.',
    referrals.referral_person_count AS COUNT(DISTINCT referrals.person_id) COMMENT = 'Distinct person IDs across the selected referrals. Do not add distinct counts across groups.',
    referrals.mean_days_to_first_attended_contact AS AVG(referrals.days_to_first_attended_contact) COMMENT = 'Average days from receipt to first attended contact among referrals with a valid observed interval. Not the average wait of everyone referred.',
    referrals.referrals_with_observed_first_attendance AS COUNT(referrals.days_to_first_attended_contact) COMMENT = 'Referrals with a valid receipt-to-first-attendance interval, the denominator of the mean.',
    contacts.contact_count AS COUNT(contacts.source_record_id) COMMENT = 'Latest referral/contact pairs in all attendance states. Use attended_contact_count for delivered care.',
    contacts.attended_contact_count AS COUNT_IF(contacts.is_attended) COMMENT = 'Contacts with attendance code 5 or 6.',
    contacts.dna_contact_count AS COUNT_IF(contacts.is_dna) COMMENT = 'Contacts with code 3 (did not attend) or 7 (arrived late, not seen).',
    contacts.cancelled_contact_count AS COUNT_IF(contacts.is_cancelled) COMMENT = 'Contacts cancelled by the patient (2) or provider (4). Their date is the scheduled date.',
    contacts.attended_contact_minutes AS SUM(IFF(contacts.is_attended, contacts.clinical_contact_duration_minutes, NULL)) COMMENT = 'Supplied clinical contact minutes on attended contacts. Duration is often missing; report coverage alongside.',
    contacts.contact_person_count AS COUNT(DISTINCT contacts.person_id) COMMENT = 'Distinct person IDs in the selected contacts. Do not add monthly or team-level distinct counts.',
    activities.care_activity_count AS COUNT(activities.source_record_id) COMMENT = 'Accepted activity occurrences; repeated monthly submissions of one activity count separately.',
    clinical.clinical_record_count AS COUNT(clinical.source_record_id) COMMENT = 'Retained clinical items. Filter clinical_record_type; assessment rows are responses, not questionnaires.',
    referral_periods.referral_period_count AS COUNT(referral_periods.referral_period_id) COMMENT = 'Referral/submission snapshots. The same referral appears each month; select one period for a snapshot.',
    referral_periods.open_referral_period_count AS COUNT_IF(referral_periods.is_recorded_open_at_period_end) COMMENT = 'Referral snapshots open at their period end. Select one reporting period.',
    caseload_referrals.current_caseload_referral_count AS COUNT(caseload_referrals.referral_period_id) COMMENT = 'Open referrals in the dataset latest month, including those without attendance or person ID.',
    caseload_referrals.caseload_referrals_without_attendance AS COUNT_IF(caseload_referrals.is_open_without_recorded_attendance) COMMENT = 'Open referrals in the latest month with no attended contact evidenced. Evidence of waiting, not waiting-list eligibility.',
    caseload_people.current_caseload_person_provider_count AS COUNT(caseload_people.caseload_person_provider_id) COMMENT = 'Person/provider combinations with an open referral in the latest month. A person at two providers counts twice.',
    caseload_people.caseload_distinct_person_count AS COUNT(DISTINCT caseload_people.person_id) COMMENT = 'Distinct people with an open referral in the latest month across the selected providers.',
    rtt.rtt_evidence_count AS COUNT(rtt.referral_to_treatment_period_id) COMMENT = 'Accepted clock records, including repeated monthly evidence of one clock. Not distinct clocks.',
    rtt.rtt_clock_count AS COUNT_IF(rtt.is_latest_clock_record) COMMENT = 'Distinct clocks, counted by their newest record.',
    rtt.two_hour_response_assessed_count AS COUNT_IF(rtt.is_two_hour_response_clock AND rtt.is_latest_clock_record AND rtt.is_response_standard_met IS NOT NULL) COMMENT = 'Two-hour urgent community response clocks (type 05) assessed against the standard, one per clock. Group by rtt_start_date month.',
    rtt.two_hour_response_met_count AS COUNT_IF(rtt.is_two_hour_response_clock AND rtt.is_latest_clock_record AND rtt.is_response_standard_met) COMMENT = 'Two-hour urgent community response clocks meeting the standard, one per clock. Divide by two_hour_response_assessed_count for the rate.',
    teams.service_team_relationship_count AS COUNT(teams.source_record_id) COMMENT = 'Latest referral/team relationships. A referral can have several.',
    demographics.person_provider_period_count AS COUNT(demographics.person_provider_period_id) COMMENT = 'Person/provider/period snapshots; not a distinct-person headcount.',
    submissions.accepted_submission_count AS SUM(submissions.n_accepted_submissions) COMMENT = 'Accepted files across the selected provider periods.',
    submissions.submitted_contact_row_count AS SUM(submissions.n_contact_source_records) COMMENT = 'Contact source rows across selected submissions, before latest-version selection. Reporting month is not contact month.'
)

COMMENT = 'CSDS community services referrals, contacts, activities, clinical records, recorded caseload, response clocks and provider submission freshness. Choose the entity matching the question; each has its own row meaning and date basis. Measures describe this extract, not all lifetime care or live operational state. Currency costing is outside this view.'
AI_SQL_GENERATION 'Return non-identifying aggregates only; never select person or patient identifiers. For West and North London figures filter the is_wnl_commissioner dimension of the table queried (commissioned activity) or the is_wnl_resident dimension (residents); about a quarter of recent activity is commissioned elsewhere. For current recorded caseload use caseload_referrals or caseload_people, which hold only the dataset latest month. For historical open referrals use referral_periods and filter one reporting period. A referral or team no longer submitted counts as closed. Use contact_date for contact activity and is_attended for delivered care. Team types are the delivering team for contacts; musculoskeletal and physiotherapy services are separate types, so include both for physiotherapy questions when appropriate. For period demographics use only the declared relationships from contacts, referral_periods, caseload_referrals, caseload_people or rtt to demographics; people dimensions describe current evidence. Aggregate each fact to the intended person or referral grain before combining facts. Check submissions for provider freshness before comparing providers. For the two-hour urgent community response rate divide two_hour_response_met_count by two_hour_response_assessed_count grouped by the month of rtt_start_date; do not use rtt_evidence_count, which repeats monthly, and do not include type 06, the two-day standard. Open referrals and clock records do not establish national waiting-list eligibility or target compliance. Assessment rows are responses, not completed questionnaires.'
AI_QUESTION_CATEGORIZATION 'Use this view for aggregate questions about CSDS community referrals and first recorded attendance, monthly recorded caseload, contacts by attendance, team type, practice and location, care activities, immunisations and assessments, two-hour urgent community response and other clocks, and provider submission freshness. Cost questions need the separate currency models. The view cannot establish live caseload, national waiting-list statistics or clinical outcomes.'
