{{ config(materialized='semantic_view', schema='SEMANTIC') }}

TABLES(
    referrals AS {{ ref('fct_iapt_referral_summary') }}
        PRIMARY KEY (referral_id) COMMENT = 'NHS Talking Therapies referrals, one row per provider-qualified referral with its status at the latest accepted month, NHS England course outcomes, waits and referral source and discharge reason groups. A provider code change resubmits a referral under a new identifier; exclude transfer successors to count referrals received once.',
    contacts AS {{ ref('fct_iapt_care_contact') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Care contacts in all attendance states, one row per referral, contact and reporting month. Use the NHS England attended-or-unplanned and treatment contact flags for national counting rules.',
    activities AS {{ ref('fct_iapt_care_activity') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Care activities within contacts, with the procedure and therapy type recorded. Activities are detail of contacts, not separate encounters.',
    assessments AS {{ ref('fct_iapt_assessment_score') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Scored assessment items recorded at referral or activity level, one row per item. A row is one question, dimension or total, not a completed questionnaire.',
    conditions AS {{ ref('fct_iapt_health_condition') }}
        PRIMARY KEY (source_record_id) COMMENT = 'Previous diagnoses, long-term conditions and presenting complaints as recorded. Includes recorded absence and generic history codes, so rows are not disease prevalence.',
    referral_periods AS {{ ref('fct_iapt_referral_period') }}
        PRIMARY KEY (referral_period_id) COMMENT = 'Referral state in each accepted reporting month. Filter one reporting period for a snapshot of open referrals and waits. A referral no longer submitted has no later rows.',
    submissions AS {{ ref('dq_iapt_provider_submission') }}
        PRIMARY KEY (provider_organisation_code, reporting_period_end_date) COMMENT = 'Delivery, volume and completeness checks for each provider and accepted month, including Refresh files much smaller than the Primary they replaced. Check before comparing providers or months.'
)

RELATIONSHIPS(
    contacts (referral_id) REFERENCES referrals,
    activities (referral_id) REFERENCES referrals,
    assessments (referral_id) REFERENCES referrals,
    conditions (referral_id) REFERENCES referrals,
    referral_periods (referral_id) REFERENCES referrals,
    contacts (provider_organisation_code, reporting_period_end_date) REFERENCES submissions,
    referral_periods (provider_organisation_code, reporting_period_end_date) REFERENCES submissions
)

DIMENSIONS(
    referrals.referrals_provider_code AS provider_organisation_code COMMENT = 'Code of the provider reporting the referral.',
    referrals.referrals_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting the referral.',
    referrals.referrals_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The referral is commissioned by West and North London. Filter true for WNL population figures.',
    referrals.referral_received_month AS DATE_TRUNC('month', referral_received_date) COMMENT = 'Month the referral was received.',
    referrals.referral_discharge_month AS DATE_TRUNC('month', service_discharge_date) COMMENT = 'Month of the recorded discharge; null when none is recorded.',
    referrals.referral_status AS as_of_referral_status COMMENT = 'Status at the latest accepted month: open, discharged, transferred or no_longer_submitted.',
    referrals.referral_source_group AS source_of_referral_group_name COMMENT = 'NHS England referral source group, such as Self referral or General medical practice. Null when no source is recorded, which is common for some providers.',
    referrals.discharge_reason_group AS discharge_reason_group_name COMMENT = 'Discharge reason group: not assessed, seen but not taken on for a course of treatment, or seen and taken on. Null when no reason is recorded.',
    referrals.ended_referral_type AS ended_referral_type COMMENT = 'NHS England ended-referral group for discharged referrals: finished_course_treatment, treated_once, seen_not_treated or not_seen.',
    referrals.referral_age_band AS age_band_nhs_at_referral COMMENT = 'NHS age band at referral receipt.',
    referrals.referral_presenting_complaint AS presenting_complaint_higher_category COMMENT = 'NHS England presenting complaint category, such as Depression.',
    referrals.referral_first_therapy_category AS first_therapy_type_category COMMENT = 'Intensity of the first therapy: Low Intensity, High Intensity, Employment Support or Internet Enabled Therapy.',
    referrals.referral_anxiety_measure AS anxiety_disorder_specific_measure_name COMMENT = 'Anxiety measure NHS England uses for recovery, such as GAD-7.',
    referrals.is_transfer_successor AS is_transfer_successor COMMENT = 'The referral continues an earlier one after a provider code change. Exclude when counting referrals received.',
    referrals.referrals_is_nhse_use_pathway AS is_nhse_use_pathway COMMENT = 'NHS England UsePathway_Flag; filter true when reconciling to published figures.',
    contacts.contacts_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting the contact.',
    contacts.contacts_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The contact is commissioned by West and North London.',
    contacts.contact_month AS DATE_TRUNC('month', care_contact_date) COMMENT = 'Month of the contact date.',
    contacts.contact_mechanism_group AS consultation_mechanism_group_name COMMENT = 'Face to face, Telephone, Video, Text-based, Other or Unknown, aligned to NHS England measures M1001 to M1020.',
    contacts.contact_appointment_type AS appointment_type_name COMMENT = 'Appointment type, such as assessment or treatment.',
    contacts.contact_attendance AS attendance_name COMMENT = 'Attendance description.',
    activities.activity_therapy_category AS therapy_type_category COMMENT = 'Low Intensity, High Intensity or Employment Support for a national therapy concept; null otherwise.',
    activities.activity_procedure AS procedure_name COMMENT = 'Procedure preferred term.',
    activities.activity_month AS DATE_TRUNC('month', clinical_date) COMMENT = 'Month of the contact the activity belongs to.',
    assessments.assessment_tool AS assessment_tool_name COMMENT = 'Assessment tool, such as Patient Health Questionnaire-9 (PHQ-9).',
    assessments.assessment_source AS assessment_source COMMENT = 'referral or care_activity: the IDS606 or IDS607 table that recorded the item.',
    assessments.assessment_month AS DATE_TRUNC('month', clinical_date) COMMENT = 'Month the item was recorded.',
    assessments.assessment_response_status AS assessment_response_status COMMENT = 'Whether the submitted value matched the published range of the tool.',
    conditions.condition_type AS condition_record_type COMMENT = 'previous_diagnosis, long_term_condition or presenting_complaint.',
    conditions.condition_name AS code_name COMMENT = 'Label of the recorded condition code.',
    referral_periods.referral_periods_provider_name AS provider_organisation_name COMMENT = 'Name of the provider reporting the referral in this period.',
    referral_periods.referral_periods_is_wnl_commissioner AS is_wnl_commissioner COMMENT = 'The referral is commissioned by West and North London in this period.',
    referral_periods.referral_periods_period_end AS reporting_period_end_date COMMENT = 'Reporting period the state is evaluated at. Filter to one period for a snapshot.',
    referral_periods.referral_periods_waiting_state AS waiting_state COMMENT = 'ended, in_treatment, awaiting_treatment or awaiting_assessment at the period end.',
    referral_periods.referral_periods_is_open AS is_recorded_open_at_period_end COMMENT = 'Received by the period end with no discharge by then.',
    referral_periods.referral_periods_is_nhse_use_pathway AS is_nhse_use_pathway COMMENT = 'NHS England UsePathway_Flag on that month''s version.',
    submissions.submissions_provider_code AS provider_organisation_code COMMENT = 'Provider organisation code.',
    submissions.submissions_provider_name AS provider_organisation_name COMMENT = 'Provider organisation name.',
    submissions.submission_period_end AS reporting_period_end_date COMMENT = 'Accepted reporting month.',
    submissions.is_latest_provider_period AS is_latest_provider_period COMMENT = 'True on each provider''s latest accepted month.',
    submissions.provider_months_behind_dataset AS provider_months_behind_dataset COMMENT = 'Months between the provider''s latest accepted month and the dataset''s latest.',
    submissions.has_refresh_section_shortfall AS has_refresh_section_shortfall COMMENT = 'The accepted Refresh holds under half the rows of the Primary it replaced in a major section; that month''s activity is understated.',
    submissions.is_source_of_referral_mostly_missing AS is_source_of_referral_mostly_missing COMMENT = 'At least half of the month''s new referrals lack a source of referral.',
    submissions.is_discharge_reason_mostly_missing AS is_discharge_reason_mostly_missing COMMENT = 'At least half of the month''s discharges lack a discharge reason.',
    submissions.is_consultation_mechanism_mostly_missing AS is_consultation_mechanism_mostly_missing COMMENT = 'At least half of the month''s contacts lack a consultation mechanism.'
)

METRICS(
    referrals.referral_count AS COUNT(referrals.referral_id) COMMENT = 'Referral rows, including both copies of a transferred referral.',
    referrals.referrals_received_count AS COUNT_IF(NOT referrals.is_transfer_successor) COMMENT = 'Referrals received, counting a transferred referral once.',
    referrals.finished_course_count AS COUNT_IF(referrals.ended_referral_type = 'finished_course_treatment') COMMENT = 'Discharged referrals that finished a course of treatment (NHS England M076).',
    referrals.reliable_recovery_count AS COUNT_IF(referrals.is_reliably_recovered) COMMENT = 'Referrals finishing a course of treatment that reliably recovered (M193).',
    referrals.reliable_recovery_denominator AS COUNT(referrals.is_reliably_recovered) COMMENT = 'Referrals finishing a course of treatment, excluding those below caseness at the start (M195 denominator).',
    referrals.reliable_recovery_rate AS COUNT_IF(referrals.is_reliably_recovered) / NULLIF(COUNT(referrals.is_reliably_recovered), 0) COMMENT = 'Reliable recovery count divided by its denominator.',
    referrals.first_treatment_count AS COUNT(referrals.days_referral_to_first_treatment) COMMENT = 'Referrals with a first treatment, the denominator for treatment waits.',
    referrals.first_treatment_within_6_weeks_count AS COUNT_IF(referrals.is_first_treatment_within_6_weeks) COMMENT = 'Referrals first treated within 42 days of receipt (M036).',
    referrals.first_treatment_within_18_weeks_count AS COUNT_IF(referrals.is_first_treatment_within_18_weeks) COMMENT = 'Referrals first treated within 126 days of receipt (M037).',
    referrals.median_days_to_first_treatment AS MEDIAN(referrals.days_referral_to_first_treatment) COMMENT = 'Median days from receipt to first treatment among referrals with one (M049).',
    contacts.contact_count AS COUNT(contacts.source_record_id) COMMENT = 'Contacts in all attendance states.',
    contacts.attended_contact_count AS COUNT_IF(contacts.is_attended) COMMENT = 'Contacts with attendance code 5 or 6.',
    contacts.nhse_attended_or_unplanned_count AS COUNT_IF(contacts.is_nhse_attended_or_unplanned) COMMENT = 'Contacts attended or unplanned, the NHS England contact counting rule.',
    contacts.nhse_treatment_contact_count AS COUNT_IF(contacts.is_nhse_treatment_contact) COMMENT = 'NHS England treatment contacts: attended or unplanned treatment appointments within the referral, without employment support.',
    activities.care_activity_count AS COUNT(activities.source_record_id) COMMENT = 'Care activities.',
    assessments.assessment_item_count AS COUNT(assessments.source_record_id) COMMENT = 'Scored assessment items; filter assessment_tool, as items are not questionnaires.',
    conditions.condition_record_count AS COUNT(conditions.source_record_id) COMMENT = 'Recorded condition items; filter condition_type.',
    referral_periods.referral_period_count AS COUNT(referral_periods.referral_period_id) COMMENT = 'Referral and month snapshots; select one reporting period.',
    referral_periods.open_referral_period_count AS COUNT_IF(referral_periods.is_recorded_open_at_period_end) COMMENT = 'Referrals open at their period end; select one reporting period.',
    referral_periods.waiting_for_treatment_count AS COUNT_IF(referral_periods.waiting_state IN ('awaiting_assessment', 'awaiting_treatment')) COMMENT = 'Open referrals with no first treatment at the period end (NHS England M038 before its use-pathway filter).',
    referral_periods.median_days_waiting AS MEDIAN(referral_periods.days_waiting_at_period_end) COMMENT = 'Median days waited at the period end by referrals awaiting assessment or treatment.',
    submissions.accepted_submission_count AS COUNT(submissions.submission_id) COMMENT = 'Accepted provider months.',
    submissions.refresh_shortfall_count AS COUNT_IF(submissions.has_refresh_section_shortfall) COMMENT = 'Provider months whose accepted Refresh is much smaller than its Primary.',
    submissions.submitted_contact_row_count AS SUM(submissions.n_contact_source_records) COMMENT = 'Contact rows retained across selected submissions, before latest-version selection.'
)

COMMENT = 'NHS Talking Therapies (IAPT data set) referrals, contacts, care activities, assessment items, recorded conditions, monthly referral state and provider submission quality. Choose the entity matching the question; each has its own row meaning and date basis.'
AI_SQL_GENERATION 'Return non-identifying aggregates only; never select person or patient identifiers. For West and North London population figures filter is_wnl_commissioner on referrals, contacts or referral_periods. Count referrals received with referrals_received_count, which counts a referral moved by a provider code change once. For outcomes use finished_course_count, reliable_recovery_count and reliable_recovery_denominator grouped by referral_discharge_month; for waits use first_treatment_within_6_weeks_count and first_treatment_within_18_weeks_count over first_treatment_count. For open referrals and waiting lists use referral_periods filtered to one reporting period. Before comparing providers or months, check submissions: a true has_refresh_section_shortfall means that month is understated, and the mostly_missing flags mean referral source, discharge reason or consultation mechanism comparisons exclude most of that provider''s records. Group by month dimensions rather than daily dates. Assessment rows are items, not questionnaires; filter assessment_tool.'
AI_QUESTION_CATEGORIZATION 'Use this view for aggregate questions about NHS Talking Therapies (IAPT) referrals, referral sources, discharge reasons, waits to first treatment, finished courses and reliable recovery, contacts by delivery mechanism, therapy types, assessment items, recorded conditions, monthly open referrals and provider data quality. It cannot identify individuals or describe care outside the IAPT data set.'
