# NHS Talking Therapies (IAPT data set) models

These models publish the NHS Talking Therapies for anxiety and depression data
set, still named IAPT in its specification and warehouse tables. Definitions
follow the
[IAPT v2.1 Enhanced Technical Output Specification v2.1.22](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/iapt/iapt-v2.1-docs/iapt_v_2.1_enhanced_technical_output_specification_v2.1.22.xlsx),
the v2.1 user guidance and the DARS output specification, with the v2.0.26
specification for older fields and codes. Accepted submissions run from
September 2020; those up to March 2022 are data set version 2.0. v2.0 submitted
the mental health source-of-referral list and consultation medium, v2.1 the
IAPT source-of-referral list and consultation mechanism. The warehouse copies
each renamed item into both its old and new columns. Staging keeps both; facts
choose the label list from the data set version.

## Where to start

- `fct_iapt_referral_summary` is the referral entry point. It holds each
  referral's status at the latest accepted month, NHS England's ended-referral
  type, reliable recovery, waits to first assessment and treatment, and the
  referral source, discharge reason and age groups.
- `fct_iapt_referral_period` gives open referrals and waits at the end of any
  accepted month. Filter one `reporting_period_end_date` for a snapshot.
- `fct_iapt_care_contact` is the contact entry point, with the NHS England
  attended-or-unplanned and treatment contact rules and a delivery group.
- `dq_iapt_provider_submission` says whether a provider's month can be
  trusted. Check it before comparing providers or months.
- `sem_iapt` is the semantic view over these and the activity, assessment and
  condition facts, with named NHS England measures.

## Initial scope

The models cover referrals, care contacts, onward referrals, care activities,
scored assessments and recorded conditions. They do not yet cover every IAPT
table. Raw models exist for the tables below, but there is no staging or fact
for them yet:

- IDS001 patient demographics and IDS002 GP registration. PDS and OLIDS remain
  the person sources.
- IDS003 accommodation, IDS004 employment, IDS007 disability, IDS011 social and
  personal circumstances and IDS012 overseas visitor charging.
- IDS108 waiting-time pauses, IDS205 internet-enabled therapy logs, IDS803 care
  clusters and IDS902 care personnel qualifications.

Add them when an analysis needs them, for example waiting-time pauses with the
first waiting-time measure.

## Loading

Providers submit a primary file for the latest month and a refresh file for
the month before; the refresh is the last chance to correct that month. In
September 2026 the cumulative accepted history held about 7,400 submissions,
one per provider and month, covering September 2020 to July 2026.

- `stg_iapt_activesubmission` is a small table rebuilt on each run from the
  accepted-submission list and its header. Every history model and clean-up
  step reads this one snapshot. Its tests fail if it is empty or if a provider
  and month appear twice. An unknown data set version fails validation so its
  code lists and definitions can be reviewed before modelling it.
- Each `stg_iapt_*_history` model keeps every accepted version of its source
  rows. It is incremental: a run inserts only accepted submissions that have a
  complete header and are not yet retained, replacing by `submission_id`.
- After each run, rows from submissions no longer accepted are deleted, for
  example a primary file replaced by its refresh. The delete is skipped when the
  snapshot is empty. A partly loaded but non-empty accepted list is an upstream
  completeness risk. The profile can reconcile the current list to retained
  history, but cannot detect a truncated list without an earlier baseline.
- Rows corrected or added inside a submission that is already retained are
  picked up at the monthly full refresh, not the daily run.
- All builds use the configured target warehouse, including full refreshes.
  The IAPT histories do not need the Large warehouse used for OLIDS.
- Facts are full-rebuild tables over the histories, refreshed weekly and at the
  monthly full refresh. Each keeps the newest version of each record, ordered by
  reporting month, file receipt time, submission identifier and submitted row
  identifier, with nulls last.

Incremental runs avoid rewriting the histories. They do not guarantee that raw
source scans are pruned.

`stg_iapt_submission_header` stages every IDS000 header, including Primary
files a Refresh replaced, so `dq_iapt_provider_submission` can compare each
accepted Refresh with its Primary. Header section totals cover the whole file;
for providers outside WNL the warehouse feed holds only WNL-commissioned rows,
so staging can hold fewer rows than the header reports.

### Submission caveats

- Truncated refresh files. In 17 provider-months the accepted Refresh holds
  under half the rows of the Primary it replaced in a section where the
  Primary held at least 100 (`has_refresh_section_shortfall`). The largest are
  all three West London NHS Trust services (RKL07, RKL14 and RKL42) in July
  2024, whose Refresh kept about 5% of the Primary's contacts and almost none
  of its activity assessments. Their referrals were resubmitted in full. Every
  model reads the accepted Refresh, so contact, activity and assessment counts
  for those months are understated. The Primary rows are not retained.
- Missing items at Central and North West London. Since moving to data set
  v2.1 in April 2022 the CNWL services (RV3 codes) have left the referral
  source blank for most new referrals (all of them in 2024 and 2025, about half
  in 2026), have sent no discharge reason since 2024, and left consultation
  mechanism blank from 2022 until mid-2025. They account for almost all
  provider-months flagged by
  `is_source_of_referral_mostly_missing`, `is_discharge_reason_mostly_missing`
  and `is_consultation_mechanism_mostly_missing`.

| Record | Fact key | Accepted versions | Records |
|---|---|---|---|
| Referral | Provider-qualified service request ID | 3.4 million | 0.92 million |
| Care contact | Reporting month, referral and provider-qualified contact ID | 3.5 million | 3.5 million |
| Care activity | Reporting month, referral, contact and provider-qualified activity ID | 3.0 million | 3.0 million |
| Onward referral | Referral, date, time, reason and receiving organisation | about 3,600 | about 3,600 |
| Activity assessment | Reporting month, referral, contact, activity and tool | 17.9 million | 17.9 million |
| Referral assessment | Referral, tool, completion date and time | 1.7 million | 1.7 million |

The specification rejects repeated onward referrals within a submission, yet
accepted data holds about 20 extra copies, each with the same person, provider,
referral and pathway as its original. The onward referral history keeps
every source row; the fact keeps one milestone per natural key.

Submitted row identifiers (`UniqueID_IDSnnn`, `RecordNumber`) change with every
submission and are never fact keys. `PathwayID` changes when the person is
re-traced. `RecordStartDate` and `RecordEndDate` are final only after the refresh
window and are not used.

Contact and activity identifiers can recur in another month for a different
dated contact. Their keys include the reporting month, which remains stable
when a primary submission is replaced by its refresh. Activities and activity
assessments link only to the matching submission's contact or activity.

`fct_iapt_referral` holds recorded values only. Its `referral_status` is
`closed` when a discharge date is recorded and otherwise `open`, as in
`fct_mhsds_referral`. `fct_iapt_referral_summary` adds the status as of the
latest accepted month (`as_of_date`), following the MHSDS and CSDS summaries.

An undischarged referral is `open` while its last reporting period is within
two months of `as_of_date`. After that it is `no_longer_submitted` and ends at
its last reporting period end. `referral_end_date_source = 'last_submission'`
marks the inferred end. This stops discontinued provider feeds leaving
referrals open indefinitely. `service_discharge_date` stays as submitted;
inferred ends create no discharge milestone. The rule is relative to the latest
accepted month, not today's date. A resumed submission can reopen a referral;
if the whole data feed stalls, statuses stay relative to that last month.
`provider_months_behind_dataset` and `is_in_latest_provider_period` show
whether a late provider, not the referral, explains a `no_longer_submitted`
status.

## Provider code changes

Referral identifiers are provider-qualified. When a provider's ODS code
changes, its open referrals are resubmitted under the new code with new local
and pathway identifiers and none of their earlier contacts. The old copy stops
being reported without a discharge. Left alone, the referral is counted as
received twice, the old copy looks abandoned, and NHS England's course fields
on the new copy (first treatment date, contact counts, first scores, completed
treatment, recovery) describe only the care after the change.

`int_iapt_referral_transfer` lists candidate pairs: the same Person_ID and
referral received date under different provider codes, the predecessor
undischarged, and the successor first reported in the month after the
predecessor was last reported. A referral with more than one candidate on
either side is left unlinked, so both identifiers are unique.

A matching person and date does not prove a code change. A code change moves a
provider's whole open caseload in one month, so a candidate is confirmed
(`is_confirmed_transfer`) only when at least 100 candidates share its
predecessor provider, successor provider and transition month. The rule names
no providers. The confirmed groups are the three known changes, each with about
150 to 2,700 pairs per provider pair and month: four services moving to North
London NHS Foundation Trust (G6V2S) in November 2024, the Camden and Islington
services exchanging codes (TAF87 and TAF88) in June and July 2024, and RWK4C
moving to RQY58 in December 2022. Together they hold about 11,000 pairs. The
other 70 or so candidates are spread over about 50 groups, none above a dozen,
and stay unconfirmed: they do not change status, chains or the event feed. A
referral can transfer twice, and about 260 did.

`fct_iapt_referral_summary` keeps both copies of a confirmed transfer as rows:

- `is_transfer_successor`, `is_transfer_predecessor`, `predecessor_referral_id`
  and `successor_referral_id` link them. `original_referral_id` is the first
  referral of the chain and has one value per course of care.
- The predecessor's `as_of_referral_status` is `transferred`, ending at its
  last reported month.
- `has_partial_nhse_course_fields` is true on a successor. The supplied NHS
  England values are unchanged on `fct_iapt_referral`.
- `pathway_first_assessment_date` and `pathway_first_treatment_date` take the
  earliest date across the chain, and `pathway_second_treatment_date` the second
  earliest treatment. The chain's first treatment is earlier than the
  successor's own for about 40% of successors, and a further 16% have no first
  treatment of their own. Waits in the summary use these chain dates.
- `fct_iapt_referral_period` counts care recorded under the earlier code, so a
  moved course is not shown as waiting again.

Exclude `is_transfer_successor` to count each referral received once.
Contacts, activities and assessments stay under the copy that reported them, so
a course of care that spans a transfer needs both referral identifiers.
`int_iapt_healthcare_event` emits no referral received milestone for a
confirmed successor. Its discharge milestone keeps the successor as source
record, and its contacts keep it as their referral parent.

## Reconciling to NHS England's publication

For services hosted in WNL, monthly figures from these facts match the
publication's primary-file method within about 1%. They will not reproduce a
past month exactly:

- The facts use the latest accepted file for each month, which is the refresh
  once it arrives. The publication's monthly figures come from the primary
  file. Staging does not retain a primary file after its refresh replaces it.
- A referral first reported late is dated to its true received month here, so
  earlier months gain referrals the publication never added.
- NHS England's published scripts filter on UsePathway_Flag, carried here as
  `is_nhse_use_pathway`. It is false for about 0.01% of referral versions.
- A transfer successor keeps its original received date, so counting referrals
  by received month counts it twice unless `is_transfer_successor` is excluded.

The facts are not limited to the WNL population. They hold all activity of
services hosted in WNL, whoever commissions it, and only WNL-commissioned
records from providers elsewhere. Filter `is_wnl_commissioner` for WNL
population figures. Do not compare an out-of-area provider's totals with the
publication, because most of that provider's activity is not in the feed.

## People

`person_id` is the IAPT Person_ID from the Master Person Service. It can be an
NHS number, a linkable unmatched-person ID or a one-off ID that cannot be
linked, and is only comparable within IAPT. `sk_patient_id` from
`stg_iapt_bridging` is the only key for joining to other datasets.

About 3,200 referrals carry more than one Person_ID across accepted versions;
about 400 of them map to more than one patient key, and none map all their
Person_IDs to one key. A changed Person_ID is therefore not treated as the same
person. Each record keeps the Person_ID of its own latest version, and a parent
is published only when it exists and agrees on referral, contact and person.

Within one accepted submission every care activity joins its contact, and
every activity assessment joins its activity, with no person or referral
mismatches.

## Attendance and outcomes

`fct_iapt_care_contact.is_attended` is the recorded attendance outcome.
`is_nhse_attended_or_unplanned` applies the NHS England counting rule, which
also counts unplanned contacts. `is_nhse_treatment_contact` applies the rule of
TreatmentCareContact_Count (I101D29): attended or unplanned, appointment type
02, 03 or 05, no employment support activity, and dated within the referral.
For discharged referrals without a transfer, counting these contacts matches
the supplied treatment count (net of internet-enabled therapy logs) for about
97%. `is_assessment_appointment_type` marks types 01 and 03.

The referral's `nhse_care_contact_count` and `nhse_treatment_contact_count` are
NHS England's supplied CareContact_Count and TreatmentCareContact_Count. Both
include unplanned contacts and internet-enabled therapy logs, so they are not
attended appointment counts.

NHS England's referral outcome flags are only ever true or null.
`fct_iapt_referral.is_recovered` is the recovery flag as supplied: true or null,
never false. `is_completed_treatment` is true from the source flag and false
only when a simple stated criterion fails: no discharge date, or fewer than two
treatment contacts. Otherwise it is null. Caseness and reliable-change statuses
are reported only for completed treatment.

Referral-level scores keep fractional values: PHQ-9, GAD-7, anxiety measure and
WSAS scores are parsed to 9 decimal places, as in `fct_iapt_assessment_score`.

### NHS England measures

The summary applies these NHS England monthly measure definitions to the
supplied referral derivations. Add `is_nhse_use_pathway` and the measure's date
window to reproduce a published figure.

| Field | NHS England measures | Rule |
|---|---|---|
| `ended_referral_type` | M073 to M076 | For a recorded discharge: `finished_course_treatment` when completed treatment is flagged, `treated_once` with one treatment contact, `seen_not_treated` with contacts but no treatment contact, `not_seen` with no contact |
| `is_reliably_recovered` | M193 over the M195 denominator | Recovered and reliably improved; FALSE for other finished courses not below caseness at the start; null outside that denominator |
| `days_referral_to_first_assessment` | M024 to M027 | Receipt to first assessment |
| `days_referral_to_first_treatment` | M032 to M035, M049 | Receipt to first treatment |
| `is_first_treatment_within_6_weeks`, `_18_weeks` | M036, M037 | 42 and 126 days or fewer |
| `days_first_to_second_treatment` | M046, M047 | First to second treatment |
| `consultation_mechanism_group` | M1001 to M1020 | Delivery group of attended or unplanned contacts |
| `fct_iapt_referral_period.waiting_state` | M029, M038 | Waiting for assessment or treatment at the period end |

A transfer successor's ended type and outcome flags cover only care after the
transfer, as NHS England derives them per provider-qualified referral; its
waits use the chain dates.

## Assessments

Scores are read against the latest published definition of each observable in
the IAPT routine outcome measure mapping. The specification version records
when a definition was published, not when a provider adopted it, and the data
cannot show which revision a v2.0 provider followed. The Diabetes Distress
Scale changed from a 17-102 total to a 1-6 mean in January 2021, inside data
set version 2.0. A value that fits only an earlier published range, such as a
DDS total, is kept as submitted with status `historical_published_range` and no
comparable score; about 130 of a few hundred DDS scores are of this kind. WSAS
work 9 and PEQ NA are non-score responses. Out-of-range values, accepted by the
source with a warning, stay visible with status `response_unmatched`.

## Labels and codes

Codes keep their submitted value next to a current UKHFD label.
`iapt_code_lookup` holds the IAPT-specific code sets: source of referral,
discharge reason, previous diagnosed condition, onward referral reason,
appointment type, psychotropic medication usage, short-notice cancellation,
integrated long-term condition service, presenting complaint coding
significance and statutory sick pay. It keeps retired codes;
`iapt_code_lookup_history` holds every UKHFD revision. Its category comes from
the newest revision that supplies one: UKHFD's 2022 revision of the discharge
reason list dropped every code's category and changed nothing else. Shared code
sets come from their existing lookups, such as `mhsds_source_of_referral` for
v2.0 referral sources and `consultation_mechanism`. A code absent from UKHFD
keeps a null label.

Two historical lists label codes that the current lists lack, and a code-set
column records which list supplied each label:

- Discharge reasons 40-45, 97 and 98 come from the retired IAPT care spell end
  code list, valid until March 2020 (`discharge_reason_legacy`). Examples are 42
  completed scheduled treatment and 44 referred to a non-IAPT service. The
  v2.0 specification deleted or replaced them in 2019 and published no
  equivalent current code. `discharge_reason_code_set` marks them. They keep
  their own UKHFD categories, assessed only or assessed and treated.
- Consultation codes use only their own version's list: consultation medium
  for v2.0 and mechanism for v2.1. A code valid only in the other version, such
  as 03, 06 or 08 sent in v2.1, keeps its code but no label, and
  `consultation_mechanism_label_status` is `other_version_code`.

### Groups

Discharge reason and referral source groups are UKHFD categories. Each has a
snake_case key (`_group`) and the category name (`_group_name`):

- `discharge_reason_group`: `referred_but_not_seen` (50),
  `seen_but_not_taken_on_for_a_course_of_treatment` (10-17 and 95) and
  `seen_and_taken_on_for_a_course_of_treatment` (46-49 and 96), the groups
  under I101250 in the ETOS. Retired care spell end codes keep `assessed_only`
  or `assessed_and_treated`; they are a fraction of a percent of discharges.
- `source_of_referral_group`: the nine categories of the v2.1 IAPT list, such
  as `self_referral` (B1, B2), `primary_health_care` (A1-A4) and `other`
  (M1-M8). v2.0 codes take the category of the retired mental health list they
  were submitted against. Keys ignore case and punctuation, so its
  "Self referral" and "Local Authority and Other Public Services:" join the
  v2.1 groups, and the name uses the v2.1 spelling. v2.0 categories with no v2.1
  counterpart, such as improving access to psychological therapies (N1-N3),
  keep their own. UKHFD gives no category to G4, I1, I2, N1, N2, N4, P1 and Q1
  in the v2.1 list, so those stay ungrouped. NHS England's monthly measures
  M002 to M018 split some categories more finely, for example general practice
  (M003) within primary health care; that grouping is not reproduced.

`iapt_code_group` is the maintained seed for groups UKHFD does not publish, one
row per code set and code with the document that defines the group:

- `consultation_mechanism_group`: `face_to_face`, `telephone`, `video`,
  `text_based`, `other` or `unknown`. Video is v2.0 code 03 (telemedicine) and
  v2.1 code 11, so it is continuous across versions. As in NHS England's M1009,
  missing codes and codes not valid for the contact's version are `unknown`.
- Anxiety measure tokens map to their score concept, which labels
  `anxiety_disorder_specific_measure_name` from `iapt_assessment_scale`.

The age band on referrals uses the project's NHS bands (`age_band_nhs` macro);
NHS England's publication bands age differently, at the period end.

Some submitted codes appear in no published list and keep a null label:
consultation mechanism CH and Si, psychotropic medication 00 and one onward
referral reason CH, plus a small number of v2.0 referral-source CH codes.
The source accepts invalid codes in these fields with a
warning only.

The contact fact publishes one consultation mechanism code, label and code
set. The warehouse copies that item into its legacy consultation-medium field;
staging retains both, but they are not separate analyst fields.

Missing codes are a larger limitation than missing labels. Since 2024 about
41-43% of recorded discharges lack a reason, concentrated in a few providers,
chiefly CNWL (see submission caveats).
About 32% of v2.1 referrals lack a referral source. Consultation mechanism is
missing for 28-41% of contacts in 2022-2024, falling below 1% in 2026. Accepted
refresh files have the same gaps as their replaced primary files. Comparisons
by referral source, discharge reason or delivery mechanism therefore describe
an incomplete and unevenly recorded population. `dq_iapt_provider_submission`
flags provider-months where most values are blank, and the aggregate profile
reports completeness by year.

Site names come from the shared organisation reference. About 660,000 contacts
use one of about 80 site codes it does not hold, mostly five-character codes, so
their `site_name` is null.

A procedure maps to an atomic SNOMED CT concept only when it is a bare concept
or a concept whose only refinement is procedure context "done". Offered,
planned or otherwise refined expressions keep the expression and a label that
names the context. ICD-10 codes keep their ICD-10 label, using the category
label for three-character codes; no ICD-10 to SNOMED CT map is applied.

Therapy categories use the terminology mapping guide, supplemented by the
technical output specification's explicit CBT definition for concept 228557008.
The reference seed records the source of each definition.

Condition types describe the submitted domain, not a positive disease flag.
Long-term condition rows include explicit absence of a condition and generic
history codes. Undated presenting complaints explicitly superseded by a later
dated version remain in the condition fact but are excluded from the clinical
record output. Other historical conditions remain; omission in a later month
does not establish clinical resolution. Multiple dated records of the same
complaint are distinct source items, not necessarily distinct conditions.

The current care activity supply contains procedures and observations, but no
populated clinical finding codes. The clinical union supports findings when
they are supplied.

## Time

A time is combined with its own date, never with the placeholder date stored in
the warehouse time column. Date-only records have `date` precision and a
midnight sort timestamp that is not an observed time. Undated records are kept
with `unknown` precision. Almost every contact has a time; about 28% of onward
referrals do not. IAPT referrals have no receipt or discharge time, and about
three quarters of accepted referral versions have no discharge date. Most
previous diagnoses are undated, and long-term conditions have no date item.

Some supplied assessment and onward-referral times are exactly midnight, with
a distribution consistent with system defaults. No specification rule proves
which are defaults, so they remain supplied timestamps. `timestamp` precision
means a time was supplied, not that its clinical accuracy has been established.

## Models

| Model | One row represents |
|---|---|
| [`fct_iapt_referral`](../models/reporting/mental_health/referrals/fct_iapt_referral.sql) | One referral, latest accepted version, recorded values only |
| [`fct_iapt_referral_summary`](../models/reporting/mental_health/referrals/fct_iapt_referral_summary.sql) | One referral with its status as of the latest accepted month, NHS England outcomes and waits, groups and provider-code transfer links |
| [`fct_iapt_referral_period`](../models/reporting/mental_health/referrals/fct_iapt_referral_period.sql) | One referral in one accepted reporting month, with its open and waiting state |
| [`dq_iapt_provider_submission`](../models/reporting/mental_health/quality/dq_iapt_provider_submission.sql) | One provider and accepted reporting month, with delivery, volume and completeness checks |
| [`sem_iapt`](../models/semantic/sem_iapt.sql) | Semantic view over the referral, contact, activity, assessment, condition, referral period and submission tables |
| [`fct_iapt_care_contact`](../models/reporting/mental_health/activity/fct_iapt_care_contact.sql) | One care contact within its referral and reporting month |
| [`fct_iapt_onward_referral`](../models/reporting/mental_health/referrals/fct_iapt_onward_referral.sql) | One onward referral milestone |
| [`fct_iapt_care_activity`](../models/reporting/mental_health/activity/fct_iapt_care_activity.sql) | One care activity within its referral, contact and reporting month, with its procedure, finding and observation |
| [`fct_iapt_assessment_score`](../models/reporting/mental_health/clinical/fct_iapt_assessment_score.sql) | One scored assessment question, dimension or total |
| [`fct_iapt_health_condition`](../models/reporting/mental_health/clinical/fct_iapt_health_condition.sql) | One previous diagnosis, long-term condition or presenting complaint |
| [`fct_iapt_clinical_record`](../models/reporting/mental_health/clinical/fct_iapt_clinical_record.sql) | One clinical item from the three clinical facts, in one list |
| [`int_iapt_referral_transfer`](../models/modelling/mental_health/referrals/int_iapt_referral_transfer.sql) | One candidate successor referral under a new provider code, with the referral it may continue and whether the transfer is confirmed |
| [`int_iapt_care_activity_timing`](../models/modelling/mental_health/activity/int_iapt_care_activity_timing.sql) | One accepted care activity version with the date and time of its contact |
| [`int_iapt_healthcare_event`](../models/modelling/mental_health/int_iapt_healthcare_event.sql) | One referral receipt, referral discharge, care contact or onward referral |
| [`int_iapt_person_clinical_record`](../models/modelling/mental_health/int_iapt_person_clinical_record.sql) | One clinical item from `fct_iapt_clinical_record` |

The history models and `stg_iapt_submission_header` are in
`models/staging/commissioning/iapt/`, and the code lookups are
`models/reference/data_dictionary/iapt_code_lookup.sql` and
`iapt_code_lookup_history.sql`. Consultation groups and anxiety measure
tokens are in the `iapt_code_group` seed.

The two `int_` feeds use the column shape of the other source feeds, are full
rebuilds and are clustered on patient key and sort timestamp. The event feed
carries the appointment type as `event_code` and the discharge or onward
referral reason as `outcome_code`. Fields without a shared column, such as the
receiving organisation, stay on the source fact: join `source_record_id` to the
model in `source_model_name`. `source_received_at` is the warehouse load time,
because files reach the warehouse a median of eight days after portal receipt
and a few dozen took more than 30 days.

`analyses/iapt_data_profile.sql` returns aggregate counts for submissions,
retained histories, grains, links, dates, patient keys, labels and assessment
status across staging, facts and feeds.

## Future integration with the cross-system models

Once `fct_person_healthcare_event` and `fct_person_clinical_record` are on main:

1. Add an IAPT branch to both unions and to `dq_person_activity_coverage`.
2. Pass the IAPT event code and outcome columns. Add nulls for columns IAPT
   lacks.
3. Add IAPT to the source-dataset and submission-period column descriptions,
   and `historical_published_range` to the assessment status description.
4. Keep procedure context out of the diagnosis qualifier column unless that
   column is renamed for general qualifiers.
5. Switch the feeds to the shared incremental configuration, watermarked on
   load time, with withdrawn-record removal per record type.

## Validation

DEV profiling in September 2026 covered about 7,400 accepted submissions, from
September 2020 to July 2026. The prepared outputs contain about 5.3 million care
milestones and 23.2 million clinical items. Their identifiers are unique, and
every dated item has a sort timestamp. Missing patient keys and undated
conditions remain in the outputs.

The published facts contain about 920,000 referrals, 3.5 million contacts, 3.0
million care activities, 3,600 onward referrals, 19.6 million assessment items
and 715,000 recorded conditions. Month-qualified keys retain contacts,
activities and assessment responses whose native identifiers are reused in
another month. The clinical output excludes about 5,000 superseded undated
complaints, which remain in the condition fact.

In the referral summary about 92% of referrals have a recorded discharge, 3.6%
are open, 3.2% are no longer submitted and 1.2% are confirmed transfer predecessors.
Before transfers were linked, those predecessors counted as no longer
submitted. Recorded discharge events are unchanged. No inferred end predates
its referral receipt.

The transfer model holds about 11,000 confirmed pairs and 70 unconfirmed
candidates, and is unique on both the successor and the predecessor. Linking
confirmed pairs removed the same number of referral received milestones from
the event feed and changed no other milestone count. Fact row counts did not
change.

In October 2026, for WNL-commissioned referrals discharged from April 2025 to
March 2026, about 33% finished a course of treatment, 28% had one treatment
contact, 3% were seen but not treated and 36% were not seen. Reliable recovery
was about 46% and recovery about 49%, close to the published national rates.
For first treatments in the same year the median wait was 15 days, with about
89% treated within 6 weeks and 98% within 18 weeks. `fct_iapt_referral_period`
holds about 3.5 million referral-months.

Validation included initial builds, a repeat incremental build, grain tests,
procedure-expression examples and published-parent integrity checks. A
controlled DEV test removed one retained onward-referral submission and added
an inactive synthetic batch. The next incremental run restored the missing
submission, removed the synthetic batch and matched the original table's
aggregate row count and content hash.

Both reference seeds were regenerated from the official workbooks and matched
the committed metadata. A full refresh of all nine histories on the configured
Medium warehouse completed with 32 staging tests in 35 seconds overall. Query
history confirmed all nine table builds used Medium; the slowest took 9.4 seconds.
The source profile and tests return aggregate evidence only.
