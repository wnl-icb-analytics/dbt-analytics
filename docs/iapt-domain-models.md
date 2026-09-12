# NHS Talking Therapies (IAPT data set) models

These models publish the NHS Talking Therapies for anxiety and depression data
set, still named IAPT in its specification and warehouse tables. Definitions
follow the
[IAPT v2.1 Enhanced Technical Output Specification v2.1.22](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/iapt/iapt-v2.1-docs/iapt_v_2.1_enhanced_technical_output_specification_v2.1.22.xlsx),
the v2.1 user guidance and the DARS output specification, with the v2.0.26
specification for older fields and codes. Accepted submissions run from
September 2020; those up to March 2022 are data set version 2.0. v2.0 submitted
the mental health source-of-referral list and consultation medium, v2.1 the
IAPT source-of-referral list and consultation mechanism. Staging keeps both.

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
September 2026 about 7,400 submissions were accepted, one per provider and
month.

- `stg_iapt_activesubmission` is a small table rebuilt on each run from the
  accepted-submission list and its header. Every history model and clean-up
  step reads this one snapshot. Its tests fail if it is empty or if a provider
  and month appear twice.
- Each `stg_iapt_*_history` model keeps every accepted version of its source
  rows. It is incremental: a run inserts only accepted submissions that have a
  complete header and are not yet retained, replacing by `submission_id`.
- After each run, rows from submissions no longer accepted are deleted, for
  example a primary file replaced by its refresh. The delete is skipped when the
  snapshot is empty. A partly loaded but non-empty accepted list is an upstream
  completeness risk; the aggregate profile checks for it.
- Rows corrected or added inside a submission that is already retained are
  picked up at the monthly full refresh, not the daily run.
- Initial and full-refresh history builds switch to a larger warehouse only
  when run by the `DBT_ADMIN` role; other runs use their target warehouse.
- Facts are full-rebuild tables over the histories, refreshed weekly and at the
  monthly full refresh. Each keeps the newest version of each record, ordered by
  reporting month, file receipt time, submission identifier and submitted row
  identifier, with nulls last.

Incremental runs avoid rewriting the histories. They do not guarantee that raw
source scans are pruned.

| Record | Fact key | Accepted versions | Records |
|---|---|---|---|
| Referral | Provider-qualified service request ID | 3.4 million | 0.92 million |
| Care contact | Referral and provider-qualified contact ID | 3.5 million | 3.5 million |
| Care activity | Referral, contact and provider-qualified activity ID | 3.0 million | 3.0 million |
| Onward referral | Referral, date, time, reason and receiving organisation | about 3,600 | about 3,600 |
| Activity assessment | Referral, contact, activity and tool | 17.9 million | 17.9 million |
| Referral assessment | Referral, tool, completion date and time | 1.7 million | 1.7 million |

The specification rejects repeated onward referrals within a submission, yet
accepted data holds 20 extra copies across 19 natural-key groups, all with the
same person, provider, referral and pathway. The onward referral history keeps
every source row; the fact keeps one milestone per natural key.

Submitted row identifiers (`UniqueID_IDSnnn`, `RecordNumber`) change with every
submission and are never fact keys. `PathwayID` changes when the person is
re-traced. `RecordStartDate` and `RecordEndDate` are final only after the refresh
window and are not used. A record missing from a later submission is not
treated as closed; an open referral is the latest state received.

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
also counts unplanned contacts.

NHS England's referral outcome flags are only ever true or null.
`fct_iapt_referral.is_recovered` is the recovery flag as supplied: true or null,
never false. `is_completed_treatment` is true from the source flag and false
only when a simple stated criterion fails: no discharge date, or fewer than two
treatment contacts. Otherwise it is null. Caseness and reliable-change statuses
are reported only for completed treatment.

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
integrated long-term condition service and presenting complaint coding
significance. It keeps retired codes; `iapt_code_lookup_history` holds every
UKHFD revision. Shared code sets come from their existing lookups. A code
absent from UKHFD keeps a null label.

Two historical lists label codes that the current lists lack, and a code-set
column records which list supplied each label:

- Discharge reasons 40-45, 97 and 98 come from the retired IAPT care spell end
  code list, valid until March 2020 (`discharge_reason_legacy`). Examples are 42
  completed scheduled treatment and 44 referred to a non-IAPT service. The
  v2.0 specification deleted or replaced them in 2019 and published no
  equivalent current code. `discharge_reason_code_set` marks them, and they have
  no discharge category.
- Consultation mechanism 06 (SMS text messaging) and 08 (online instant
  messaging) are consultation medium codes from the list v2.1 replaced. The
  warehouse holds them in the mechanism field, almost all for v2.0 contacts.
  They take the medium label only when the contact's consultation medium field
  holds the same code; `consultation_mechanism_code_set` marks them.

Some submitted codes appear in no published list and keep a null label:
consultation mechanism CH and Si, psychotropic medication 00 and one onward
referral reason CH. The source accepts invalid codes in these fields with a
warning only.

Site names come from the shared organisation reference. About 660,000 contacts
use one of about 80 site codes it does not hold, mostly five-character codes, so
their `site_name` is null.

A procedure maps to an atomic SNOMED CT concept only when it is a bare concept
or a concept whose only refinement is procedure context "done". Offered,
planned or otherwise refined expressions keep the expression and a label that
names the context. ICD-10 codes keep their ICD-10 label, using the category
label for three-character codes; no ICD-10 to SNOMED CT map is applied.

## Time

A time is combined with its own date, never with the placeholder date stored in
the warehouse time column. Date-only records have `date` precision and a
midnight sort timestamp that is not an observed time. Undated records are kept
with `unknown` precision. Almost every contact has a time; about 28% of onward
referrals do not. IAPT referrals have no receipt or discharge time, and about
three quarters of accepted referral versions have no discharge date. Most
previous diagnoses are undated, and long-term conditions have no date item.

## Models

| Model | One row represents |
|---|---|
| [`fct_iapt_referral`](../models/reporting/mental_health/fct_iapt_referral.sql) | One referral, latest accepted version |
| [`fct_iapt_care_contact`](../models/reporting/mental_health/fct_iapt_care_contact.sql) | One care contact within its referral |
| [`fct_iapt_onward_referral`](../models/reporting/mental_health/fct_iapt_onward_referral.sql) | One onward referral milestone |
| [`fct_iapt_care_activity`](../models/reporting/mental_health/fct_iapt_care_activity.sql) | One care activity with its procedure, finding and observation |
| [`fct_iapt_assessment_score`](../models/reporting/mental_health/fct_iapt_assessment_score.sql) | One scored assessment question, dimension or total |
| [`fct_iapt_health_condition`](../models/reporting/mental_health/fct_iapt_health_condition.sql) | One previous diagnosis, long-term condition or presenting complaint |
| [`fct_iapt_clinical_record`](../models/reporting/mental_health/fct_iapt_clinical_record.sql) | One clinical item from the three clinical facts, in one list |
| [`int_iapt_healthcare_event`](../models/modelling/mental_health/int_iapt_healthcare_event.sql) | One referral receipt, referral discharge, care contact or onward referral |
| [`int_iapt_person_clinical_record`](../models/modelling/mental_health/int_iapt_person_clinical_record.sql) | One clinical item from `fct_iapt_clinical_record` |

The history models are in `models/staging/commissioning/iapt/`, and the code
lookups are `models/reference/data_dictionary/iapt_code_lookup.sql` and
`iapt_code_lookup_history.sql`.

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

DEV profiling on 11-12 September 2026 covered all 7,430 accepted submissions,
from September 2020 to July 2026. The prepared outputs contain 5,316,409 care
milestones and 23,155,911 clinical items. Their identifiers are unique, and
every dated item has a sort timestamp. Missing patient keys and undated
conditions remain in the outputs.

The published facts contain 920,139 referrals, 3,546,309 contacts, 3,023,174
care activities, 3,623 onward referrals, 19,587,619 assessment items and
714,966 recorded conditions. Composite contact and activity keys preserve
records whose native identifier appears under a different referral or contact.

Validation included initial builds, a repeat incremental build, grain tests,
procedure-expression examples and published-parent integrity checks. A
controlled DEV test removed one retained onward-referral submission and added
an inactive synthetic batch. The next incremental run restored the missing
submission, removed the synthetic batch and exactly matched the original
table's aggregate row count and content hash.

Both reference seeds were regenerated from the official workbooks and matched
the committed metadata. Query history confirmed that initial history builds
used the large warehouse and subsequent incremental writes used the target
warehouse. The source profile and tests return aggregate evidence only.
