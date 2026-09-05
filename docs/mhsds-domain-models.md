# MHSDS domain models

The source facts and relationships preserve recorded MHSDS events and links,
but their columns and names are designed for analysts. They do not reproduce
every numbered source table or every source field. Derived models support
analysis and provide inputs to cross-system event and clinical record models.

Definitions follow the current
[MHSDS v6 ETOS](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance).
Historical field mappings are checked against the archived v4.1 and v5 ETOS.
Records retain their submitted MHSDS version because fields and relationships
change between versions.

## How monthly submissions and repeated rows are handled

### 1. Choose the accepted file for each provider-month

Providers can submit more than one file for the same reporting month. Monthly
submission tables are first joined to `stg_mhsds_activesubmission`, which names
the file accepted for each provider and month. This prevents an earlier version
of the whole file being counted alongside its replacement.

This first step does not remove records repeated in later months. The person
bridge is a separate identity-linking feed and does not use the monthly file
rule.

### 2. Decide what a repeat means for that table

What happens next depends on the type of table:

| Type of table | Rows retained by our models | Examples |
|---|---|---|
| Records updated in later submissions | The newest reported version of each record. | Referrals, care contacts, referral-team relationships, legal-status periods, hospital spells and ward stays. |
| Monthly history | Rows from every accepted month. | MHS001 patient history, MHS005 patient indicators, MHS204 indirect activity and MHS902/MHS903 reference snapshots. |
| Repeated rows needing a table-specific rule | One row using identifiers and ordering defined for that use. | Currency diagnosis uses referral and timestamp; clinical diagnosis also retains scheme, code and person boundaries. MHS903 uses provider, submission and ward code. |

The choice is made from the meaning of the source table. A referral, contact,
legal-status period, spell or ward stay can be reported again with corrected or
newly completed fields. Those rows are versions of the same record. MHS001 and
MHS902 instead describe what was submitted for a month, so their earlier rows
remain useful history.

### 3. Select the newest submitted version where records are updated

Models first identify rows that represent the same record. They then order
those rows by:

1. reporting-period end date, newest first;
2. file receipt timestamp (`effective_from`), newest first;
3. submission identifier, highest first; and
4. source row order, last row in the file first, then the submitted-row
   identifier where another tie-break is needed.

The reporting month and receipt time determine which version is newer. The
remaining fields make the result repeatable when those dates are equal; they do
not imply that one row is clinically more reliable. Source row order runs
descending, treating the last row for a key in a file as the later correction.
Currency diagnosis is the exception: when diagnosis timestamps are equal it reads
source record and row numbers ascending, following the NHS England grouper
order.

The `select_latest_mhsds_record` macro applies this ordering where tables share
the same pattern. Models with table-specific matching rules apply the same
first three steps directly in their SQL.

`MHSxxxUniqID` fields identify individual submitted rows, so they cannot usually
link versions across months. Referral, contact, spell and ward-stay identifiers
are used where the source provides them.

Spell and ward-stay identifiers are unique within an accepted submission, but
the available history contains rare cases where a provider reuses an identifier
for different people or start dates. The staging and fact models treat these as
revisions and retain the newest source state. They are therefore suitable for
analysis of the latest reported record, but they do not guarantee a complete
lifetime event history where an identifier has been reused.

### 4. Rules applied by the current staging models

| Staging model | Rows treated as the same record | Rows retained |
|---|---|---|
| `stg_mhsds_referral` | Same unique service request | Newest submitted referral |
| `stg_mhsds_carecontact` | Same reporting period, service request and unique care contact | Every accepted period record; `int_mhsds_latest_care_contact` selects the newest state for existing contact models |
| `stg_mhsds_care_activity` | Same reporting period and unique care activity | Every accepted MHS202 activity; identifiers are reused between periods |
| `stg_mhsds_staff_activity` | Same submission, care activity and professional | Every accepted v6 MHS206 relationship, including rows whose parent was not received |
| `stg_mhsds_staff_details` | Same submission and professional | One MHS901 snapshot; duplicate source groups retain the last source row and a quality flag |
| `stg_mhsds_other_service_or_team_type` | Same referral and available service or team identifier | Newest relationship; rows without a service or team identifier are excluded |
| `stg_mhsds_servicetype` | Same referral | Newest referral service information, supplemented from MHS101, MHS102 and MHS902 |
| `stg_mhsds_mhactperiod` | Same unique Mental Health Act episode | Newest submitted legal-status period |
| `stg_mhsds_spell` | Same unique hospital-provider spell number | Newest submitted spell, including a later discharge |
| `stg_mhsds_mhs502wardstay` | Same unique ward-stay identifier | Newest submitted ward stay, including a later end date |
| `int_mhsds_currency_primary_diagnosis` | Same referral and diagnosis timestamp | Currency-scoped primary diagnosis using NHS England grouper ordering; rows without a timestamp are excluded |
| `stg_mhsds_mpi_history` | No matching across months | Every MHS001 row from accepted files, identified by its submitted-row id |
| `stg_mhsds_patientindicators` | No matching across months | Every MHS005 row from accepted files because the reporting period is its only time reference |
| `stg_mhsds_indirectactivity` | No matching across months | Every MHS204 row from accepted files because it is activity for that reporting period |
| `stg_mhsds_service_or_team_details` | No matching across months | Every MHS902 team snapshot from accepted files |
| `stg_mhsds_mhs903warddetails` | Same provider, submission and ward code | One ward definition within each accepted file; ward snapshots remain separate across months |

Each model's YAML description states what one row represents, and its tests
check the identifiers expected to be unique after these rules are applied.

### 5. Treat later absence separately from correction

This process gives the newest version seen in an accepted file. MHSDS does not
provide one deletion marker that works across all tables, so a record missing
from a later submission is not automatically treated as deleted. Models for
current occupancy or status also use recorded end dates and recent submission
evidence. An apparently open record is therefore the latest state received, not
proof that the provider still considers it open today.

### MHS001 patient identity needs separate handling

MHS001 is monthly patient history, not a one-row-per-person dimension.
`stg_mhsds_mpi_history` retains every MHS001 row from accepted submissions.
Joining it to activity by `person_id` alone will multiply rows across reporting
periods. Use `stg_mhsds_bridging` for the one-row-per-person link to
`sk_patient_id`, and use `dim_person_mh_profile` for a person-level analytical
summary.

Accepted files from January 2016 to July 2026 contain 16.4 million MHS001
snapshot rows for 1.05 million distinct MHS001 person identifiers. Almost all
of that volume is monthly history: there are 16.39 million distinct
provider-local-patient months. The remaining 9,490 rows repeat that grain and
contain the conflicting identifiers described below. The 2.81 million people
in `stg_mhsds_bridging` are the wider MHSDS-linked population, not an MHS001
patient count.

Some accepted MHS001 files contain more than one row for the same extended
local patient identifier. The rows can disagree on `person_id` and the
pseudonymised NHS number, so source order cannot identify an authoritative row.
The history model retains them and labels the submitted pseudonym separately
from the cross-system patient key supplied by the bridge.

## Date and time precision

Source times arrive attached to the placeholder date `1970-01-01`; staging
keeps only the recorded time. Published `*_at` fields combine the source date
and time. Where no time was supplied, the timestamp uses midnight and the
matching `*_time_precision` field is `date`. A recorded time uses `timestamp`.
Use the precision field to distinguish a date-only record from an event
actually recorded at midnight.

Source dates before 1901 are Excel-epoch missing-value sentinels rather than
plausible clinical dates and are exposed as null.

Reference models supply current UKHFD descriptions for submitted codes. Their
`_history` models retain definition revisions. This includes referral, contact,
language, safeguarding and inpatient codes. The inpatient lookup combines
current admission and discharge dictionaries with their retired predecessors,
so historical MHSDS records keep useful labels. It selects the latest definition
for each code, including a retired definition, and prefers the current dictionary
when current and predecessor dictionaries contain the same code. Descriptions
remain null when a submitted code is absent from UKHFD. The specialised mental
health service reference list is published outside UKHFD, so the ward-stay fact
exposes the source-derived name explicitly rather than presenting it as a UKHFD
label.

Care professional staff group, main specialty, registration body and job role
labels come from UKHFD. Their history model retains every UKHFD revision, and
the current lookup keeps the newest definition for each code even when the
concept is retired. NHS Occupation Codes are not available in UKHFD, so the
occupation labels come from the NHS England Occupation Code Manual v22.1. The
seed includes valid and retired codes and is generated by a checked-in script.
Descriptions remain null for invalid submitted values rather than assigning a
plausible label to an unknown code.

The care activity fact retains its procedure, finding, observation and unit
fields for the clinical-record model. Procedure descriptions use SNOMED CT.
Finding and observation descriptions use the submitted scheme, while retired
Read codes also expose the source-pipeline SNOMED CT mapping. Finding and
observation schemes come from UKHFD. Unit names use the warehouse unit
dictionary and its known aliases. Each field has a label-status column so
analysts can distinguish missing codes, invalid values, post-coordinated SNOMED
expressions and coding systems without a local reference.

## Published interfaces

The clinical-record fact combines recorded diagnoses, assessment questions or
dimensions, and populated care-activity components. It keeps different codes
at the same time, unlike currency diagnosis selection. Repeated dated diagnoses
use their latest accepted row, with person identity included to avoid combining
different people's histories. Undated records and assessment occurrences remain
separate. First and last reporting periods describe submission evidence, not
clinical onset or resolution.

`fct_mhsds_clinical_record` is for clinical-item and outcome analysis, not
encounter counts or questionnaire counts. Use `clinical_record_type` and
`clinical_label_status` when interpreting descriptions. Assessment values remain
as submitted text alongside a NUMBER(38,9) value and parse status. MHS606 retains
the assessor identifiers needed for same-clinician pairing. MHS607 uses its
same-submission care activity for time and flags conflicting recorded links.

See the [clinical-record design and validation](mhsds-clinical-record-plan.md).

| Model | One row represents | Use |
|---|---|---|
| [`fct_mhsds_referral`](../models/reporting/mental_health/fct_mhsds_referral.sql) | One unique service request | Referral receipt, decision, discharge planning, rejection, closure, organisations and primary service context. |
| [`rel_mhsds_referral_service_team`](../models/reporting/mental_health/rel_mhsds_referral_service_team.sql) | One referral, relationship role and service or team | Primary, referred-to and additional teams without multiplying the referral fact. |
| [`fct_mhsds_care_contact`](../models/reporting/mental_health/fct_mhsds_care_contact.sql) | One unique service request and care contact | Patient contact timing, attendance, delivery method, location, booking, accessibility and service context. |
| [`fct_mhsds_care_activity`](../models/reporting/mental_health/fct_mhsds_care_activity.sql) | One provider reporting period and care activity | Same-submission contact timing, service and commissioner context, duration and populated clinical-component flags. |
| [`fct_mhsds_clinical_record`](../models/reporting/mental_health/fct_mhsds_clinical_record.sql) | One retained diagnosis, assessment question/dimension or activity component | Submitted codes and labels, values, direct relationships, time provenance and source-quality flags. |
| [`dim_mhsds_care_professional_period`](../models/reporting/mental_health/dim_mhsds_care_professional_period.sql) | One accepted reporting period and care professional | Period-specific staff group, specialty, occupation and job role without applying current details to historical activity. |
| [`rel_mhsds_care_activity_staff`](../models/reporting/mental_health/rel_mhsds_care_activity_staff.sql) | One recorded care activity and professional relationship | Pre-v6 direct MHS202 staff and v6 MHS206 many-to-many relationships, including missing-parent states. |
| [`fct_mhsds_hospital_provider_spell`](../models/reporting/mental_health/fct_mhsds_hospital_provider_spell.sql) | One provider-qualified hospital spell | Recorded admission and discharge, route, planned discharge, provider, commissioner and referral context. Missing discharge is recorded source state, not current occupancy. |
| [`fct_mhsds_ward_stay`](../models/reporting/mental_health/fct_mhsds_ward_stay.sql) | One provider-qualified ward stay | Recorded movement through wards, timings, site, ward characteristics, admitted-patient classification, specialised service and distance from home. |
| [`fct_mhsds_referral_episodes`](../models/reporting/mental_health/fct_mhsds_referral_episodes.sql) | One referral | Currency-scoped summary of contacts, indirect activity and inpatient use. Use the source facts when individual records are needed. |
| [`fct_mhsds_currency_contacts`](../models/reporting/mental_health/currencies/fct_mhsds_currency_contacts.sql) | One referral and care contact | Mental health contact currency and proxy cost. |
| [`fct_mhsds_currency_bed_days`](../models/reporting/mental_health/currencies/fct_mhsds_currency_bed_days.sql) | One hospital spell and financial year | Mental health bed-day currency and proxy cost. |
| [`fct_mhsds_current_inpatients`](../models/reporting/mental_health/currencies/fct_mhsds_current_inpatients.sql) | One person with a current spell | Current occupancy using the latest active-submission evidence. |
| [`dim_person_mh_profile`](../models/reporting/mental_health/dim_person_mh_profile.sql) | One MHSDS person | Person-level summary built from the referral, contact and current-inpatient facts, with existing diagnosis, Mental Health Act and safeguarding models. |

Join a contact to its recorded referral using
`fct_mhsds_care_contact.referral_source_record_id =
fct_mhsds_referral.source_record_id`. A valid contact can refer to a service
request absent from the retained referral population, so use a left join.

MHS501 renamed five admission and discharge fields in version 5. The spell fact
uses the general hospital-provider-spell fields through version 4.1 and the
mental-health-specific fields from version 5. The source pipeline duplicates
the values into both field sets, so specification version decides the source.

Join a ward stay to its retained spell using
`fct_mhsds_ward_stay.hospital_provider_spell_source_record_id =
fct_mhsds_hospital_provider_spell.source_record_id`. The ward-stay fact retains
rows whose recorded parent is absent and reports the link state. Before version
6 it uses ward characteristics submitted on MHS502. From version 6 it uses the
MHS903 record with the same specification-defined ward key in the accepted
submission. Source-pipeline convenience fields do not decide this mapping.

MHS903 is monthly ward configuration and capacity, not patient activity.
Available and closed bed days are not copied onto ward stays because doing so
would invite false sums. The current feed populates those measures on only a
minority of ward snapshots, so MHS903 remains supporting staging. A future
capacity model should have an explicit provider, ward and reporting-month
grain and a confirmed analytical use.

The referral and contact facts retain the source-derived ICB commissioner and
publish a resolved ICB commissioner. When the source derivation is blank but
the submitted commissioner has the ODS ICB role, the resolved fields use the
submitted commissioner. `icb_commissioner_derivation_method` identifies which
route supplied the value.

`is_wnl_commissioner` provides a present-day footprint filter. It includes the
current WNL ICB, the legacy NCL and NWL ICB and sub-ICB codes, and their 13
predecessor CCGs. It does not classify the London Commissioning Hub as WNL.

MHS202 activity identifiers are unique within a reporting period, not across
the received history. The care activity fact therefore includes provider and
reporting period in its key. It uses the MHS201 parent from the same accepted
submission for timing and service context. Using the latest contact state would
attach a different person's contact to 1,247 historical activities in the
current data. The same-submission join has no person mismatches and leaves 303
activities with a visible missing-parent state.

Before v6, MHS202 held one care professional directly. V6 moved the
many-to-many relationship to MHS206. The relationship model combines both
routes and never fills a missing historical MHS901 row from a later reporting
period. This matters because 7,808 received MHS206 relationships without a
same-submission professional snapshot have that professional in another
period, while 4,802 have no MHS901 snapshot in the accepted history.

## Nearby source facts

The [MHSDS analytical-domain plan](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1028)
organises the remaining source records around analytical subjects rather than
publishing one model for every numbered source table.

| Source | Analytical model or decision | One row represents and use |
|---|---|---|
| MHS103 Other Reason for Referral | `rel_mhsds_referral_reason` | One referral and additional-reason code. The primary reason remains on the referral fact. |
| MHS104 Referral to Treatment | `fct_mhsds_referral_to_treatment_period` | One logical RTT period using its latest submitted revision. |
| MHS105 Onward Referral | `fct_mhsds_onward_referral` | One dated onward-referral decision and receiving organisation. This is a recorded service-movement signal. |
| MHS106 Discharge Plan Agreement | `fct_mhsds_discharge_plan_agreement` | One dated discharge-plan agreement linked to a referral. |
| MHS202 Care Activity | `fct_mhsds_care_activity` | One care activity linked to its contact and referral. Its populated clinical components can produce separate clinical-record rows. |
| MHS203 Other in Attendance | `rel_mhsds_care_contact_attendee_type` | One contact and attendee-type code. It does not identify another person. |
| MHS204 Indirect Activity | `fct_mhsds_indirect_activity` | One dated activity undertaken for a patient while the patient was not present. It is not a care contact. |
| MHS205 Patient Self-Directed Digital Intervention | Data-quality review in #1032 | A future intervention-period fact if the new v6 records can be linked reliably. |
| MHS206 Staff Activity | `rel_mhsds_care_activity_staff` | One recorded care-activity-to-professional relationship. Missing MHS202 parents remain a visible source-quality state. |
| MHS609 Presenting Complaint | Data-quality review in #1033 | A future clinical-record contribution if its v6 patient and referral links are usable. |
| MHS901 Staff Details | Reporting-period professional context | Staff group, specialty, occupation and job role at the source-defined snapshot grain. |

MHS107 medication prescriptions has no rows in accepted submissions and will
not receive an empty reporting model. It can be added when source data begins
flowing.

Aggregate profiling in August 2026 found about 0.7 million additional referral
reasons, 0.8 million RTT rows, 0.1 million onward referrals, 0.2 million
contact-attendee rows and 1.2 million indirect activities. MHS205 and MHS609
are held behind data-quality reviews because their new v6 records do not
currently link reliably. About 419,000 MHS206 rows refer to an MHS202 care
activity absent from the received data.

These facts need the matching UKHFD code sets before publication. UKHFD holds
the referral reason, RTT status, onward-referral reason, care-contact attendee,
indirect-activity person, consultation medium, cancellation, advocacy and other
MHSDS dictionaries. dbt reference models should expose the current code set and
its history in `REFERENCE.DATA_DICTIONARY`.

## Other MHSDS domains

The following groups need their own model families rather than more columns on
the referral or contact facts:

- MHS002–MHS014: dated GP registration, accommodation, employment, patient
  indicators, care coordination, disability, care plans, social circumstances
  and fit notes. These are person context, not healthcare events.
- MHS401–MHS405: Mental Health Act and community treatment order periods.
- MHS501–MHS502 are the published spell and ward-stay facts. MHS503–MHS518
  contain care-professional assignments, discharge delays, leave, incidents
  and discharge readiness, which need their own event or relationship models.
- MHS601/MHS603–MHS607 now feed the clinical-record fact. MHS608 anonymous
  assessments remain outside the person-linked model; MHS609 awaits #1033.
- MHS701–MHS702: Care Programme Approach episodes and reviews.
- MHS901–MHS903: staff, service/team and ward reference entities.

MHS301 group sessions are provider-level activity with no patient-level link.
MHS302 drop-in contacts also sit outside the patient-linked data model in the
MHSDS specification. They can be published for service analysis, but they must
not receive `sk_patient_id` or enter a person event timeline unless a supported
linkage route is established.

## Model boundaries

- Keep source-recorded parent identifiers on child facts.
- Do not turn a one-to-many child table into columns or arrays on its parent.
- Do not treat time proximity as a recorded relationship.
- Keep patient contacts, indirect work and clinical items distinguishable.
- Use current UKHFD descriptions while retaining submitted codes and historical
  validity.
