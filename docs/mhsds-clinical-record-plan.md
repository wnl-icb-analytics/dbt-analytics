# Clinical records (#1017)

The [5 September method review](mhsds-clinical-method-review.md) records the
latest fixes and aggregate results. Earlier counts below describe the successive
review snapshots.

## Intent

Publish one analyst-facing clinical-item fact for recorded diagnoses, care-activity
components and scored assessment questions or dimensions. Keep source meaning,
direct relationships, codes, labels, values and time provenance visible. Do not
present clinical items as encounters, disease onset or whole questionnaires.

## Proposed model

### Role in the cross-source plan

This is the MHSDS clinical-detail input to [#1006](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1006)
and the later cross-source clinical-record model in #1019. It is not an event
timeline. Use `sk_patient_id` for cross-source person joins and keep clinical
codes, descriptions, values, units and time precision together. Recorded referral
and activity links remain separate from person-and-time association. Demographic
attributes belong in the established person models, not repeated here.

The column order puts the warehouse patient key and clinical concepts first,
then clinical time, values, units, organisations and recorded relationships.
Source quality and submission audit fields follow.

For #1006 and #1019, `source_record_id` now identifies one clinical item and is
equal to the unchanged `clinical_record_id`. Cross-source adapters can use it
for a one-row drill-down join to this fact. The previous `source_record_id` is
preserved as `originating_source_record_id`; it may repeat across components of
one care activity. Existing referral and care-activity parent keys are unchanged.
Consumers of the former source-occurrence key must use the new column name.

The DEV fact retains 26,278,485 unique clinical items and standard drill-down
keys across 24,715,018 originating source keys. It retains 146,136 items without
a warehouse patient key and 11,148 without clinical time. The cross-source model
must report those coverage gaps rather than infer patient identity or time.
All nine selected tests passed. The pre-existing content checksum is unchanged
when the renamed originating key is compared in place of the former source key.
No activity-parent keys changed.

### Grain and population

`fct_mhsds_clinical_record` has one row per retained source clinical item and
component type. It includes accepted MHS601, MHS603, MHS604, MHS605, MHS606 and
MHS607 records, plus populated procedure, finding and observation components
expanded from `fct_mhsds_care_activity`. Observation values without codes survive.
No staff relationship join enters this fact.

MHS204 awaits #1030. MHS609 awaits #1033. MHS608 is anonymous and excluded;
MHS107 has no accepted data. Do not create globally empty placeholder columns.

| Subject | Proposed selection | Time meaning |
|---|---|---|
| Previous diagnosis, MHS601 | Latest accepted row per provider-local patient, national person ID, scheme, code and clinical time/precision | When the previous diagnosis was discussed, not its onset |
| Provisional, primary and secondary diagnoses | Latest accepted row per referral, national person ID, scheme, code and clinical time/precision | Recorded diagnosis timestamp |
| Referral assessment, MHS606 | Each accepted assessment question/dimension record; no unsupported collapse of distinct scores | Submitted completion timestamp |
| Activity assessment, MHS607 | Each accepted assessment question/dimension record; retain different scores | Same-submission activity/contact time, explicitly inherited |
| MHS202 components | Expand the established fact without reselection | Reuse its time and precision |

Use the stored timestamp first, then the separate source date with date precision.
Keep both fields and flag disagreements. Do not infer original precision from
midnight. When both are absent, retain the source row without cross-period
collapse. Different diagnosis codes at the same referral and timestamp survive.
Dates before 1901 follow the project's existing source-epoch rule: preserve the
source values and flag them, but do not use them as clinical time or revision keys.

Include the source person identifier alongside the specification's diagnosis
key. The profile found 206 primary-diagnosis keys spanning different warehouse
patient keys. Preserve these identity boundaries and flag changing person IDs.
This deliberately retains possible duplicates after national identifier changes;
it does not combine potentially different people's clinical histories.
Missing national person IDs form their own group within a complete provider-local
key. They do not force one diagnosis per reporting period. Missing parent, code,
scheme or usable time still retains individual source rows.

Diagnosis IDs hash the guarded logical key. MHS202 component IDs hash the
established activity key and component type. Assessment IDs hash the accepted
source table, submission and row ID. Assessment occurrence IDs are stable for
the retained source record, not across replacement submissions. An invented
ordinal would not establish an enduring identity for indistinguishable records.
Retain first/last reported periods and source-row counts for diagnosis evidence.
These are recorded diagnoses, not a determination of which diagnosis is correct
or clinically current. Currency supersession remains a separate rule.

Keep accepted source rows in staging. Put any cross-period clinical revision
selection in modelling. Change `stg_mhsds_primdiag` to the complete accepted
source interface and move its existing currency selection and mapping unchanged
to a modelling model. All existing consumers must retain their current results.
Do not introduce a second staging route to MHS604 or reference raw data outside
staging models. Aggregate QA analyses may inspect source evidence.

## Analyst fields

Include clinical-record ID, retained source-record ID and table, clinical item
type, direct referral/contact/activity identifiers, source person and patient
key, clinical date/time and precision/basis, submitted coding scheme and code,
authoritative description, source-supplied SNOMED mapping and description,
value and unit, provider code/name, accepted period and submission provenance.
Retain MHS601's provider-local patient identifier as its direct patient link.
Use a label-status field to distinguish missing, unmatched, mapped and
unavailable-reference cases. Keep the submitted assessment score as text and
offer a NUMBER(38,9) technical parse with parse status. Separate specification
response labels, tool grouping and interpreted scores from that technical parse.
`assessment_score_numeric` excludes known non-score responses and requires an
enumerated numeric response or the published range and precision. It does not
derive a whole-questionnaire score. Retain the directly supplied MHS606 local and derived assessor
identifiers. Do not join the one-to-many activity staff relation to clinical items.

Use the existing ICD-10 and SNOMED references. Add the UKHFD diagnosis-scheme
reference through `scripts/sources`, retaining every historical code with its
latest definition, not only currently valid codes. Check its values against
the MHSDS scheme because similarly named coding schemes have different numbers.
Reuse care-activity labels and label states. Do not substitute a source-pipeline
SNOMED mapping for the submitted code without showing its provenance.

Warehouse diagnosis and referral-assessment timestamps are TIMESTAMP_NTZ.
Their original offset and submitted precision are unavailable. Do not infer
date-only precision merely because the stored time is midnight.
All 2,540 retained timestamp/date disagreements occur in the 23:00 hour of
the preceding calendar day. That is consistent with an upstream time-zone
conversion, but does not prove which field preserves the intended time.
Keep both fields; do not silently shift timestamps. Period comparison flags
describe stored calendar dates, not formal offset-aware submission validation.

## Evidence checked

- ETOS v5 and v6 MHS604/MHS605 RecordEndDate derivations both include referral,
  coding scheme, diagnosis timestamp and diagnosis code in the merge key.
  The narrower currency key is not the full clinical-record grain.
- ETOS v5 MHS603 has the equivalent provisional-diagnosis key. MHS603 is absent
  from the v6 workbook; retain received historical records.
- V6 MHS607 rejects duplicate activity, assessment-code and score combinations.
  Different scores are not automatically revisions.
- V6 MHS606 completion falls within its submitted reporting period. Its group
  has no equivalent duplicate-rejection rule in the inspected specification.
- Initial aggregate profile found unique source IDs within each version and no
  missing clinical codes or parent identifiers across the six accepted sources.
  Missing national person identifiers and diagnosis timestamps do occur.
- The latest guidance includes an April 2026 assessment/ICD-11 uplift. Historical
  concept labels must not depend on membership of today's submission code list.

Sources: [current guidance and ETOS](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance),
[archived specifications](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance/mental-health-services-data-set-archived-specification).
The March 2026 user guidance sections 4.3 and MHS601/MHS606/MHS607 clarify time
and assessment meaning. Workbook field derivations, rather than group prose
alone, define revision keys.

## Validation and review

1. Profile candidate revision keys, exact duplicates, conflicting person/code/
   score values, null times, timestamp/date disagreement and repeated periods.
2. Challenge this design with the interrogate panel before implementation.
3. Build with the tracked `dev` target in the existing `DEV__` layers. Follow
   the established dependency and deferral workflow; DEV is intentionally shared.
4. Test each model's grain and documented component rules. Reconcile source
   survival and output counts by type. Profile labels, numeric parsing, direct
   parent links, patient mapping, date quality and join multiplication.
5. Compare preserved currency selection with the baseline and build affected
   downstream models in DEV. Inspect lineage with `dbt ls`.
6. Open a draft PR, check its rendered description, run implementation
   interrogate and verify CodeRabbit findings against the specifications and
   safe aggregates. Do not merge as part of this work.

All query outputs and committed evidence must be high-level, non-identifying
aggregates. Never preview source rows or disclose patient identifiers.

## Architecture interrogate verdict

### Intent

Challenge the clinical-item grain, time interpretation, record identity, labels
and currency compatibility before completing the implementation.

### Reviewers

- Claude Fable, high: six findings.
- GPT-5.6 Sol, xhigh: five findings.

### Act on

- Both reviewers: define timestamp/date precedence and preserve records without
  either. The profile found no date-only source rows today, but incomplete keys
  must not become a shared null-valued diagnosis key.
- Both reviewers: define occurrence IDs separately for diagnoses, activities and
  assessments. Accepted assessment-row identity cannot promise stability across
  replacement submissions.
- Both reviewers: expand the committed aggregate profile to test actual key
  collisions, repeated periods, score conflicts, date differences and parent
  links. Null counts alone do not justify selection rules.
- Claude: preserve first/last reporting evidence and the currency model's full
  ordering and output columns. The existing currency checksum must match.
- GPT: specify decimal precision and retain assessor identifiers. The profile
  found fractional scores and different assessors on apparently repeated rows.

### Consider

None remain from the architecture review.

### Noted

- Claude: distinct corrected codes can coexist in the clinical fact. That is
  intentional; the fact does not decide which diagnosis is clinically current.
  Currency supersession is a different analytical rule.
- Claude: the existing currency ICD-10 normalisation merits separate review.
  This PR preserves it, rather than changing currency results incidentally.

### Dismissed

- Claude's proposed general terminology resolver would duplicate or replace
  established activity labels. The clinical fact reuses those labelled outputs;
  diagnosis scheme numbers are handled separately.
- Claude's proposed assessment ordinal does not create a durable clinical
  identity for indistinguishable submitted occurrences. Retain source-row
  identity and state its resubmission limitation, as GPT recommended.

### Agreement map

Both reviewers identified time, identity and profiling gaps. Their main
disagreement concerned assessment identity. Source occurrence IDs preserve the
received evidence without claiming an unsupported longitudinal match.

## Aggregate validation, 4 September 2026

Accepted source periods cover January 2016 to July 2026. The six diagnosis and
assessment sources have globally unique source-row IDs. No accepted source
clinical codes or parent IDs are null. Missing person IDs and diagnosis times
remain present. The three checked-in clinical profile analyses reproduce the
source, candidate-key and final-fact checks without returning identifiers.

| Clinical item | Final rows | Without a description |
|---|---:|---:|
| Previous diagnosis | 776,297 | 23,680 |
| Provisional diagnosis | 211,142 | 939 |
| Primary diagnosis | 640,302 | 1,449 |
| Secondary diagnosis | 546,682 | 585 |
| Referral assessment | 6,617,619 | 0 |
| Activity assessment | 7,995,810 | 0 |
| Procedure | 7,056,111 | 99,188 |
| Finding | 1,777,585 | 24,550 |
| Observation | 656,965 | 20,038 |

All clinical-record IDs are unique. Component counts reconcile with the source
facts; 1,533 observations with values but no code survive. Every provider has a
label. Unmatched codes and expressions remain visible with label status; no
clinical meaning is inferred from an invalid scheme or a code's appearance.

The profile caught asymmetric ICD-10 normalisation: submitted codes had their
dots removed but the dictionary retained them. Both the new diagnosis lookup
and the existing activity finding lookup now use `clean_icd10_code`. The fix
adds 45,533 finding labels without changing activity grain or source fields.
A regression test checks descriptions against the normalised dictionary.

MHS606 has 111,754 non-numeric or out-of-range values and 6,202 fractional
scores; MHS607 has 28,125 and 3,949 respectively. No parsed values are rounded
at scale 9. Most unparsed assessment values are alphabetic responses. They are
retained as text, not classified automatically as invalid scores.

All 7,995,810 MHS607 rows link to their same-submission activity, with no person
or referral differences. There are 79 conflicting recorded contact IDs and
13 rows without inherited clinical time. Source links and quality flags stay
visible. MHS606 has 200 stored completion dates before the reporting period
and two after it; these records remain in the fact.

The final fact has 26,278,513 records. Post-review profiling found 122 MHS601
source rows with known Excel-epoch dates. Treating those dates as unknown keeps
all 122 source occurrences instead of collapsing them into four dated diagnoses.
Their original dates and a sentinel flag remain available. No published clinical
date precedes 1901. An aggregate reconciliation test proves that every accepted
diagnosis source row is represented in the retained rows' source counts.

The activity-time review found 98 MHS607 records with missing national person
identifiers on both the assessment and its directly linked activity. Their
provider, submission, activity and referral links agree. Null-safe consistency
now permits that recorded activity time to be inherited. It does not infer a
new person relationship. A regression test checks this time inheritance.

No clinical codes or scheme codes are blank strings. Observation units are
recorded on 559,346 items; 118,800 have no label in the existing unit dictionary.
Of those, 74,894 use BMI-style unit notation. Both v5 and v6 specify UCUM, so
this is not a new v6 rule. The September method review below supersedes this
initial profile with a case-sensitive UKHFD UCUM lookup. Authoritative UCUM
examples include [BMI units](https://hl7.org/fhir/valueset-ucum-vitals-common.html)
and [compound laboratory units](https://ucum.org/ucum).

The currency diagnosis move has the same 589,243 rows and full-row aggregate
checksum as the original staging output. Its 12 downstream models built in
isolation, including the cost-index models and semantic view. Of 66 downstream
tests, 65 passed and the existing `epd_covers_slam_cost_window` test warned about
missing prescribing coverage. No currency logic or prescribing inputs changed.

The initial core build passed 24 tests across 11 models. The corrected activity
and clinical facts passed all 16 selected grain, reconciliation and label tests.
The diagnosis-scheme source/reference build passed 13 tests. Initial validation
used task-specific databases, contrary to the established shared-DEV workflow.
Those four databases have been dropped and the custom profile removed. The
models now use the tracked `dev` target in the existing `DEV__` layers.
The hardening build then passed 18 tests across six models, including source
reconciliation, inherited times and source-epoch exclusion.
The final link/provenance build passed all 40 tests across six models, including
the affected activity-staff relationship model. All 18,043,704 existing activity
staging rows and their full-row checksum match the baseline when the new
provenance flag is excluded. The clinical fact retains every MHS601 local patient
link, including 10,861 rows without a warehouse patient key. It identifies
636,451 observation schemes as specification-fixed rather than submitted.
The final clinical-record count and unique-key count both remain 26,278,513.

### Shared DEV rebuild

The 5 September rebuild used the tracked profile and `dev` target. All 37 models
and two seeds succeeded. Of 148 tests, 147 passed and the existing
`epd_covers_slam_cost_window` test warned. Models use the existing DEV staging,
reference, modelling and reporting layers; seeds retain their configured shared
location. The final hardened fact has 26,278,485 unique clinical items, matching
the pre-cleanup aggregate. The currency selection retains 589,243 rows and its
full-row checksum. All 251,118 explicit non-score responses remain excluded from
the interpreted score field.

## Implementation interrogate verdict

### Intent

Check the implemented clinical-item fact against the MHSDS specifications,
accepted-source profiles, direct relationships and unchanged currency outputs.

### Reviewers

- Claude Fable, high: seven findings.
- GPT-5.6 Sol, xhigh: two findings.

### Act on

- Claude: make activity-time inheritance and consistency flags agree on missing
  person identifiers. Aggregate verification found 98 affected assessments;
  their direct activity and referral links agree. Fixed and regression-tested.
- Claude: distinguish unrecognised diagnosis schemes and unavailable references
  from an unmatched clinical code. Fixed without guessing a coding system.
- GPT: retain MHS601's provider-local patient link after diagnosis selection.
  `local_patient_id` now reaches the fact alongside the provider code. It is not
  replaced by a time-based or inferred person relationship.
- GPT: distinguish the observation scheme fixed by the specification from a
  submitted scheme. The activity stage and fact now expose its origin; the
  clinical fact uses `fixed_snomed` and a null submitted scheme for those rows.
  V4.1, v5 and v6 MHS202 field definitions confirm the rule. Existing activity
  scheme values remain unchanged.
- Lead profile: preserve source epochs without using them as diagnosis times
  or revision keys. Retaining individual incomplete-key rows corrects the
  unsupported collapse described above.

### Consider

- Lead profile: extend the shared observation-unit reference, or handle that
  as a follow-up. The current unmatched-unit counts and statuses are explicit;
  no unit meaning or conversion is guessed.

### Noted

- Claude: align diagnosis-scheme references with their family. Applied the
  existing history/latest-definition macros and column documentation. All six
  UKHFD codes, including two retired codes, remain available with unique keys.
- Claude: the assembly table duplicates storage before the reporting fact.
  Keep the project materialisation default for now. The measured hardening
  build took 56 seconds across six models; no performance problem was found.
- Claude: rename the legacy `stg_mhsds_primdiag` to match the newer names.
  The responsibility change is documented; another rename is not needed here.

### Dismissed

- Claude's proposal to remove legacy Read handling lacks historical-data
  support. The profile contains a legacy scheme, and a labelled code is not a
  claim that the scheme was valid for every MHSDS version. Keep source mappings
  explicit and preserve unmatched values.
- Claude's objection that the ICD-10 label test repeats a lookup does not make
  it redundant. It protects against the demonstrated asymmetric-normalisation
  regression and passed in about a second in the hardening build.
- Claude's proposed rounding of every small count is not a project rule.
  These are non-identifying, whole-dataset quality aggregates, without patient,
  provider, demographic or narrow-period breakdowns.

### Agreement map

The implementation findings were complementary rather than independently
repeated. Claude found null and label-status semantics; GPT found lost link
and scheme provenance. Both inspected the specification and surrounding
models. Each accepted finding was checked against source logic and safe
aggregate evidence; reviewer agreement alone was not treated as proof.
