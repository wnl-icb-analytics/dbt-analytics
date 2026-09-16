# ECDS observations and scored assessments

ECDS measurements and scores have their own record grains. A respiratory rate
measurement carries a value and unit; its NEWS2 component score carries points.
Neither belongs in the procedure model. These facts provide the missing domain
models consumed by the longitudinal clinical record.

| Model | One row represents | Clinical timestamp |
|---|---|---|
| `fct_sus_uec_observation` | Attendance plus source observation sequence | When made or verified |
| `fct_sus_uec_scored_assessment` | Attendance plus source assessment sequence | When the score was validated |

Both facts retain all delivered records. They preserve reported codes, text
values, source approval flags and import identifiers. Numeric values are parsed
separately as `NUMBER(38,9)`; parsing does not establish clinical validity. Raw
text remains available when parsing fails or rounds a value. The source timestamps
are already `TIMESTAMP_NTZ`, so the original timezone offset cannot be recovered.

Attendance context comes from `obt_encounter_uec`. A missing attendance leaves
its context null and sets `is_attendance_linked` to false. Distinct source
sequences remain separate even when code, value, unit and timestamp match.

## National definitions and reference data

The [ECDS user guide, version 4.7, sections 5.12 and 5.13](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/ecds/ecds-user-guidance-v4.7---approved.pdf)
separates scored assessments from observations. NEWS2 component scores and total
scores are separate submissions. Only the first NEWS2, pain, 4AT and Clinical
Frailty Scale assessment needs submission, so these feeds do not establish a
complete sequence of observations throughout an attendance.

`ecds_measurement_code` uses the existing UKHFD `ECDS_TOS.dim_Code_Sets`
table. No new warehouse ingestion is required. It selects the latest available
definition for each code and record type, with the source filename retained.
File timestamps determine release order because UKHFD can import several releases
in one batch. Sheet names are matched without section numbers, which change
between releases. Historical codes remain available for labelling; reference
presence is not evidence of validity on the event date.

The [national ECDS guidance page](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/emergency-care-data-set-ecds/ecds-guidance)
provides the ETOS releases. UKHFD currently contains version 4.0.8. Assessment
categories distinguish NEWS2, pain, delirium and frailty. The original ECDS
group code remains available, including `FUNC` for the 4AT and Rockwood tools.
Observation categories use their published observation terms.

Facts use ETOS preferred terms first, then the existing `snomed_concept`
reference. They expose the label source and retain unmatched codes. The shared
`clinical_unit_of_measurement` lookup resolves exact-case units
and established aliases after trimming outer whitespace. Its match type and
reference source distinguish UCUM codes from dictionary labels. Missing units
are not inferred and
values are not converted. An unmatched unit is not necessarily invalid because
the finite reference does not cover every legal UCUM expression. Unrecognised
local spellings require a source-backed alias definition, not case folding.

ACVPU response labels follow the ETOS notes: A is Alert, C is Confused, V is
response to voice, P is response to painful stimulus and U is Unresponsive.
Unrecognised responses retain their raw value without an inferred category.
No score severity thresholds or QuickReport measures are derived.

## Longitudinal integration

`int_ecds_person_clinical_record` consumes both facts and publishes them through
`fct_person_clinical_record` as `observation` and `scored_assessment`. Each source
sequence remains a separate row, including records without a patient key.
`source_model_name` and `source_record_id` link back to the fact and its detailed
category, code-label provenance and unit-match status. Attendance remains parent
context. These records do not enter the procedure model or encounter code arrays.

Observation values, categorical labels and reported units populate the shared
result fields. Resolved unit labels do not establish compatibility with the
observation. Scores populate the raw and parsed result fields; the tool code is
`source_code`, with its label in `assessment_tool_name`. A supplied score has
`assessment_response_status = 'not_validated'`; an absent value is `value_missing`.
`assessment_score_numeric` stays null because numeric parsing alone does not
establish a usable tool-specific score.

Clinical time is the observation's `observed_at` or the assessment's
`validated_at`, with that basis recorded explicitly. Missing clinical times stay
unknown. Attendance dates and delivery timestamps do not replace them.

The adapter uses the existing parent-delivery watermark and withdrawal handling.
Build the child facts before the adapter. A full refresh of the ECDS adapter is
required on deployment because its stored schema gains result fields. Subsequent
loads replay the boundary delivery; the existing monthly full refresh reconciles
older corrections and reference changes. Coordinate source and attendance refreshes
before building the facts, since a refresh can replace attendance identifiers.
Do not deduplicate a measurement against a score or related finding merely because
they share an attendance.

## Validation

The subsequent [coverage and correctness review](ecds-measurement-review.md)
separates source fidelity from clinical validity and records the refresh, unit
and patient-linkage limitations found by profiling.

The DEV build on 16 September 2026 preserved 62,256,079 observations and 42,857,394 scored
assessments, matching their staging row counts. All records linked to an
attendance. Grain tests passed at staging, reference and fact level.

Two unmatched assessment codes account for 192,244 records: `1104051000000100`
and `1104331000000100`. Their similarity to NEWS2 codes does not authorise a
correction. Confirm their origin and intended meaning with the source supplier
before adding a mapping. Preserve both the reported code and any authorised
correction if this is resolved upstream.

The final facts resolve labels for 62,254,213 observations and 42,665,150
assessments. The shared unit reference labels 15,085,697 observation records;
26,875,912 have an unmatched reported unit and 20,294,470 have no reported unit.
Missing units are expected for categorical ACVPU responses. Common unresolved
spellings include `DEGC`, `BRMIN`, `MMHG` and `BPM`; none is silently converted
into a case-sensitive UCUM code.

The selected nine-model DEV build passed its tests. The two facts rebuilt in
about 48 seconds after the unit lookup change. Static reference-boundary,
hardcoded-reference, description and test-coverage checks passed. Ownership
checks found no missing owners. The local verification helper's version check
does not recognise the installed dbt 2.0.1 banner, so warehouse verification
used the documented direct CLI commands with the tracked `dev` target.

## Clinical-record integration validation

On 16 September 2026, the refreshed facts supplied 62,278,119 observations and
42,874,509 scored assessments. The
[aggregate reconciliation](../analyses/acute/ecds_clinical_record_integration.sql)
compared every source fact row with `fct_person_clinical_record`. Both record
types had zero missing rows, unexpected rows or changed payloads. The comparison
covers source identity, person and provider linkage, parent identity, codes,
values, unit labels, tool labels, interpretation status and clinical timestamps.
It also checks that unvalidated ECDS scores do not populate the usable-score field.
A separate comparison with current staging also found zero missing, extra or
changed source records across both refreshed facts.

All 2,598,813 observations and 1,325,165 assessments without a patient key remain
in the clinical record. These rows are discoverable by source and attendance;
they cannot support person-linked analysis until the source supplies linkage.
The seven-type ECDS adapter contains 201,391,188 rows. Its full refresh took
63 seconds using the existing navigation warehouse hook. Adapter grain and
SNOMED mapping checks passed, as did shared clinical-record grain and timestamp
checks and the downstream coverage model's grain check.

The prerequisite build initially found stale shared DEV staging definitions and
older diagnosis, procedure and encounter-array snapshots. Rebuilding those
existing models resolved both source-count reconciliation failures. Their SQL
was not changed by this PR.

The subsequent incremental replay took 127 seconds on the configured medium
warehouse. Counts and whole-row fingerprints were unchanged across all seven
record types and 201,391,188 rows, including both new types after the withdrawal
hook ran. The replay build and its selected tests passed.
