# CSDS clinical records

`fct_csds_clinical_record` supplies clinical detail for #1018 and the later
cross-source work in #1006. One row is a retained coded immunisation, an accepted
assessment occurrence or a populated procedure, finding or observation from a
care activity. It is not an encounter or a whole-questionnaire table.

## Source meaning and selection

The implementation uses the
[CSDS ETOS v1.6.10 workbook](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_etos_v1.6.10_final.xlsx)
and [user guidance v1.9](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_v1.6_user_guidance_v1.9.pdf).
Accepted submissions follow `stg_csds_activesubmission`. Staging retains their
source occurrences, including repeated reporting periods.

CYP501 records a coded immunisation on an administration date. It includes
childhood records and adult records when submitted. ETOS C501D37 defines the
successor key using national person, provider, procedure and date. The clinical
model also keeps scheme boundaries, although profiling found no scheme changes
within these keys. It selects the latest accepted reporting evidence and records
the first and last reported periods and the number of represented source rows.
Missing people or incomplete clinical keys retain individual occurrences.

Provider-local identifiers do not define additional immunisations. The source
profile found 581,592 candidate national-person keys with different local
identifiers across reporting periods. The retained row preserves its own local
identifier without treating each changed identifier as a new immunisation.

CYP609 records an assessment completed outside a specific contact. Its submitted
completion date has date precision. Current ETOS C609D37 uses referral, concept
and score to derive source supersession. The historical CSDS_1.0 column in
Complex Derivations used completion date instead of score. Neither source rule
defines the required longitudinal clinical history: 41 current successor keys
contain different completion dates. The clinical model therefore preserves all
accepted assessment occurrences, including equal scores on different dates.

CYP612 links directly to a care activity, despite its "Contact" table name.
It has no submitted assessment date. Its `dmic_observation_date` is a warehouse
addition absent from ETOS. The model keeps that date separately and inherits
contact time through the same accepted activity only when populated person
identifiers agree across assessment, activity and contact. CYP202 components
likewise require a person-consistent contact before inheriting its time.
Missing or conflicting identity does not authorise time inheritance.

Complex Derivations lists a person, provider, concept and score key for CYP612,
but the group has no RecordStartDate or RecordEndDate fields. Profiling found
995 such keys across different activities. The clinical model retains accepted
occurrences rather than collapsing those contacts.

CYP202 clinical components reuse `fct_csds_care_activity` and its same-submission
contact. Observations with values but no code survive. Staff relationships do
not enter the clinical fact. Guidance assigns scored assessments to CYP609 or
CYP612; a recognised assessment code in a CYP202 component receives a flag and
keeps its submitted record type.

## Labels and assessment interpretation

Scheme numbers are field-specific. Procedure 06, finding 04 and observation 03
mean SNOMED CT. Procedure 04/05, finding 02/03 and observation 01/02 mean Read v2
and CTV3. Finding 01 means ICD-10. CSDS still accepts these legacy schemes; the
MHSDS fixed-SNOMED observation rule does not apply.

UKHFD supplies scheme labels. Exact SNOMED and normalised ICD-10 lookups reuse
the shared references. Dictionary.dbo.ReadCodes supplies Read v2 and CTV3 terms
through explicit membership flags. Case and significant dots remain unchanged.
Exact dictionary codes take precedence over source-provided alternative codes;
ambiguous alternatives remain unresolved. Source master and mapped fields were
empty across accepted CYP202 and CYP501 records, so these dictionary mappings
are labelled separately as reference enrichment.

The Read dictionary has 310,652 distinct exact-case codes, including 157,238
Read v2 memberships and 276,841 CTV3 memberships. There are no duplicate code
or alternative-code keys within either system. Of the recorded SNOMED mapping
values, 239,560 are non-positive placeholders. The shared reference exposes
these as absent mappings rather than publishing them as clinical concepts.
Unmatched clinical codes remain visible. Numeric appearance does not establish
SNOMED provenance.

Observation units use `clinical_unit_of_measurement`, with exact-case UCUM
codes, dictionary symbols and recognised aliases. The model does not convert
values or infer a unit from similar typography.

The reproducible CSDS assessment seed has 385 public response or range
definitions for 175 concepts. Only three concepts occur in the existing MHSDS
seed, so the CSDS reference has its own source and published domains. The
generator carries concept and precision through continuation rows. It preserves
the workbook row and collection start date.

ASQ-3 rows represent dimension totals, with a published range of 0 to 60 and one
decimal place. ASQ:SE publishes integer totals of 0 to 465. EQ-5D-5L response 9
means "Missing value" for its five dimensions, while Clinical Frailty Scale
response 9 means "Terminally ill". Non-score interpretation belongs to the
concept and response pair. No numeric range is inferred for the EQ-5D index
whose published value says "See comments".

The fact retains submitted text, a technical numeric parse and its parse state.
Score interpretation checks plain decimal syntax and significant fractional
digits before matching numeric responses. Values rounded at scale nine and
unverified numeric formats do not receive interpreted scores. It does not use
floating-point equality to decide whether a response is exactly an integer.
`assessment_score_numeric` includes only enumerated numeric responses or values
within the published range and precision, excluding explicit non-score
responses. An unmatched response remains available as source evidence. Reference
membership and interpreted scores are not retrospective submission validation.

Regenerate the public seed after downloading the named workbook:

```powershell
python scripts/reference/generate_csds_assessment_seed.py --spec logs/csds_etos_v1.6.10.xlsx --output seeds/csds_assessment_scale_definitions.csv
```

The seed contains public specification metadata.

## Identity and validation

The clinical item's `source_record_id` equals `clinical_record_id` and supports
a one-row cross-source drill-down join. `originating_source_record_id` preserves
the immunisation revision or accepted source occurrence. An activity occurrence
can have several clinical components. Assessment occurrence keys change when
a replacement submission replaces the source row; they do not claim a durable
match between indistinguishable assessments.

Initial shared-DEV validation built six models and one seed, with all 11 selected
tests passing. The immunisation model retains 2,460,465 unique clinical items
representing all 16,137,212 accepted source occurrences. All 14 occurrences
without national person identifiers remain separate. Submitted clinical dates
are preserved without importing a date cutoff from MHSDS. Duplicate old and
renamed procedure scheme fields had no differences, so staging exposes the
community-care field once.

Permanent tests cover grain, represented-source reconciliation, clinical
component membership, drill-down identity, date precision, safe parent-time
inheritance, response meanings and published ranges. Singular tests return the
failing model keys for warehouse diagnosis. Agent-visible validation uses only
non-identifying aggregates.
The checked-in `analyses/community/csds_clinical_record_profile.sql` supplies
aggregate coverage, parent, time, response and label checks.


## Initial shared-DEV evidence on 8 September 2026

The final model build passed six models and 16 selected tests. It rebuilt the
clinical fact in 21 seconds. A subsequent run passed all eight singular tests
after adding source-key membership and dictionary preservation checks. Five
synthetic numeric cases also passed, including a value just below nine with
18 fractional digits that must not become Frailty score nine. The reporting
fact has 37,606,281 rows and the same number of distinct clinical IDs.
`dbt ls -s fct_csds_clinical_record+` found no downstream business models.

| Clinical record type | Retained rows | Populated code without a label | Missing clinical time |
|---|---:|---:|---:|
| Immunisation | 2,460,465 | 1,790,007 | 0 |
| Referral assessment | 132,672 | 0 | 0 |
| Activity assessment | 807,956 | 0 | 10 |
| Procedure | 9,373,770 | 411,404 | 172 |
| Finding | 8,923,846 | 988,476 | 140 |
| Observation | 15,907,572 | 3,581,720 | 74 |

All 37,606,281 records have provider labels. Responsible-organisation labels
cover 2,454,889 of 2,457,238 populated responsible-organisation codes. The fact has
30,200,514 clinical descriptions. Dictionary alternatives resolve 3,169,266
Read-coded items while preserving their submitted codes. Separate positive
Read-to-SNOMED mappings cover 3,319,056 items, with 3,280,556 mapped descriptions.
Unmatched codes stay visible; this work does not infer expressions, repair Read
codes or assert that dictionary membership establishes clinical validity.

Every assessment activity parent linked in the same accepted submission. No
populated assessment/activity/contact person identifiers disagreed. The 396
missing inherited times arise from incomplete person identity. CYP612 warehouse
derived dates agreed with the safely inherited contact dates wherever both were
available. There are 10,258 clinical records without a linked patient key.

The assessment references recognise all 940,628 submitted assessment concepts.
They interpret 940,526 scores: 938,773 within published ranges and 1,753
published enumerated responses. Another 102 responses remain unmatched. No
explicit non-score response occurs in the current assessment population; public
reference tests protect the distinction between EQ-5D missing value 9 and
Frailty score 9. The 128,599 recognised assessment concepts submitted as CYP202
components remain procedure, finding or observation records.

There are 634,160 observations with a value but no code. Of 5,157,054 populated
observation units, 3,000,170 receive an exact UCUM, dictionary-symbol or recognised
alias label; 2,156,884 remain unresolved. Values remain in submitted units.
The numeric parser labels 16,196,924 values numeric, 21,408,891 absent and 466
non-numeric or outside the supported numeric representation.

These counts describe the accepted source population at validation time. They
are not fixed data-volume assertions. The legacy contact-activity extract joins
a separate coded-item table and other sources; its row count is not the grain
of either the care-activity fact or this clinical fact.


## Follow-up profiling on 9 September 2026

Profiling covered all 65 original reporting columns. Every column had populated
values and none contained blank strings. The historical date and time checks
found no dates before 1900, future dates, dates after the reporting period or
conflicts between stored time and declared precision. No new date cutoff was
introduced. Code lookups had no duplicate organisation, Read or SNOMED keys.

The analyst interface now uses `submission_id`, `referral_id`, `activity_id` and `contact_id`.
`clinical_code_system` gives a consistent SNOMED CT, Read v2, CTV3 or ICD-10 name.
The field-specific submitted scheme code and authoritative `coding_scheme_name`
remain separate. Clinical time uses the submitted contact relationship even
when a later contact revision changes the current parent.
`is_submitted_contact_person_consistent` names that historical comparison
explicitly. Activity components with a missing person ID return null for person
consistency with their originating activity, rather than asserting agreement.
This corrects 386 prior true flags: 172 procedures, 140 findings and 74 observations.

The derived-date comparison previously returned false when comparison was
unavailable for 36,798,335 records. It now returns null without both dates.
A false result means the two populated dates agree. Organisation names use the
shared latest UKHFD ODS definition, with Dictionary fallback for absent codes.

The largest code gap is historical Read-v2 immunisation delivery. Of 985,817
retained Read-v2 immunisations, 974,621 have unlabelled numeric codes. Those
unlabelled numeric values span 973,198 distinct tokens and occur only in retained
reporting years 2017 to 2020. The source values do not equal the recorded person,
local patient, source row or record-number identifiers. The raw interface selects
the correctly named immunisation-procedure field. This unusual cardinality needs
delivery-source investigation; a larger terminology dictionary alone does not
explain or repair it. Records and their unmatched status remain intact.

Full UKHFD SNOMED description history adds no concept-identifier matches for
the remaining records submitted as SNOMED CT. Read gaps have no case-only matches,
and source clinical codes contain no outer padding. Some unlabelled CTV3 tokens
occur in UKHFD migration maps, but a mapped target description is not the original
Read term. This change does not relabel a submitted scheme or treat a migration
map as an equivalent source description.

The 2,156,884 unresolved observation-unit records use 200 distinct tokens. Four
numeric tokens account for 1,267,074 records; none matches UKHFD UCUM ConceptID
or the SNOMED concept dictionary. Matching internal dictionary IDs does not
establish source meaning. Another 553,066 records would match after changing
case, but UCUM is case-sensitive. No unresolved token matches a historical UCUM
code or alias omitted by the latest reference. Units remain unchanged until an
authoritative equivalent is available.

The 102 unmatched assessment responses split into 85 outside the published
range, one further response outside the permitted precision, six non-numeric or
outside the numeric representation, and ten unmatched enumerations or values
without a published range. None receives an interpreted assessment score.


The follow-up build passed both changed clinical models and all nine selected
tests in 66 seconds; the clinical fact built in 22 seconds. A fresh profile
confirmed the same 37,606,281 unique records, 30,200,514 clinical descriptions,
940,526 interpreted scores and 3,000,170 unit labels. All 65 reporting columns
have populated values and no blank strings. All provider names now come from
UKHFD ODS API; responsible-organisation coverage remains 2,454,889 records.

There are 36,978,580 named clinical code systems. The remaining 627,701 records
are observations without a submitted scheme. No SNOMED, Read or ICD label is
assigned under a different established system. The date comparison has 807,946
agreements, no disagreements and 36,798,335 unavailable comparisons. No missing
person identifier receives a known activity-consistency result. The 396 missing
clinical times remain absent. Full-stack compilation also passed with 6,888
nodes, including hooks.

## Legacy immunisation investigation

The follow-up investigation on 9 September 2026 found no justified code
replacement for the 974,621 unlabelled numeric Read-v2 immunisations. All are
submitted under CSDS version 1.0, with retained reporting periods between
1 October 2017 and 30 June 2020. Their 973,198 distinct tokens remain unchanged.
The [retained Snowflake diagnostics](../scripts/snowflake/profile_csds_legacy_immunisation.sql)
return aggregate results only and require the existing DEV clinical fact and
legacy source views. They do not create or alter warehouse objects.

The [CSDS ETOS v1.6.10](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_etos_v1.6.10_final.xlsx)
defines C501020 as a clinical terminology code, with format checks specific to
the declared procedure scheme. The relevant rule was updated in v1.6.2, so it
does not establish the delivery rules applied to these v1.0 submissions.
C501D37 includes the clinical code alongside person, provider and administration
date in the successor definition. Matching only person, provider and date loses
that distinction and cannot establish which vaccination a replacement describes.

The active CYP501 view passes the shared source through without changing its
columns. Its declared column order agrees with the shared table metadata, and
the raw model selects the correctly named immunisation-procedure field. The
source code does not equal the row's recorded patient, local patient, source row,
submission or record-number identifiers. This does not establish whether the
unusual values originated in a provider submission or an upstream transformation.
Snowflake does not expose the producer's table DDL through this shared database.

The deprecated national CYP501 source contains 829,572 rows and 821,889 distinct
source identifiers. Its mapped and master SNOMED code and term fields are all
null. The deprecated BSP source is empty. Comparing the unresolved current
records with the national legacy source gave these results:

| Comparison | Result |
| --- | ---: |
| Same source identifier and provider | 0 records |
| Person present in the legacy source | 419,861 records |
| Same person, provider and administration date | 91,476 records |
| One legacy code at that broad person/provider/date combination | 89,037 records |
| Matching submission, local patient or record-number identifier among the broad matches | 0 rows for each check |

The 89,037 single-code candidates contain only 24 legacy tokens. None has a
scheme-qualified Read label or a complete Read-code/term-code migration match.
Neither the terminology nor the occurrence evidence supports copying these
codes into the current records. More than one vaccination can occur on a day.

`REPORTING.MAIN_DATA` contains CSDS referral, contact and contact-activity tables,
with no separate immunisation table. The supplied contact-activity SQL reads
`Simple.tblCare_Activity_Coding` and joins its code directly to a preferred
SNOMED description without checking the submitted coding scheme. That lookup
does not establish that a Read-coded immunisation is a SNOMED concept and should
not be copied into this fact.

A search of successful CYP501-related creation statements in the available
365-day query history found another author's `Simple.tblPatient` creation query
on 7 May 2026. Its immunisation section groups and counts the same submitted
code, scheme and administration date. It supplies no alternate code, mapping or
repair. The local deployment SQL also contains no CYP501-specific repair.

These checks found no usable replacement in the inspected legacy objects or
available creation SQL. The later full Read-term history check below identifies
18 exact seven-character term labels. Explaining the remaining numeric values
needs the upstream delivery owner to establish how the v1.0 procedure field was
populated. No clinical
meaning has been inferred from numeric shape, internal dictionary identifiers
or same-day records.

## Approved historical observation-unit fallback

The clinical fact reuses `csds_observation_unit_alias` for the approved historical
weight, height and BMI spellings. It applies only to CYP202 observations after
an exact shared unit-reference match fails. The observation must carry the named
SNOMED measurement concept, either directly under the SNOMED observation scheme
or through a positive, scheme-qualified Dictionary Read mapping. Immunisations,
assessments, procedures and findings cannot receive this fallback.

The resulting match type is `csds_etos_alias`, with definition source
`CSDS ETOS historical unit alias`. Submitted clinical codes, descriptions, units
and values remain unchanged. These labels describe historical spellings; they
do not assert current submission validity, derive measurements or convert values.
The shared reference retains the ETOS data item, change date and canonical unit
definition source. A clinical regression test checks context and exact-match
precedence, then reconciles observation units and values with the activity fact.

The approved fallback added 171,507 clinical unit labels: 83,494 weight and
88,013 BMI observations. This brings unit coverage to 3,171,677 of 5,157,054
populated units, leaving 1,985,377 unresolved. The first estimate of 161,383
missed 10,124 BMI units written with a superscript 2. The retained profile uses
`chr(178)` for that spelling to avoid command-line encoding differences. All
3,000,170 prior exact-reference labels and their provenance remain unchanged.

The final clinical build passed the fact and all nine selected tests in
59 seconds; the fact built in 40 seconds. The new test found no context,
precedence or activity-unit disagreement. There are still 37,606,281 unique
clinical records, 30,200,514 clinical descriptions, 940,526 interpreted assessment
scores and 396 unavailable clinical times. An aggregate fingerprint of record
identities, clinical codes and descriptions, submitted units and values,
clinical timestamps and precision, and assessment scores and statuses was
unchanged before and after the build.

The final full-stack compile passed with 6,904 nodes including hooks: 2,070
models, 4,713 tests, 28 snapshots, 40 seeds and 48 analyses. Existing optional
credential and unused-configuration warnings remain unrelated to this change.


## Historical clinical labels

Retirement does not remove a code's historical meaning. Shared references must
retain historical codes with their latest available definition. SNOMED's
[concept permanence rule](https://docs.snomed.org/snomed-ct-specifications/snomed-ct-release-file-specification/release-types-packages-and-files/3.1-common-features-of-all-release-files/3.1.4-meaning-of-the-active-field)
explicitly preserves inactive concepts and their readable descriptions. The
current staging model retains 312,564 inactive SNOMED concepts; it has no
active-only filter. The Read reference likewise has no retirement filter.

The [historical-label diagnostics](../scripts/snowflake/profile_csds_historical_labels.sql)
compare unresolved submitted codes with every available UKHFD revision, without
filtering status, `Is_Latest` or `In_Source_Data`. UKHFD contains 157,236 distinct
Read v2 concept codes, 332,112 CTV3 concept codes and their term histories. No
additional unresolved record matches a concept label within its declared Read
system. This finding concerns these unmatched values, not the validity of Read
coding in historical records.

The full Read v2 term archive does identify 18 exact seven-character labels in
the numeric immunisation population. [NHS Digital's terminology guidance,
page 11](https://github.com/nhsconnect/gpconnect/blob/3027e354fc146623e7bd561700ebc750bb839189/pages/accessrecord_structured/GuidanceOnCodeableConcept.pdf)
defines this representation as the five-character Read code followed by its
two-character term code. The CSDS ETOS Clinical Terminology Validations sheet
accepts five- or seven-character Read values. The shared reference now uses the
latest available UKHFD term for each full code, including historical codes,
after existing Dictionary matches. It exposes `UKHFD Read v2 term history`
provenance. These terms do not inherit a five-character code's SNOMED mapping:
the NHS guidance warns that alternative Read terms are not always synonyms.
A literal terminology label does not establish that an immunisation was coded
appropriately.

Two larger apparent sources of labels do not justify changing the output:

- Dictionary entries without Read system membership cover 5,106,887 unlabelled
  CTV3 records and 912 Read v2 records. Every matching term is a `Read Code:`
  prefix followed by the submitted code. These are placeholders, not labels.
- Separate UKHFD CTV3 term identifiers match 1,176,619 records. The NHS guidance
  states that a CTV3 term identifier identifies text and needs its concept code
  to establish clinical meaning. Some have multiple historical concept links;
  others have none. They remain investigation candidates, not clinical labels.

The [NHS England Terminology Server](https://digital.nhs.uk/services/terminology-server)
also holds Read 4-byte, Read v2 and CTV3 releases from 2009 to 2018. Its historical
coverage is a further source for reconciliation; no unresolved source tokens
were sent to that external service during this investigation.


The historical-term build passed the clinical fact and all nine selected tests
in 51 seconds. It added 18 labels, bringing clinical description coverage to
30,200,532 records and leaving 974,603 numeric Read v2 immunisations unlabelled.
All 37,606,281 record keys remain unique. Aggregate fingerprints confirm that
existing labels and mappings, submitted codes and values, units, clinical times
and assessment results are unchanged. None of the new term labels receives a
SNOMED mapping. Full-stack compilation passed all 6,912 nodes.
