# Clinical record method review, 5 September 2026

## Intent

Publish useful clinical items for analysts without confusing source rows,
diagnosis revisions, questionnaire responses and encounters. Preserve source
evidence, label codes and response categories, and separate numeric parsing
from clinical interpretation. Check the method against MHSDS specifications
and high-level data profiles before merging #1087.

## Reviewers

- Claude Fable, high: seven findings against `ca967e20`.
- GPT-6 Astra, high: three findings and one contact-timing check against the
  same snapshot. A focused check of the fixes caught a multi-letter response
  omission, now corrected and tested.

Both reviewers received the same intent, specification locations, rubric and
code-quality guidance. The old local Codex CLI could not run Astra, so its
review used the app's native agent with the requested model and effort.

## Act on

- **Assessment interpretation, both reviewers.** Concept-label coverage was
  complete but response meanings were absent. ETOS v6 "MH Assessment Scales"
  rows 73-74 define 888 and 999 for one Current View observable as unknown and
  missing. V5 rows 178-179 contain the same meanings. Added tool grouping,
  response labels, known non-score states and `assessment_score_numeric`.
  The technical parse remains visible and explicitly includes numeric sentinel
  codes. Exclusions apply to the concept/response pair, never a number alone.
- **Unit reference and case, both reviewers.** MHS202 M202100 requires UCUM
  in v4.1, v5 and v6. Uppercasing a case-sensitive symbol does not establish
  the corresponding case-insensitive UCUM symbol. Added UKHFD UCUM through
  `scripts/sources`, then exact-code and recognised-alias lookups with provenance.
  All 321 historical codes survive with their latest definition. The 293 absent
  from the latest source extract are not assumed clinically invalid.
- **Missing person identifiers, Fable.** A national person identifier is not
  part of the specification's diagnosis merge key. Keeping every untraced
  occurrence overstated diagnoses despite a complete provider-local key.
  Missing IDs now form a separate group, while different populated IDs still
  remain separate. This removes 28 repeated occurrences and preserves every
  accepted source row through the represented-source count.
- **Clinical label status vocabulary, Fable.** Normalised the long fact's
  `code_unmatched` and `code_or_expression_unmatched` states. A permanent test
  checks its documented vocabulary. Activity-specific statuses remain unchanged.
- **Master SNOMED provenance, Astra.** Corrected descriptions to say submitted
  or mapped. V4.1 M202D14 copies submitted SNOMED for scheme 04; a populated
  master field does not always prove a mapping.
- **Contact timing provenance, Astra's check.** Current data has no conflicting
  contact person IDs. The long fact now retains the upstream contact-person
  consistency flag so future conflicts are visible alongside inherited time.
- **Profile and person-profile descriptions, CodeRabbit.** Distinguished null
  source-version groups from grand totals. Clarified that the established
  person profile uses dated primary diagnoses. Its selection is unchanged.

## Consider

- **Unit case variants, both reviewers.** Exact matching adds 8,474 labels but
  removes 84,454 labels supplied only by case-insensitive dictionary matching.
  Most removed matches pointed to kilogramme or per-day definitions. This does
  not prove the source unit is invalid. It means that the available exact-code
  reference does not establish its meaning. Further aliases need an authoritative
  definition or an agreed source correction, not automatic uppercasing.
- **Remaining assessment responses, lead profile.** About 1.09 million responses
  do not match the latest published domain. DIALOG accounts for 1,057,970, of
  which 1,057,322 are zero. Both supplied v5 and v6 sheets publish 1-8 for these
  observables. Retain the values without reinterpreting zero as missing or a
  valid score. This is not a claim about historical submission acceptance.

## Noted

- **Repeated scans and inheritance predicates, Fable.** The affected five-model
  build completed in 75 seconds. No demonstrated performance issue justifies a
  broader materialisation or SQL redesign in this PR.
- **Source-epoch helper, CodeRabbit.** The existing explicit 1901 rule remains
  unchanged. A helper would reduce repetition but would not correct a result.
- **Small aggregate counts, Fable.** These are whole-dataset, non-identifying
  totals without person-level dimensions. They meet the project's safety rule.

## Dismissed

- **Prefer the separate source date on discrepancies, Fable.** The 23:00 pattern
  is consistent with a time-zone conversion, but it does not prove which field
  preserves the intended date. Both fields and the discrepancy flag remain.
- **Treat all BMI-style unmatched units as valid `kg/m2`, Fable.** The profile
  verified only 90 exact matches to that UCUM expression in the initial gap.
  Similar typography or case is insufficient evidence for a replacement label.
- **Publish failing patient IDs or raw clinical-code strings, CodeRabbit.** Tests
  deliberately return aggregate diagnostics under the public-data safety rule.
  Patient-level investigation belongs in a human-controlled Snowflake session.
- **Remove the ICD label regression check, CodeRabbit.** It protects the observed
  dotted-code lookup defect. A grain test alone cannot detect missing labels.

## Agreement map

Both reviewers found the response and unit gaps independently and supported the
diagnosis revision structure and unchanged currency method. Astra found no
demonstrated defect in the conservative missing-person fallback; Fable challenged
it. The aggregate profile resolved that difference in favour of deduplicating
complete provider-local keys while preserving person boundaries.

No additional exact matches came from the existing TRUD or UKHFD SNOMED/ICD-10
feeds. The PR does not add duplicate terminology pipelines or label a whole
post-coordinated expression from its first concept.

## Aggregate evidence

The final fact has 26,278,485 unique clinical items. Diagnosis represented-source
counts still reconcile to all 17,621,504 accepted diagnosis rows. Assessment and
activity component counts are unchanged.

| Assessment interpretation | Referral | Activity |
|---|---:|---:|
| Explicit non-score response | 190,073 | 61,045 |
| Enumerated response | 4,380,625 | 4,296,125 |
| Within published range and precision | 1,843,843 | 2,748,611 |
| Unmatched response | 203,078 | 890,020 |
| Observable absent from assessment reference | 0 | 9 |

The reference labels 8,927,868 enumerated responses. All 251,118 explicit
non-score responses have a null interpreted score. Tool grouping covers all
but nine assessment rows; their existing SNOMED concept labels remain present.
V4.1 supplies a CORE-OM observable omitted from v5/v6, restoring tool definitions
for 231 rows. The seed contains 4,043 public definitions for 525 current or
historical concepts.
`NA = Not Applicable` and numeric `9 = Terminally ill` have regression examples:
the first is non-score, the second is not a missing-response code.

Of 656,965 observations, 97,619 have no unit. Of 559,346 populated units, 102,701
match exact UCUM codes, 141,882 exact dictionary symbols and 119,983 recognised
aliases. Another 194,780 remain unresolved. No observation is deleted and no
numeric unit conversion is applied.

The combined build passed all 48 tests across 12 models in 92 seconds. After
adding the v4.1 definition, the seed and its three downstream models passed all
13 selected tests. Compilation and the source-generator test passed. A read-only
merge check against freshly fetched `origin/main` found no conflicts. Existing
unused-config and expired platform-credential warnings did not prevent warehouse
validation.

Reproduce the aggregate checks with
[`mhsds_clinical_interpretation_profile.sql`](../analyses/mental_health/mhsds_clinical_interpretation_profile.sql).
The [seed workflow](../scripts/reference/README.md) records the source workbooks
and extraction method. Unit semantics follow the [UCUM specification, sections 2-3](https://ucum.org/ucum).
MHSDS definitions are in the [current ETOS and guidance](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance)
and [archived specifications](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance/mental-health-services-data-set-archived-specification).

## CodeRabbit follow-up

The review of `272ec66e` identified a signed-upper-bound parsing gap. The parser
now extracts each bound from the complete range. Merely adding an optional minus
sign to the suffix would misread the separator in `0-40` as a negative sign.
Synthetic tests cover positive, mixed-sign, negative and decimal ranges, plus
non-range responses. Capture-group behaviour follows the
[Snowflake REGEXP_SUBSTR definition](https://docs.snowflake.com/en/sql-reference/functions/regexp_substr).

All 11 selected tests passed after rebuilding the reference and clinical fact.
The 525 current observable definitions have unchanged bounds. The fact still has
26,278,485 unique clinical items and excludes all 251,118 explicit non-score
responses from its interpreted score field.

Public reference-example failures now show expected and actual meanings.
Patient-linked checks still return aggregate diagnostics. The ownership warning
was dismissed: named business-owner metadata is required by project conventions
and is not a patient record. The review date uses Europe/London; the commit's
5 September local date is consistent with its 4 September UTC timestamp.
Separate ad-hoc aggregate queries remain a non-blocking cost consideration,
not a reason to add a routinely materialised profiling model.
