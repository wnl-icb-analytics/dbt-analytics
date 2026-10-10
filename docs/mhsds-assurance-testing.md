# MHSDS assurance and testing

This records the testing behind the current MHSDS reporting layer: what was
checked, what the checks found and what remains uncertain. Use the
[analyst overview](mhsds-domain-overview.md) to choose a table and the
[reporting guide](mhsds-domain-models.md) for its definitions.

Evidence review updated: 23 September 2026. Counts below describe development
snapshots, not fixed targets for later submissions. Analyst acceptance has not
yet been recorded.

## Assurance position

Aggregate checks found that selected source records reach the intended reporting
tables without unexplained row loss. Tested grains, links, dates and semantic
measures hold. The profiles also found gaps in submitted data. The models leave
these visible rather than inventing referrals, patients, beds or clinical states.
A passing test proves its stated rule, not fitness for every analytical use.

Validation combined specification and reference-data checks; profiles of
versions, counts and links across source, staging and reporting; DEV builds and
regression tests; and direct reporting-to-semantic comparisons. The development
record is spread across the [referral/contact PR](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1025),
[source-selection PR](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1026),
[inpatient PR](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1075),
[activity PR](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1082),
[clinical PR](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1087) and
[domain expansion PR](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218).
The removed [domain review](https://github.com/wnl-icb-analytics/dbt-analytics/blob/b56cb2047f035006c821afc341d9c2ae69ddc8f4/docs/mhsds-domain-review.md)
and [clinical-method review](https://github.com/wnl-icb-analytics/dbt-analytics/blob/b56cb2047f035006c821afc341d9c2ae69ddc8f4/docs/mhsds-clinical-method-review.md)
hold further historical profiles. Their old model instructions are superseded.

## What was checked

### Submission selection and referral links

The [source-selection review](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1026)
profiled null keys, repeated periods, changed values, version ordering and
parent-link coverage against MHSDS v6 guidance. Staging selects the latest
accepted version for current-state models and retains accepted period history
for time-based questions. One reviewed build passed 27 models, 121 tests and a
snapshot. This checks how supplied records are selected; it cannot recover an
unsubmitted provider record.

The [referral/contact build](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1025)
passed 170 selected nodes. Source-to-fact counts, referral and contact grains,
relationships, sentinel dates and the WNL commissioner regression were checked.
The profile found 122,926 contacts without a matching current MHS101 referral,
mostly in v6. The contact is retained with an explicit missing referral link.

The [RTT profile](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
reconciled 819,599 accepted records exactly across raw, staging and reporting.
It found 535,804 populated starts and 194,123 populated ends. The MHS104
specification explains why these need not occur in equal numbers. All pathway
identifiers were absent, so the available grouping is diagnostic rather than
a certified count of distinct waiting-time clocks.

The [person-summary check](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
confirmed that all identifiable people in modelled evidence are represented,
including people without a retained referral, contact or inpatient record. Its
final focused build passed three models and eight tests. Current caseload
reconciles open referrals and distinct people to the latest dataset month. The
July profile found 180,159 open referrals and 152,034 people; 71,593 open
referrals had provider evidence more than two months behind the dataset month.
A recorded open referral is not proof of active treatment or recent provider
reporting.

### Care activity, staff and group care

The [activity build](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1082)
reconciled 18,043,704 accepted MHS202 care activities to the activity fact and
5,292,207 accepted MHS901 records to retained source rows. It profiled
pre-v6 direct staff links and v6 MHS206 links separately. Targeted builds
passed, including 23 post-review nodes; all 6,422 selected project nodes
compiled. Staff and team links have their own grain, so joining them can
multiply contact rows. Unlinked staff evidence remains visible.

The [domain expansion](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
compared contact identity and counts before and after refactoring. Contact,
indirect-work and care-plan totals were also checked through the semantic view.
The total cost difference in the existing costing comparison was zero.
Identifiable group-therapy contacts are a subset of contacts; anonymous group
sessions have no recoverable attendee list. None of these checks establishes
that every delivered activity was submitted.

### Inpatient care, legal status and capacity

The [inpatient build](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1075)
checked version-specific source fields, one-row grains and same-submission ward
context. Its final targeted build passed three models and 23 tests. The profile
contained 112,406 spells and 206,437 ward stays; each stay matched at most one
retained spell. Reversed dates, unmatched ward context and unavailable date
comparisons were exposed rather than silently corrected. Occupancy and
detention regression comparisons in [#1218](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
preserved the established rules. They do not independently establish clinical
correctness.

The [capacity check](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
retained all 63,452 accepted ward/submission records. In July 2026, 347 of 651
ward-periods reported available-bed days and 314 reported both available and
closed-bed days. Earlier records for the same provider and ward were examined,
but older or other-submission values were not substituted for accepted periods.
Three models and ten tests passed; semantic totals reconciled for 100 periods.
Recorded capacity is usable where submitted, but does not support a complete
live bed-utilisation rate across providers.

### Clinical evidence and assessments

The [clinical build](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1087)
passed 37 models, two seeds and 147 tests, with one existing
prescribing-coverage warning. Aggregate reconciliation covered 26,278,485
unique clinical items; currency row counts and checksum comparisons stayed
unchanged. Diagnosis revisions and incomplete but identifiable records were
retained. Assessment bounds, unit-code case and recorded times were checked.

The [later assessment work](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
retained all 24,443,224 observations. Published definitions labelled 2,702,663
previously unmatched observations, leaving nine unmatched. Component-level
grouping corrected the assessment-instance grain. Defined unknown responses
did not become numeric scores. Descriptive score change is not clinical
improvement, and a recorded diagnosis is not a decision about which diagnosis
is clinically current.

The complaint profile checked the declared Read-code meaning. It labelled
5,537 more complaints, bringing coverage to 37,668 of 47,703 relevant records.
Ambiguous and unmatched codes remain visible.

### Circumstances, dates and labels

The [employment check](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
retained all 11,578,686 observations and labelled all 9,837,186 populated
weekly-hours codes. Eight models and 19 tests passed. Weekly hours are
submitted categories, not numbers to sum or average. Accommodation and
employment label coverage were profiled separately; an unlabelled code was not
treated as a known circumstance.

The [care-plan correction](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
retained 6,812,996 records. All 5,805,562 populated update times matched the
source time of day, including 52,455 midnight values, with no mismatch. Six
models and 14 tests passed. The 1970 anchor on a source time is no longer
presented as a clinical update date. Selected sentinel-date checks and the
published Snowflake DATE/TIME types were also inspected.

### Semantic view and published descriptions

The [final semantic checks](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
reconciled tested entity counts and breakdowns to reporting SQL, including
period demographic joins. A 144-model DEV run and three-model reference
follow-up passed. Warehouse metadata matched all 144 checked table or view
descriptions. Static comparison found the description pass changed SQL
comments, not model queries. These checks cover named measures and selected
groupings, not every question an analyst or agent might ask.

## Build and deployment result

The main [#1218 DEV build](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1218)
ran 136 models, 403 tests and two snapshots. All MHSDS tests in that selection
passed. A separate segmentation row-count test failed, so the full run was not
green. Later focused builds checked the MHSDS corrections above. The
[first production deployment](https://github.com/wnl-icb-analytics/dbt-analytics/actions/runs/35445420585)
also failed on segmentation after the 52 MHSDS reporting models and semantic
view built; a failed deployment does not roll back built tables.
[PR #1219](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1219)
removed the invalid comparison of historical segmentation with current
demographics. Its [production deployment](https://github.com/wnl-icb-analytics/dbt-analytics/actions/runs/35586115105)
succeeded. That state-based deployment did not rerun the full MHSDS selection.

The final CodeRabbit review of #1218 was skipped because the PR exceeded its
file limit. Resolved threads and deployment status are not a substitute for
the profiles, tests or analyst acceptance.

## Permanent regression checks

Model YAML tests protect row grains and documented fields. These SQL checks
guard particular failures found during development:

| Question | Checks |
|---|---|
| Do person population and recorded caseload reconcile? | [Person summary](../tests/mhsds_person_summary_rules.sql), [caseload](../tests/mhsds_current_caseload_reconciles.sql), [period evidence dates](../tests/mhsds_referral_period_evidence_dates.sql) |
| Are diagnoses and clinical items retained without duplicate identities? | [Diagnosis source](../tests/mhsds_diagnosis_source_reconciliation.sql), [clinical records](../tests/mhsds_clinical_records_reconcile.sql), [clinical identity](../tests/mhsds_clinical_record_identity.sql) |
| Are assessment responses and instances interpreted consistently? | [Response semantics](../tests/mhsds_assessment_response_semantics.sql), [signed ranges](../tests/mhsds_assessment_signed_ranges.sql), [pair context](../tests/mhsds_assessment_pair_context.sql) |
| Do version and context rules hold? | [Care activity fields](../tests/stg_mhsds_care_activity_uses_spec_version_fields.sql), [contact context](../tests/mhsds_contact_context_rules.sql), [inpatient fields](../tests/mhsds_inpatient_uses_specification_defined_fields.sql) |
| Are known label and date errors guarded? | [Employment codes](../tests/mhsds_employment_reference_coverage.sql), [complaint Read codes](../tests/mhsds_complaint_declared_read_labels.sql), [sentinel dates](../tests/mhsds_reporting_dates_exclude_source_sentinels.sql) |
| Do named semantic counts agree with reporting? | [Semantic count reconciliation](../tests/generic/mhsds_semantic_counts.sql), attached to `sem_mhsds` |

These checks are narrower than the aggregate profiles. The date test names
particular fields; the semantic test names particular measures and breakdowns.

## Remaining assurance work

Analysts still need to test real questions against direct reporting SQL and the
semantic view with the same provider, period, population and unknown
categories. Start with current caseload, historical referral state, group
care, diagnoses and outcomes, occupancy and reported capacity. Record the
query, interpretation, aggregate result and unresolved gap. Compare
provider/month coverage before interpreting differences between providers or
groups. Equal totals can conceal different members, so compare aggregate
missing and extra key counts where membership matters.

For future changes, record the commit, DEV target, selected dbt nodes, upstream
freshness, build result, warnings, aggregate reconciliations and reviewer. Use
the tracked shared DEV layers and check downstream selection before building.
Keep patient-level inspection in an approved Snowflake session. Repository and
GitHub evidence must contain only non-identifying aggregates or synthetic
examples.

Provider completeness, clinical correctness and the meaning of an unsubmitted
event remain outside automated checks. The delivered scope is recorded in
[epic #1028](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1028).
Further modelling of self-harm, assaults, police assistance, digital
interventions and deferred referral or attendee detail is parked. These
subjects have no assurance decision in the current reporting layer.
