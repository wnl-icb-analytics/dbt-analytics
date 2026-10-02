# ECDS measurement coverage and review

Reviewed on 16 September 2026 against foundation commit `c8b2b25c`.

The implementation faithfully preserves the submitted records. That conclusion
is supported by an exhaustive comparison of about 105M rows against the source
at the build time, rather than a sample. I would use these facts for provenance,
record discovery and qualified descriptive analysis. I would not yet use their
numeric fields as standardised measurements or treat missing records as absence
of assessment.

## What was checked

The review covers both new facts, their staging and reference models, source
payloads, key uniqueness, lookup cardinality, attendance and patient linkage,
coverage by year/provider, code and unit resolution, values, timestamps and
repeated content. It also checks the two unmatched assessment codes against the
UK SNOMED terminology server. No patient rows were exported.

The [SQL profile](../analyses/acute/ecds_measurement_profile.sql) and
[companion notebook](../analyses/acute/ecds_measurement_review.ipynb) retain the
queries and non-identifying aggregate evidence. The notebook includes the
same-snapshot checks used for the conclusions below.

## Refresh alignment matters

The facts were built at 14:24 BST. Source child tables refreshed around 14:59,
and the shared attendance model was rebuilt during the wider source update.
A later live comparison found about 1.5M old observation rows and 1.2M old
assessment rows whose keys were no longer in the latest source. All source
fields still agreed for keys present in both versions.

Snowflake Time Travel at **13:24:20 UTC on 16 September 2026** reproduces the
fact-build source. Against that snapshot, both facts have **zero missing rows,
zero added rows and zero altered source fields**, including codes, values, units,
timestamps, approval flags, sequences and import identifiers.

The shared attendance model also had about 18.4M rows when profiled, while the
source snapshot used by the facts had about 18.1M. Using those different versions
would understate recent coverage. The tables below use the source attendance
snapshot aligned to the facts. Source retention is one day, so the recorded
aggregates outlive the ability to rerun this exact historical comparison.

This is an operational limitation. Source-derived keys are not immutable across
resubmissions. Build related facts and attendances after a completed source
refresh and verify that they describe the same delivery before joining them.
A stored `is_attendance_linked` flag describes the join at build time.

## Coverage of delivered attendances

The denominator is the ECDS population delivered to this warehouse. It is not
all NHS emergency activity, all activity at a named trust, or the population
clinically eligible for an assessment. A record counts whether its value or unit
is usable. The 2018 and 2026 periods are partial years.

| Attendance year | Delivered attendances | With observations | With scored assessments |
|---|---:|---:|---:|
| 2018, from April | ~675K | 0.24% | 0.20% |
| 2019 | ~2.10M | 6.97% | 0.18% |
| 2020 | ~1.70M | 10.19% | 0.24% |
| 2021 | ~2.18M | 27.30% | 11.67% |
| 2022 | ~2.31M | 45.54% | 29.06% |
| 2023 | ~2.26M | 49.32% | 33.90% |
| 2024 | ~2.42M | 60.08% | 37.96% |
| 2025 | ~2.54M | 60.69% | 39.44% |
| 2026, to the September snapshot | ~1.93M | 55.88% | 39.37% |

Fewer than 1K attendances have no arrival date. Observation coverage
in 2026 falls from about 61.5% in January/February to about 53% from April.
Assessment coverage stays around 38% to 41% over the complete recorded months.
This is evidence of changing capture, not a measured change in clinical need.

Among 2025 major emergency department attendances, coverage is **75.94% for
observations and 53.56% for assessments**. Provider variation is substantial:

| Provider, major departments only | Delivered 2025 attendances | Observation coverage | Assessment coverage |
|---|---:|---:|---:|
| Royal Free | ~310K | 63.00% | 62.93% |
| Chelsea and Westminster | ~244K | 85.85% | 59.92% |
| London North West | ~166K | 90.89% | 72.40% |
| UCLH | ~164K | 96.35% | 0.36% |
| Imperial | ~146K | 93.75% | 72.82% |
| Whittington | ~83K | 0.00% | 12.06% |
| Hillingdon | ~69K | 97.33% | 73.15% |

These differences rule out interpreting missing records as normal observations,
zero scores or care not delivered. Trend and provider comparisons need an
explicit reporting population and capture assessment.

## Terminologies and code families

Both clinical code fields are intended to contain SNOMED CT identifiers. ECDS
ETOS supplies the dataset-specific permitted lists, preferred labels and group
codes; it is not a separate clinical terminology. General SNOMED labels come
from the shared UK concept reference when a code is outside the ECDS list.

| Clinical field | Observed code coverage | Records |
|---|---|---:|
| Observation code | 6 ECDS-list SNOMED codes | ~62.2M |
| Observation code | 27 further recognised SNOMED codes | ~18K |
| Observation code | 3 unresolved codes | ~1K |
| Observation code | No code supplied | <1K |
| Assessment tool code | 12 ECDS-list SNOMED codes | ~42.7M |
| Assessment tool code | 11 further recognised SNOMED codes | ~3K |
| Assessment tool code | 2 unresolved codes | ~192K |

Labels resolve for **99.997% of observations and 99.551% of assessments**.
This is label coverage, not clinical validity. No ICD-10, OPCS-4 or Read mapping
is applied to these two feeds.

The six core observations are respiratory rate, blood oxygen saturation,
core body temperature, pulse, systolic pressure and ACVPU consciousness.
The assessment feed is overwhelmingly NEWS2:

| Published assessment family | Records | Distinct attendances |
|---|---:|---:|
| NEWS2 components and total | ~42.5M | ~4.3M |
| Verbal Rating Scale pain | ~147K | ~57K |
| Rockwood Clinical Frailty Scale | ~16K | ~15K |
| 4AT delirium | <1K | <1K |

NEWS2 is 99.16% of all assessment rows. Its eight component-code options include
the two alternative oxygen-saturation scales; the total score has its own code.
There are about 3.9M reported NEWS2 total-score rows across about 2.0M attendances.
Do not sum the total and components together. The family attendance counts above
are not additive because an attendance can have several tools.

Unit metadata mixes UCUM, the warehouse unit dictionary and local spellings:

| Unit resolution | Observation rows | Share of all observations |
|---|---:|---:|
| Exact code in the finite UKHFD UCUM reference | ~9.4M | 15.02% |
| Warehouse dictionary symbol | ~3.4M | 5.47% |
| Established alias in either reference | ~2.3M | 3.74% |
| Supplied unit not resolved | ~26.9M | 43.17% |
| Unit absent | ~20.3M | 32.60% |

UCUM matching is case-sensitive. Reference absence does not prove that a UCUM
expression is invalid. Dictionary and alias matches do not certify UCUM validity.
The original unit and its label source remain separate from any resolved symbol.

ACVPU uses five categorical letters, A/C/V/P/U, in the value field. They are
responses, not additional SNOMED concepts. Of about 9.2M ACVPU records, 99.6%
resolve to a response label. About 29K are unrecognised text responses,
about 4K numeric responses and about 4K absent responses. A unit is legitimately absent
for ACVPU. ECDS group codes `NEWS2`, `PAIN` and `FUNC` are classification metadata;
`FUNC` is split into delirium and frailty using the specific published tool.
Provider and site codes are attendance identifiers, not clinical terminologies.

## Findings that limit clinical use

1. **High: unit matching is not unit validity.** Only 24.23% of observation rows
   resolve to a unit label, and even some resolved units conflict with the
   observation. There are about 15K temperature records labelled `bpm`, which the
   shared dictionary correctly resolves to `/min`. Another 78K respiratory
   rate records carry `%`. Broadly accepting all matched units would therefore
   admit contradictory measurements. Preserve the reported unit and assess
   compatibility with the observation before comparison or conversion.

2. **High: units cannot be inferred safely from values.** The ~1.5M unlabelled
   core-temperature records have an approximate median of 97.9. This suggests a
   different unit convention from Celsius, but it does not identify the unit of
   each record. Common local spellings such as `DEGC`, `BRMIN`, `MMHG` and `BPM`
   need source-backed, observation-specific mappings. Some spellings also occur
   against the wrong observation, so a global alias replacement is insufficient.

3. **High for longitudinal coverage: missing patient linkage.** All records had
   an attendance at build time, but about 2.6M observation rows (4.17%) and 1.3M
   assessment rows (3.09%) have no patient key. Attendance linkage does not prove
   person linkage. An adapter requiring a patient key must quantify this loss.

4. **Medium overall, material for affected providers: unresolved assessment
   codes.** `1104051000000100` and `1104331000000100` account for about 192K rows.
   Both returned 404 from the UK SNOMED server; the valid NEWS2 total-score
   control code resolved in the 26 August 2026 UK edition. Barts accounts for
   about 151K of these rows. Source approval is not a reliable substitute: about 50K
   unresolved rows are marked approved. Confirm intended codes with the supplier;
   do not repair their final digits by similarity.

5. **Medium: numeric parsing does not validate scores.** About 20K numeric NEWS2
   component rows fall outside the published discrete point values, out of
   about 38.6M numeric component rows (0.051%). This includes about 1K of the
   ~36K oxygen-saturation scale-2 records. Component validity follows the
   [RCP NEWS2 scoring chart](https://rcp.ac.uk/media/alxev00t/news2-chart-1_the-news-scoring-system_0_0.pdf).
   Flag these for quality review; do not recalculate scores from incomplete or
   ambiguously paired observations. Code placement can also be misleading:
   about 2K observation rows use the NEWS2 respiration-component code but have an
   approximate median value of 16. Recognising the code does not establish that
   its value has the meaning stated by the code.

6. **Medium: timestamps and repeated content need qualified use.** Timestamps
   are present throughout and none is future-dated at review time. However,
   about 14K observations and 56K assessments predate the attendance arrival
   date; about 141K observations and 160K assessments are over a week later.
   These are diagnostic exceptions, not automatic exclusions. There are also
   about 106K observation and 118K assessment sequences that repeat another
   record's attendance/code/value/unit/time content. Their source keys differ;
   the evidence does not establish which are replayed deliveries versus genuine
   repeated records. Preserve the grain and avoid unqualified event counts.

## Review judgement and next steps

No transformation defect was found in key preservation, source field mapping,
left joins or reference cardinality. The record-type-specific hashes prevent
observation and assessment IDs colliding merely because their attendance and
sequence are equal. Numeric parsing retains the source text, and unresolved
codes remain visible. The new family does not route these records into procedures.

The separation agrees with the
[ECDS guide, sections 5.12 and 5.13](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/ecds/ecds-user-guidance-v4.7---approved.pdf).
That guidance requires only the first relevant assessment to be submitted; this
is not a complete bedside-monitoring record. Historical labels also do not prove
that a code was permitted on the event date.

Before using these as standardised longitudinal measures, I recommend:

- Coordinate source, attendance and child-fact refreshes, and check current
  join consistency before publishing a longitudinal extract.
- Add a source-backed observation/unit compatibility definition, with separate
  statuses for recognised spelling, compatible unit and authorised conversion.
  Keep raw values and do not infer absent units.
- Resolve the two unrecognised assessment codes with the supplier, and
  report score-domain exceptions separately from numeric parse success.
- Make provider/year coverage and missing patient keys visible in downstream
  eligibility and counts. Keep the first-observation submission limitation.

This review does not establish clinical correctness of individual records,
complete provider reporting, or valid pairing of component scores into a single
assessment session. The appropriate next work is a quality layer and source
clarification, not treating every labelled record as a validated measurement.
