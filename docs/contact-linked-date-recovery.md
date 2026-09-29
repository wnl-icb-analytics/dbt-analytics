# Contact-linked date recovery

Care-contact dates are complete in the profiled MHSDS and CSDS facts. This
change recovers missing dates on their related activities and clinical items.
It does not infer diagnosis dates, person identities or clock times.

## Rules

- CSDS inherits the explicit same-submission activity/contact time when the
  parent exists and populated person identifiers do not conflict. Missing
  person identifiers and unknown consistency flags remain unknown. Contact
  history is unique on submission and contact identifiers.
- MHSDS keeps the submitted contact date first. When that date is unavailable,
  it uses `dmicActivityDate` from the selected activity row only if the date is
  after 1900 and within the reporting period. The existing time-basis column
  identifies `source_derived_activity_date`, with date precision and no clock
  time. The same provenance passes into linked clinical records.

Date-only records belong on their recorded day in the longitudinal timeline.
They cannot establish ordering within that day. Midnight in their timestamp
representation is not a supplied time of day.

## Validation on 11 September 2026

All 396 previously undated CSDS clinical items had exactly one submitted
contact, with matching provider, local contact identifier and date. Every one
lacked a person identifier; none had a known person conflict.

All 303 undated MHSDS activities had a valid source-derived date. Among dated
controls, all 18,043,373 populated derived dates agreed with the contact date.
Another 143 missing-date activities had one agreeing historical contact date.
Those historical submissions were inactive; the fallback uses the selected
activity's own derived date rather than attaching an inactive parent.

The tracked DEV build covered seven models and passed 51 tests. Three further
parent/date checks passed. Thirteen synthetic cases exercised supplied-date
precedence, absent dates, sentinel dates, dates outside the reporting period,
missing parents, unknown person consistency and known person conflicts.

| Output | Rows compared | Dates recovered | Existing dates changed | Keys changed |
| --- | ---: | ---: | ---: | ---: |
| MHSDS care activity | 18,043,704 | 303 | 0 | 0 |
| MHSDS clinical record | 26,278,485 | 365 | 0 | 0 |
| CSDS clinical record | 37,606,281 | 396 | 0 | 0 |

The 365 MHSDS clinical items include components and assessments of the
recovered activities; they are not additional encounters. All CSDS clinical
records and MHSDS activities are dated after the change. The 10,783 remaining
undated MHSDS clinical records are diagnoses, outside this recovery rule.

Full-row hash comparisons excluded temporal fields and date-derived quality
flags. No other fields changed in the activity or CSDS outputs. Eight MHSDS
clinical descriptions differed from the materialised production fact; all
eight use the current ICD-10 dictionary through unchanged labelling SQL.
Clinical codes, measured values and identity/linkage fields were unchanged.

Production was read only. These results validate DEV; production receives the
change through the normal merge and deployment workflow.
