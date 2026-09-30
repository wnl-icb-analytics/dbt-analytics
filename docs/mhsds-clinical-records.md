# MHSDS clinical evidence

`fct_mhsds_clinical_record` brings together recorded diagnoses, presenting
complaints, assessment responses and clinical components of care activities.
Each row is a clinical item, not a visit or a completed questionnaire. Focused
diagnosis, complaint and assessment tables provide easier starting points for
those questions.

## Records and repeated submissions

| Evidence | What is retained | Meaning of the clinical date |
|---|---|---|
| Previous diagnosis, MHS601 | Latest accepted version of the identified diagnosis | When it was discussed, not necessarily onset |
| Provisional, primary and secondary diagnosis, MHS603-605 | Latest accepted version of the identified diagnosis; different codes remain separate | Recorded diagnosis date/time |
| Referral assessment, MHS606 | Each accepted response occurrence | Recorded completion date/time |
| Activity assessment, MHS607 | Each accepted response occurrence | Time inherited from a consistent activity in the same submission |
| Historical clustering response, MHS802 | Latest response per provider, source assessment and concept | Completion time from the matching MHS801 record in the same submission |
| Presenting complaint, MHS609 | Latest provider/referral/person/scheme/code/date item | Recorded complaint date, not necessarily symptom onset |
| Care-activity components, MHS202 | Each populated procedure, finding or observation component | Time and precision supplied by the established activity table |

Incomplete diagnosis and complaint identities remain separate source records.
Different person IDs remain separate, even when a national identity change may
explain them. A missing national person ID does not prevent diagnosis-version
selection when the provider-local identifying details are complete.

`clinical_record_id` and `source_record_id` identify the clinical item.
`originating_source_record_id` identifies its source occurrence and can repeat
across several components of one activity. Occurrence IDs do not promise stable
identity across replacement submissions. First and last reporting periods
describe the submission evidence.

## Dates, labels and values

Recorded timestamps take precedence over separate dates. Both remain available,
with disagreement and precision fields. Original time-zone offsets and submitted
precision are unavailable for some sources, so midnight does not prove that only
a date was recorded. Dates before 1901 are missing-date markers and are not used
as clinical time. No automatic time-zone correction is inferred.

Keep clinical codes with their declared coding schemes. Diagnosis and finding
schemes use different numbering. Labels come from maintained references;
unmatched and missing states remain explicit. A source-supplied SNOMED mapping
does not silently replace the submitted code. Complaint alternative-label
fields identify possible scheme ambiguity without resolving it.

Submitted assessment responses remain available as text. A numeric parse only
shows that text can be read as a number. `assessment_score_numeric` also requires
a response allowed by the instrument's maintained definition, including its
range and precision, and excludes recognised non-score responses. Unit matching
is case-sensitive; a similar-looking unit is not enough to establish equivalence.

## What can be concluded

Diagnosis rows describe recorded evidence, not a decision about the clinically
current diagnosis. Assessment rows can be individual questions or dimensions.
`fct_mhsds_assessment_instance` groups related responses without claiming a
completed questionnaire. Historical clustering definitions include the SARN
items from the specification's separate "Cluster Tools for MH" worksheet.
HoNOS and SARN responses form separate tool groups within a source assessment.
`assessment_tool_group` falls back to a concept code only when the tool is unmatched.
`fct_mhsds_assessment_score_change` compares successive
eligible scores within the same concept and context; current minus previous has
no universal clinical direction.

Currency models use their own diagnosis selection for costing. Their narrower
choice must not replace the general clinical evidence.

See the [domain overview](mhsds-domain-overview.md) and
[reporting guide](mhsds-domain-models.md) for the table families and joins.
