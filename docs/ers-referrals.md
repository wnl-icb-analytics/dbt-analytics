# e-RS referrals and actions

`REPORTING.ERS.FCT_ERS_REFERRAL_ACTION` has one row per active submitted action.
It includes lifecycle and administrative actions, advice and triage requests.
`FCT_ERS_REFERRAL` has one row per UBRN identifier across those actions. It does
not require a Referral Created action, which is absent in part of the history.

Use `action_id` for the recorded sequence. Referral attributes come from the
highest action ID. `latest_action_name` can be an administrative action; it is
not a derived open/closed status. The latest non-null pathway start remains
separate from the first observed action date.

A request can involve several services. Last-recorded service, provider and site
fields come together from `last_service_action_id`. They do not claim to describe
a current destination. Full changes remain in the action fact. Initial, previous
and next UBRNs remain as recorded; no journey is inferred.

`sk_patient_id` uses the shared pseudonymised NHS-number key. Blank values and
the unknown marker 1 become null. Conflicting keys within a request produce a
null referral key and fail the consistency test. Missing keys do not remove
requests. Recorded patient, practice and residence context is retained on actions
and copied from the latest action onto requests. It is separate from current
person demographics. See [analyst validation](ers-analyst-validation.md) for
the legacy comparison, context timing and worked analyses.

## Names and codes

`REFERENCE.DATA_DICTIONARY.ERS_*` exposes e-RS action, reason, priority, clinic,
appointment type, assessment outcome, specialty and service definitions. These
administrative dictionaries belong in DATA_DICTIONARY under the current project
conventions, rather than TERMINOLOGY as originally suggested in #853.

The lookups include every retained code, without filtering closed or historical
values. Action and reason codes absent from the maintained dictionary use their
latest supplied display by action ID. `name_source` in reference identifies this
fallback. No historical effective-date definition history is supplied or invented.

Organisation names use the existing UKHFD ODS history, Dictionary and closed
archive, supplemented by e-RS application organisations. Source labels are the
last fallback. Names are latest available definitions, not labels at action time.
Service names can be looked up without assigning a historical provider from the
current service directory. Search and service specialties remain separate;
e-RS specialty is not a national treatment-function code.

## Refresh and source replacement

`STG_ERS_UBRN_ACTION` is a view with an ActiveSubmission existence filter. New,
replaced and withdrawn submissions are reflected directly, without a daily
full-table staging rebuild or a watermark that could miss old corrections.
The action ID is unique in the active population. No arbitrary deduplication
is applied.

The e-RS feed loads daily before the scheduled build, so the `models/reporting/ers`
folder carries the `daily` tag and the scheduled `+tag:daily` selection rebuilds
these tables and their parents each day. The facts rebuild from the complete
active population, so corrections and removed submissions disappear on refresh.
Use the tracked DEV target for development.

## Validation and legacy differences

The 10 September 2026 profile contains 118,671,778 active actions and 18,678,390
unique requests. All 6,057 month/action-code groups reconcile exactly to staging.
The source has 104,192 inactive rows. Normalised UBRN is also unique at the
referral grain. No request has conflicting non-null patient keys.

The legacy action base has 118,776,168 rows. Its SQL omits ActiveSubmission and
its lookup joins add a further 198 rows over the unfiltered source. The new facts
preserve the tested action grain. The legacy referral table selects action 1422,
no previous UBRN, from 1 April 2019. It is a narrower population.

The legacy RAS outcome procedure filters action 1608 and labels it Ras Request.
That is a request action. Recorded triage outcomes remain separate action and
reason codes here. The published ERS_ALL_SOURCES report also applies provider
and period filters. These facts do not replace its published definitions.

Final full writes on the tracked Medium warehouse took about 82 seconds for
actions and 24 seconds for referrals, including the additional label lookups.
Both tables cluster by patient and date for person-history analysis. Due date
and test-patient flag are omitted because both are entirely unpopulated. A
missing test flag must not be interpreted as false.

A direct-reference check found 3,010,321 OLIDS/e-RS matches: 3,006,805 with
matching patient keys, 2,359 with conflicting keys and 1,157 with missing keys.
Reference agreement alone is not a confirmed person-level link.

## Dashboard measures

`FCT_ERS_DASHBOARD_MEASURE_EVENT` holds the NHS England e-RS dashboard measures as
one row per measure and counted action. Rules follow the
[dashboard metadata](https://digital.nhs.uk/dashboards/e-referral-service-dashboard-private-version)
("Download the full data logic") and the Databricks SQL embedded in it, which is
more specific than the written definitions. The model description gives each
rule. Points to know:

- NHS England runs the SQL one Monday-to-Sunday week at a time. Only referrals
  accepted depends on the period: it takes the latest accept per referral within
  each week (`week_end_date`), so sum weeks for longer periods.
- An accept with a Referral Updated (1423) action at the same time is not counted
  unless accept and auto-accept are both present, when the auto-accept counts.
- A&G first response takes its organisation and specialty breakdowns from the
  initial request and is counted in the week of the response.
  `advice_response_time_band` uses the dashboard's 48, 120 and 240 working-hour
  thresholds.
- `dashboard_specialty_*` is the service specialty where present, otherwise the
  referral search specialty, as the dashboard groups it.
- Received by provider excludes actions without a provider, as the dashboard does.
- RAS outcomes use Triage Outcome Recorded (1770), not the request action.
- DBS bookings are not built: NHS England flags directly bookable services from
  the EBSX05 service extract, which this project does not load.

## Sources

- [NHS e-RS extract descriptions](https://digital.nhs.uk/services/e-referral-service/reports-and-statistics/ebsx-reports)
- [NHS RAS interpretation and action sequencing](https://digital.nhs.uk/services/e-referral-service/document-library/ras-interpreting-data)
- [NHS guidance on decoding extracts](https://digital.nhs.uk/services/e-referral-service/document-library/extracts-translating)

The queries in `analyses/ers/` return aggregate validation without patient records
or identifier examples.

## Final profile and review

All 44 action columns and 38 referral columns were profiled. None is entirely
empty. Every populated action and reason code has a label. The reference tables
contain 56 action codes and 108 reason codes; five action and three reason labels
come from their latest supplied displays with explicit reference provenance.
All populated referring, action, provider and site organisation codes have names.
There remain 127 action rows without a service name and 1,007 actions without a
supplied action code. These are retained; missing codes cannot define a lifecycle
event without further source information.

All copied action fields have the same whole-table fingerprint as the 118,671,778
active source rows. Synthetic checks using compiled SQL passed for source
replacement, withdrawal, referral history without a creation action, latest
service details after an administrative action, and conflicting patient keys.

Code review suggested a single-read aggregation for referrals. A full-size
comparison produced an identical fingerprint across all 18,678,390 rows, but its
build took 47 seconds versus 25 seconds for the existing narrow aggregation and
joins on selected action IDs. The faster existing query is retained. The fallback
reason provenance was corrected to identify action-reason displays explicitly.

The final test selection passed 44 tests. The existing staging completeness test
warned on the 1,007 actions without a code; those source rows remain available.
