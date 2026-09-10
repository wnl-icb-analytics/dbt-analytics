# e-RS analyst validation

This review compares the published e-RS facts with the NHS extract guidance,
`REPORTING.MAIN_DATA` and the ECDS encounter model. Profile date: 10 September
2026. All results below are whole-population aggregates unless a threshold is
stated. Patient records and identifier examples are not included.

## What an analyst should count

| Question | Model and unit | Date and selection |
|---|---|---|
| How many requests have we observed? | `fct_ers_referral`: one UBRN identifier | Includes advice, triage and requests without a creation action. Use first observed action for an observation cohort, not as an asserted referral creation date. |
| How many referrals were created? | First action 1422 per UBRN in `fct_ers_referral_action` | Creation action time. The legacy initial-referral population also excludes a previous UBRN and starts in April 2019. |
| How much workflow activity occurred? | `fct_ers_referral_action`: one active submitted action | Action time; distinguish action code and reason. |
| How many providers received requests? | First eligible action per UBRN/provider | The worked example reproduces the legacy receipt action set 1412, 1430 and 1608 with a deterministic tie-break. A request can reach several providers. |
| How many appointment slots are recorded? | `fct_ers_appointment`: UBRN, service and scheduled timestamp | Scheduled time. Latest recorded action is extract-current, not status as of that scheduled date. |
| Which actions concern each slot? | `rel_ers_appointment_action` | One row per action linked to a slot. Joining this into appointments multiplies slots. |
| Which acute records carry the same booking reference? | `rel_ers_referral_acute_record` | One UBRN/acute-source/acute-record match. Require patient-key agreement for person-level linkage; this does not prove a completed referral pathway. |

The profile contains 118,671,778 actions, 18,678,390 requests, 12,675,218 slots,
39,579,692 slot/action links and 19,799,328 recorded-reference acute links.

## Lessons applied from ECDS

`obt_encounter_uec` publishes a wide, labelled encounter without multiplying its
grain. It retains recorded patient and commissioning context, financial dates
and useful clinical fields. Repeating clinical records live separately. Its
recorded practice and residence are not replaced by current person attributes.

The e-RS action fact now follows that approach for supplied age, sex, residence
LSOA, IMD 2019, registered practice, local authorities and referrer commissioner.
It also supplies financial year, financial month, month name and ISO week end
from the action date. All existing columns retain their meanings.

The request fact carries the patient and commissioning context from
`latest_action_id`, matching its existing rule for other request attributes.
For creation-time or receipt-time analysis, select the appropriate action
instead. The appointment fact retains its slot grain; join its
`latest_action_id` to the action fact for that action's recorded context.
That context is not asserted to describe the future appointment date.

Names prefer retained reference definitions. Organisation names use the shared
UKHFD ODS history, Dictionary and closed archive. e-RS sex uses its application
dictionary, not national numeric sex codes. Geography uses Dictionary
ONSCodeEquivalent, retaining terminated codes. Supplied names remain fallback
where available. No terminology server is queried by analysts.

There is an important limit to the ECDS analogy. The public e-RS guidance does
not establish when patient age was calculated or when locally added patient
geography was resolved. These columns therefore describe context supplied on
the action; they do not claim a fully validated demographic snapshot at event
time. IMD explicitly names its 2019 release. It is not mixed with 2025 deciles.

## Comparison with REPORTING.MAIN_DATA

The eight `ERS_*` views select corresponding physical tables in
`MODELLING.MAIN_DATA`. Their producers all use the common action base.
They have a common 70-column reporting shape but different selections.

| Legacy object | Recorded selection | Observed grain and implication |
|---|---|---|
| `ERS_REFERRALS` | Action 1422, no previous UBRN, from April 2019 | 10,218,891 rows / 10,206,225 UBRNs. Counting rows overstates distinct requests by 12,666. |
| `ERS_REFERRALS_RECEIVED` | Earliest 1412/1430/1608 timestamp per UBRN/provider | 11,464,902 rows / 11,458,282 pairs. Timestamp ties and joins do not guarantee one row per pair. |
| `ERS_REFERRALS_ACCEPTED` | 1533/1420/1423, with a first-timestamp join | 3,468,804 rows / 3,448,163 UBRNs. Preserve the local definition separately from general request state. |
| `ERS_REFERRALS_REJECTED` | Action 1424, no previous UBRN | 135,746 rows / 131,312 UBRNs. This counts selected activity, not unique requests. |
| `ERS_RAS_REQUEST` | Action 1608 | 5,911,957 rows / 5,321,231 UBRNs. Requests can recur. |
| `ERS_RAS_OUTCOME` | Also action 1608, labelled Ras Request | Same 5,911,957 rows. The producer selects requests, not outcomes. Use 1770 for recorded RAS outcomes. |
| `ERS_ASI` | Action 1430 | 2,921,282 rows / 2,762,331 UBRNs. Worklist-entry activity is not a count of current waiting referrals. |
| `ERS_ASI_OUTCOME` | Action 1810 | 952,206 rows / 919,975 UBRNs. Booking-deferral outcomes, separate from RAS. |

The legacy action base omits ActiveSubmission and its dictionary joins multiply
some source rows. The new facts remove replaced submissions through staging and
test their stated grains. Differences in these populations are deliberate;
the new facts are not a byte-for-byte replacement for report selections.

Two further dynamic reporting tables, `ERS__REFS_CREATED` and
`ERS__REFS_RECEIVED`, have 137,767 and 4,501,351 rows. Their refresh history shows
10 September refreshes; both extend through 6 September. Their activity sums
equal their row counts. The definitions establish narrower dashboard populations:

- Created starts from legacy referrals, stops at its latest observed Sunday
  timestamp and filters a derived referring commissioner name to North Central
  London. Receiving organisation is the first received record per UBRN. Its
  tie-break uses the same UBRN again, so equal timestamps have no deterministic
  ordering. Both commissioner lookups use `DEV__MODELLING.CANCER__REF` objects.
- Received starts from legacy receipts, applies the same Sunday approach and
  limits providers to the local RAL/RAN/RAP/RKE/RP4/RP6/RRV prefixes. Organisation
  context combines Dictionary, WNL practice lookup and a DEV cancer organisation
  dimension. It is not an unrestricted e-RS dataset.

Neither output exposes its request identifier, making duplicate detection
difficult for consumers. Both age-band expressions send missing ages to `80+`.
Rolling periods use the latest action timestamp while the visible data stops at
the latest Sunday, so these are not complete calendar-year windows. Retain these
report choices separately from shared request and action facts. Production e-RS
facts should not inherit their DEV dependencies or missing-age classification.

| Legacy field group | New analytical route |
|---|---|
| PRIMARY_ID, PATIENT_ID | `ubrn` and the normalised shared `sk_patient_id`; count the documented key for the intended grain. |
| DATE_START, DATE_END, ERS_DATETIME_INITIAL_ASSESSMENT | `pathway_started_at`, `action_at`, `appointment_at`. The legacy initial-assessment field is actually the supplied appointment timestamp. |
| FIN_YEAR, FIN_MONTH, FIN_MONTH_NAME, DATE_WEEK_END | Action financial fields and ISO week end. Financial-year format follows the shared ECDS macro, for example 202526, rather than the legacy slash format. |
| PROVIDER and SITE | Recorded provider/site codes with retained labels. The legacy site lookup uses the current service directory; it does not prove the historical site. |
| PATIENT_AGE, GENDER | Supplied `patient_age`, `patient_sex_code` and `patient_sex_name`. No current-person substitution. |
| GP, BOROUGH, COMMISSIONER, LSOA, DEPRIVATION | Supplied registered practice, local-authority and referrer-commissioner context, labelled geography, and explicitly versioned IMD 2019. |
| PRIORITY, CLINIC, SERVICE, APPOINTMENT TYPE | Separate labelled code fields on the action fact. |
| TFC_CODE, TFC_NAME, IS_GENERAL_ACUTE | Legacy local specialty mapping is not a national treatment-function mapping. Use separate search and service specialties. A General & Acute or cancer reporting grouping requires a maintained, owned definition. |
| ERS_REF_OUTCOME_CODE/NAME | `action_reason_code/name`, interpreted together with action code. They are not universal request outcomes. |
| ACTIVITY, POD, POD_GROUP | Count the chosen grain and apply a stated action selection. Do not sum rows across overlapping legacy outputs. |
| ETHNICITY | Entirely blank in all ten inspected legacy outputs. Current ethnicity can be joined explicitly from the person dimension; it is not supplied historical e-RS ethnicity. |
| CDS_UNIQUE_ID, GP_LOCALITY, MAIN_SPECIALTY, HRG, DIAGNOSIS, PROCEDURE, POSTCODE_ID, ERS_REFERRAL_REASON | Empty placeholders in the common legacy action-base SQL; no equivalent e-RS fact column is warranted. |
| BASE_COST, TOTAL_COST, BED_DAYS, CC_BED_DAYS, EXCESS_BED_DAYS | Literal zero in the legacy base, not measured costs or bed use. Omitted. |
| IS_PBR, ADMIN_CATEGORY, COM_SERIAL_NUMBER, IS_SPEC_COMM | Empty legacy placeholders. Omitted. |
| Provider groups, organisation type/commissioner groups, PCN, locality, age bands and rolling-period categories | Dashboard/dimension derivations. Retained organisation codes, supplied age and action dates allow these analyses without hiding identifiers or mixing grains. Reuse maintained organisation definitions; do not copy the dashboards' DEV cancer dependencies, current-organisation assignments as historical facts, or null-age-to-80+ rule. |
| DATE_REFRESH | Legacy table-build time. `source_imported_at` is source import time, not the same measure. Warehouse metadata supplies fact refresh time. |

## Completeness and time checks

The available actions run from 1 June 2015 to 7 September 2026. Latest source
import is 8 September. September is partial; August is the last complete
calendar month in this extract. Recent monthly median import delay is 1-2 days
and p95 is 3-4 days, measured in calendar-date boundaries, not an SLA.

The monthly/provider analysis returns whole-month totals and provider-month
groups with at least 1,000 actions. Missing service or patient details must be
assessed within workflow: all 14,247,354 creation actions have no service or
provider, which is expected at that step. Search specialty cannot silently
replace service specialty.

| 2026 supplied field | Populated actions / 10,025,649 | Coverage |
|---|---|---|
| Age | 10,023,532 | 99.979% |
| Sex | 10,023,539 | 99.979% |
| Residence LSOA | 10,022,625 | 99.970% |
| Registered practice | 10,016,293 | 99.907% |
| Referrer commissioner | 10,025,599 | 99.9995% |

Across all years, no supplied age falls outside 0-120. That is a plausibility
check, not proof of its calculation date. All supplied sex codes resolve.
IMD 2019 has no exact match for 6,910 actions with a supplied LSOA; retain null
rather than invent a geography-vintage conversion. Eight actions lack a shared
practice lookup entry; supplied practice labels remain available as fallback.

Material limitations remain:

- June 2021 has 334,717 service-bearing actions across 5,171 services without
  recorded provider and service-specialty context. The whole-history count
  without provider is 342,155. Other observed actions have only one known
  provider and specialty for 307,235 of these rows, but 34,708 have changing
  context. This does not establish a valid historical backfill. The inspected
  Dictionary service table has one row per service, not version history; no
  EBSX05 service-history table was found in the DATA_LAKE ERS schema. A matched
  historical service extract is needed to resolve this safely.
- Patient-key gaps are concentrated in older periods, including 27,109 actions
  in October 2025 and 20,244 in December 2025. Counts retain them. Person-linked
  analysis must report its denominator. All 2026 actions have a patient key.
- 1,148,741 actions precede their supplied pathway start. Do not calculate every
  action's waiting time by subtracting that field. No actions have an import
  date before their action date in this profile.
- 1,007 actions lack an action code; 127 service-bearing actions lack a service
  label. Neither is dropped. All populated action and reason codes have labels.
- The test-patient flag and due date are entirely absent. The NHS RAS exclusions
  cannot be reproduced fully by treating a missing test flag as false.
- Appointment timestamps contain source anomalies. There are 133 slots more
  than two years beyond the profile date, with a maximum year of 9202. This is
  a diagnostic threshold, not a maximum permitted booking horizon. Keep an
  explicit reporting period and do not infer a corrected date from its digits.
  The service directory also has 143 future effective starts, including a date
  in 2922. Its dates cannot establish a reliable historical backfill on their own.

## Worked analyses

The runnable examples in `analyses/ers` deliberately use different denominators:

- `ers_example_referrals_created`: first 1422 per UBRN, grouped by creation month.
  Full history contains 14,247,354 such requests.
- `ers_example_advice_response`: first 1407 per UBRN/service and first later 1408
  for that same non-null service, ordered by action ID. Searches full response
  history before grouping request months. There are 493,850 groups, including
  323 without service; 466,967 have a later matching response. Missing responses
  are not necessarily open worklists. It is a first-response measure, not every
  dialogue turn or a clinical outcome.
- `ers_example_triage_activity`: keeps 1608 requests, 1770 RAS outcomes, 1810
  booking-deferral outcomes and 1836 booking-review outcomes separate. The last
  three share a display label. NHS guidance supports the RAS and ASI meanings;
  the booking-review distinction comes from the retained Dictionary meaning.
- `ers_example_appointment_slots`: scheduled slots grouped by latest recorded
  appointment action. Repeated booking actions do not become extra slots.
- `ers_example_referrals_received`: one request/provider receipt under the local
  legacy action definition, with recorded-context coverage and financial periods.

Example output groups require at least 100 underlying rows. This is a practical
limit on diagnostic detail, not a declaration of an NHS disclosure policy.
Consequently sums of displayed groups may be lower than whole-table totals.
Distinct request counts across providers, months or codes are not additive.
These examples do not calculate RTT compliance, attendance rates or current
waiting-list size.

## Specification evidence

[NHS EBSX descriptions](https://digital.nhs.uk/services/e-referral-service/reports-and-statistics/ebsx-reports)
establish the action extract and separate code, organisation and service
lookups. The page's current full help link returned HTTP 403 during this review.
The field mapping therefore distinguishes directly supplied warehouse fields,
reference labels and local derivations. It is not certification against an
unavailable complete current column specification.

[NHS decoding guidance](https://digital.nhs.uk/services/e-referral-service/document-library/extracts-translating)
supports using EBSX03/04/05 and matching extract periods. Current lookup names
do not establish historical service ownership.

[NHS RAS guidance](https://digital.nhs.uk/services/e-referral-service/document-library/ras-interpreting-data)
supports action-ID sequencing, separate organisation roles, search/service
specialty distinctions and RAS action/reason interpretation.
[NHS ASI guidance](https://digital.nhs.uk/services/e-referral-service/document-library/appointment-slot-issue-data-extract---nhs-e-referral-services)
identifies booking-deferral outcomes and explains why current worklists cannot
be reconstructed exactly from simple action counts.

The SQL and YAML beside each model define every output column. The companion
[field mapping](ers-field-mapping.md) records their selected expressions and distinguishes supplied
fields from derived outputs. The live field profiles cover every published
column, with only aggregate completeness returned.

## Validation of this change

The tracked DEV build passed eight models and 20 tests, including the downstream
appointment and link models. All five e-RS outputs have the same row counts and
whole-table fingerprints as production for their pre-existing columns. The
wider action table built in 113 seconds and the request table in 38 seconds on
the tracked Medium warehouse. No database, schema, schedule or project
configuration was added for this work.

All 61 action columns and 51 request columns have at least one populated value.
Full-source joins found zero differences in the added age, sex code, residence
LSOA, registered practice and referrer commissioner code. Financial dates cover
every action. The unchanged appointment and link columns retain their earlier
field-profile evidence; the mapping now covers 149 output columns in total.

Synthetic data exercised the actual compiled advice and creation examples:
repeated requests, an earlier response ID, later-month replies, wrong-service
replies, null services and reversed timestamps. The advice result had 400
request/service groups, 200 matched responses, 100 reversed timestamps and a
two-day median excluding those reversed intervals. Repeated creation actions
produced 100 requests rather than 200 actions. Live receipt examples returned
4,685 provider/financial-month groups containing 11,302,251 receipts after the
100-row output threshold.
The receipt fixture also passed equal-timestamp tie-breaking, multiple providers,
missing providers and a first receipt before the reporting start date.

The review pass distinguishes SQL nulls, empty/whitespace strings and their
combined missing count in every field profile; `populated` remains the non-null
count. All 149 fields passed a synthetic four-row check with a null, an empty
string, whitespace and a populated value. The creation example selects the
earliest timestamp with action ID as its tie-breaker, including when ID and
timestamp order differ. Advice response sequencing still follows action IDs.
Geography joins now use the same trimmed codes exposed to analysts. No existing
actions have padded geography codes, so this correction preserves current data.
The follow-up DEV build passed all five e-RS models and 14 tests. Geography-code
uniqueness is enforced in reference, where the latest definition is selected;
staging retains supplied definitions without asserting one row per code.

Eddie Davison owns the e-RS reporting models, their e-RS reference models and
the staging/reference additions in this change.
