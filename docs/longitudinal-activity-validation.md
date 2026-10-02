# Longitudinal activity validation

Validation used the established shared `dev` target on 11 September 2026.
Only non-identifying aggregates were returned. OLIDS incremental loading is in
dbt-OLIDS #302. Encounter context is prepared separately in dbt-OLIDS #303.
The initial person-lookup correction below rewrote the prepared OLIDS snapshots;
the later context validation also rebuilt the clinical snapshot from conformed data.

## Grain, time and increment behaviour

All source adapter grain tests pass. Both shared outputs pass their global ID
uniqueness and time-consistency tests. The shared clinical uniqueness check
took 5 minutes 2 seconds on M across 2,330,987,200 rows in the final build.
That build passed seven models and ten tests, including the OLIDS passthrough
grains and both shared time checks. The final SUS refresh also passed its grain test.

An eight-model repeat incremental build and ten tests passed in 3 minutes
14 seconds. Counts and whole-row aggregate fingerprints matched before and
after the run for every materialised adapter. Separate full-source queries,
compiled with `--full-refresh`, produced identical counts and fingerprints.
Those comparisons exercise the complete output, not just its selected key.

The synthetic Snowflake lifecycle check uses the repository delivery and
withdrawal macros. It passed new historical activity, per-type watermarks,
equal delivery timestamps, null receipts, changed milestone dates, removed
optional milestones, withdrawn records, repeat-run idempotence and monthly
reconciliation of a delivery behind the watermark.

Run the checks with:

```powershell
uv run scripts/checks/test_navigation_warehouse.py
uv run scripts/checks/navigation_incremental_fixture.py > logs/navigation-fixture.sql
snow sql -c dbt-admin --warehouse WH_NCL_ENGINEERING_XS -f logs/navigation-fixture.sql
```

The SQL fixture creates only session-temporary tables in the existing DEV
schema. Five warehouse-selection checks also pass. Actual Snowflake query
history confirms that initial MHSDS and CSDS adapter builds used L. Table
metadata confirms person-first clustering with the timestamp/date expression.

The revised coverage table aggregates prepared adapters directly. Its query plan
contains no sort operators, compared with two when it read the ordered analyst
views. A DEV build on M fell from 8 minutes 40 seconds to 15 seconds. A later
concurrent validation run took 52 seconds; its grain test passed. Existing
source references supply the clinical labels; no terminology service runs at
query time. New model descriptions and owner checks pass.

The shared reporting models were moved to `DEV__REPORTING.CROSS_SYSTEM` with
Snowflake renames. The event view now includes a final person/date/time/ID sort
for direct lookups. A synthetic filtered lookup without an outer `ORDER BY`
interleaved source rows by date and time, excluded the other synthetic person,
put undated rows last. The final date-anchor convention places date-only midnight
anchors before timed rows on the same day, without claiming within-day sequence.
The compiled model passed. Applying the view definition preserved its existing
metadata and grants and did not rebuild any data tables. Outer queries still
need their own `ORDER BY` when result ordering must be guaranteed.

## Person lookup correction

An analyst XS clinical lookup took 49 seconds and scanned about 123 GB.
OLIDS stored a numeric `sk_patient_id` but clustered by its different
`person_id`. The staging text conversion and trimming prevented useful pruning.

Both prepared OLIDS snapshots were rewritten on L, preserving grants, column
metadata and all source values. `sk_patient_id` is now stored as text and leads
the clustering key. Staging retains the missing-key sentinel rule without
trimming the already canonical key. Counts and whole-row aggregate fingerprints
matched after converting the old numeric key to text for comparison:
179,290,676 events and 1,974,867,939 clinical records. Rewriting the prepared
snapshots took 31 seconds for events and 489 seconds for clinical records;
these timings exclude the separate fingerprint checks.

Re-planning the same lookup inside Snowflake, returning only scan statistics,
reduced the OLIDS branch from 6,569 assigned partitions to one, about 19 MB.
The complete query plan fell from 123.1 GB to 126.4 MB across 25 partitions.
These are planned scan sizes, not a measured post-change execution time.

Both shared views retain the upstream OLIDS UUIDs without text prefixes,
including event-to-clinical links. The clinical view now applies the same
person/date/time/ID presentation order as the event view. Its synthetic direct
lookup passed dated, timed, date-only, undated and other-person cases. Both
projects compiled and the five changed analytics views built successfully.
The corresponding source changes must merge before the next scheduled OLIDS
build to retain the corrected storage layout.

## Population reconciliation

These are source records, not distinct clinical occurrences or people.

| Source | Event rows | Clinical rows |
|---|---:|---:|
| OLIDS | 179,290,676 | 1,974,867,939 |
| MHSDS | 28,031,608 | 26,278,485 |
| CSDS | 87,073,985 | 37,606,281 |
| SUS APC | 24,812,477 | 138,372,321 |
| SUS OP | 112,525,012 | 57,788,928 |
| ECDS | 71,235,661 | 96,073,246 |
| e-RS | 81,106,443 | Not a clinical-record source |

The final counts total 584,075,862 events and 2,330,987,200 clinical records.
ECDS grew by 42,438 source clinical rows during validation; its grain test passes.

The acute profiles found nine replayed APC procedure rows with identical
source position, code and date. Staging now removes those exact replays and
enforces its existing grain without the former error threshold. Conflicting
codes or dates at one position remain visible and fail the grain test.

SUS procedure dates were present upstream but omitted from staging. They are
now exposed and retained in clinical records. Acute diagnoses without supplied
dates remain undated. The event adapter uses the general outpatient appointment
model so DNA and other non-attended slots are not lost through an attendance-only
encounter population.

## Known coverage limits

### SNOMED mapping validation, 11 September 2026

The new `snomed_concept` reference contains 1,151,519 recognised concepts,
including 312,564 inactive concepts. Every concept has a retained preferred
term. Historical source concepts keep their original code, without replacement
by an active successor.

The initial mapping-validation snapshot, before the later ECDS delivery, was:

| Adapter | Rows | Populated mapped codes | Historical source SNOMED records |
|---|---:|---:|---:|
| CSDS | 37,606,281 | 15,445,981 | 330,385 |
| MHSDS | 26,278,485 | 23,922,929 | 891,570 |
| ECDS | 96,030,808 | 93,431,141 | 1,320,395 |

The full refreshes passed all six grain and mapping tests. Aggregate
fingerprints prove identical row counts and values for every unchanged field.
Only mapped code, mapped label and mapped system changed, together with ECDS
source labels. No recognised source SNOMED code was replaced or left unmapped;
all mapped targets were recognised and had the latest retained label at build
time. `analyses/navigation/clinical_mapping_coverage.sql` reproduces those
aggregate checks without sorting the shared clinical timeline.

This adds direct mappings for 129,489,791 previously unmapped source SNOMED
records and 18 MHSDS Read records. It withholds 38,500 existing CSDS and 23,316
MHSDS target codes absent from both available SNOMED dictionaries and the
history table. Their source rows, source codes and detailed facts remain.

OLIDS retains its prepared EMIS mappings. Of its 1,974,867,939 clinical rows,
1,619,598 have targets unrecognised in those references and 3,048 have no map.
The available EMIS reference supplied no supported replacement for these
exceptions. Unrecognised does not establish invalidity. A further 23,480,922
OLIDS rows have recognised inactive targets, which remain valid historical
identifiers.

TRUD ingestion remains in `DATA_LAKE.TERMINOLOGY`. dbt maps the landing
resources through `scripts/sources`; only selected analytical references
belong in `REFERENCE.TERMINOLOGY`. No classification reverse map was used to
assign a clinical SNOMED code.

### Source coverage

`dq_person_activity_coverage` publishes missing patient keys, missing provider
codes, code-label coverage and date completeness by output and source type.
These are diagnostic counts, not automatic exclusion rules.

All populated OLIDS clinical codes have labels. ECDS has 149 clinical records
with a code but no label. SUS APC and OP have 180,457 and 142,193 respectively.
The adapters retain them and their original source position.

The separate source-reference change in #1174 reduced unlabelled CSDS codes
from 6,771,589 to 5,594,970, recovering 1,176,619 supported labels. Historical
CTV3 term identifiers can supply labels without an inferred SNOMED mapping.
Source counts and aggregate fingerprints of IDs, codes, dates, values and person
keys remained unchanged. Residual Read/CTV3 interpretation is tracked in #1175.
See [CSDS clinical validation](csds-clinical-validation.md).

MHSDS has 168,956 populated clinical codes without labels. The increase of 60
is limited to source placeholders that previously matched the legacy empty
Dictionary key labelled "Unknown". No non-empty legacy ICD-10 definition was lost.

MHSDS retains 8,781,231 clinical records whose stored timestamp has unknown
submitted clock precision. Their available day now has date precision and a
midnight sorting anchor. The source fact retains both stored and separately
supplied dates; 2,540 source inconsistencies remain explicitly flagged.
Another 10,783 MHSDS clinical records have no supported date. Date-only records
with validated dates remain on those days.

Both shared outputs pass the time-consistency checks: every retained date has
the documented timestamp anchor, genuine clock times are preserved, and missing
clinical dates remain null. The final coverage contains 21,828 undated events
and 142,265,593 undated clinical records. Parent and recording dates do not
replace a missing clinical date.

The clinical output now retains interpreted assessment scores, non-score status
and tool labels. All 251,118 numeric MHSDS non-score responses remain excluded
from `assessment_score_numeric`. Coding positions, the ECDS primary-diagnosis
indicator and SUS present-on-admission information retain their source meanings.

OLIDS #303 supplies separate dated encounter context for 49,838 otherwise undated
observations. Its full refresh passed the three grain/type tests and synthetic
checks for missing, deleted and different-person encounters. Whole-population
ID/date/recording-date fingerprints match the conformed candidate. The stable
key remains text, clustered by `sk_patient_id, clinical_record_date`.

The final shared adapters identify 1,355,833 conflicting OLIDS encounter links
and 1,199 conflicting APC episode links. None supplies a promoted parent ID or
parent context date. Source keys remain in the detailed models. Outpatient
person agreement is not labelled independently verified when its person key
comes from the same appointment relation.

Provider gaps include actions with no service context and OLIDS local
organisation coverage. Missing provider codes are not filled from a person's
current practice. Population and delivery windows differ across sources; see
the [analyst guide](longitudinal-activity.md).

## e-RS anomalous slot investigation

There are 133 slots more than two years beyond the profile date. This is a
diagnostic threshold, not a specification booking limit. Of these, 89 have
years of 2100 or later. All 133 were already over two years ahead at their
first observed action.

The investigation checked the full action histories, explicit rebooking links,
other slots for the same referral/service, raw extract versions and recorded
SUS booking-reference links. No raw version provides a different timestamp for
the same action. The single explicit rebooking link does not resolve to a
near-term slot. Ten suspect slots have another near-term slot on the same
referral/service; four match month, day and clock time and appear later.

Fifteen suspect slots have a SUS outpatient booking-reference match with the
same patient key. Eleven have a unique matching month/day. Seven also match
the exact clock time and provider. All seven candidate dates follow the first
e-RS action. None duplicates an existing e-RS slot or competes with another
suspect slot for the same candidate. Six original years are 2100 or later.

These seven are corroborated recovery candidates. Applying them would establish
a cross-source date-recovery rule; the original dates remain unchanged pending
the business owner's decision. The remaining records have no demonstrated
repair. No dates are corrected by rearranging their digits.
The outstanding source date-quality and recovery decisions are tracked in #1176.

`analyses/navigation/ers_appointment_date_recovery_profile.sql` reproduces the
strong-candidate aggregate checks without returning person-level records.
These counts do not establish a general maximum future booking date.
