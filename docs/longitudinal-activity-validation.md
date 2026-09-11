# Longitudinal activity validation

Validation used the established shared `dev` target on 11 September 2026.
Only non-identifying aggregates were returned. OLIDS incremental loading is in
dbt-OLIDS #302. The later person-lookup correction below rewrites the existing
prepared OLIDS snapshots without rerunning their upstream transformations.

## Grain, time and increment behaviour

All source adapter grain tests pass. Both shared outputs pass their global ID
uniqueness and time-consistency tests. The shared clinical uniqueness check
took 2 minutes 17 seconds on M across about 2.33 billion rows.

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

The coverage table and its grain test passed in 35 seconds on M. Existing
source references supply the clinical labels; no terminology service runs at
query time. New model descriptions and owner checks pass.

The shared reporting models were moved to `DEV__REPORTING.CROSS_SYSTEM` with
Snowflake renames. The event view now includes a final person/date/time/ID sort
for direct lookups. A synthetic filtered lookup without an outer `ORDER BY`
interleaved source rows by date and time, excluded the other synthetic person,
placed date-only rows after timed rows on their day and put undated rows last.
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
| ECDS | 71,235,661 | 96,030,808 |
| e-RS | 81,106,443 | Not a clinical-record source |

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

TRUD resources and the weekly loader now live in `REFERENCE.TERMINOLOGY`.
All ten moved tables matched their previous counts and all-column fingerprints.
Compatibility views preserve existing read paths; all 16 dependent views
compiled. The loader skipped an already-loaded release. No classification
reverse map was used to assign a clinical SNOMED code.

### Source coverage

`dq_person_activity_coverage` publishes missing patient keys, missing provider
codes, code-label coverage and date completeness by output and source type.
These are diagnostic counts, not automatic exclusion rules.

All populated OLIDS clinical codes have labels. ECDS has 149 clinical records
with a code but no label. SUS APC and OP have 262,549 and 154,023 respectively.
The adapters retain them and their original source position.

CSDS has 6,771,589 populated clinical codes without a supported label. This
matches the previously validated source fact, including its known CTV3 tokens
and numeric Read-v2 immunisation anomalies. The shared adapter does not replace
them with Dictionary placeholders or unsupported term-only mappings. See
[CSDS clinical validation](csds-clinical-validation.md). MHSDS has 168,896
populated clinical codes without labels.

MHSDS retains 8,781,231 clinical records whose stored timestamp has unknown
submitted precision. Their shared timestamp remains null and their precision
is unknown. The source fact retains both stored and separately supplied dates.
Another 10,783 MHSDS clinical records have no supported date. Date-only records
with validated dates remain on those days.

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

`analyses/navigation/ers_appointment_date_recovery_profile.sql` reproduces the
strong-candidate aggregate checks without returning person-level records.
These counts do not establish a general maximum future booking date.
