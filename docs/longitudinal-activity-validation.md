# Longitudinal activity validation

Validation used the established shared `dev` target on 11 September 2026.
Only non-identifying aggregates were returned. OLIDS production outputs were
read and compiled, not rebuilt. Their incremental change is in dbt-OLIDS #302.

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
