# GP appointments, healthcare events and clinical records

`fct_gp_appointment` contains one current patient-associated appointment from
the filtered NCL OLIDS patient spine. It retains all current statuses, future
appointments, administrative contexts and the 605 patient-associated blocked
slots in the profiled snapshot. Analysts can select the population they need.
Existing access and costing models retain their narrower clinical population.

`fct_gp_appointment_booking` contains one current recorded booking per appointment
with a supplied `booked_at`. dbt-OLIDS selects these rows in conformed and
publishes `APPOINTMENT_BOOKING`. The fact consumes that prepared output, with
booking-method labels and the current appointment's provider and publisher.
Those organisations do not establish who made the booking. Use `booked_at` to
date booking activity and join `appointment_id` for the appointment details.
The view represents the booking still recorded on the current appointment,
not all historical bookings, rebookings or cancellations. A source update can
change or remove an earlier booking row. No separate booking ID is invented.

`booked_at` and `scheduled_at` describe different times on the same current
record. Neither proves attendance. Source status names preserve distinctions
such as telephone completion and did not attend, alongside upstream mapped
codes and labels. A source Slot Available status can occur on a past slot.
The source's separate derived status can disagree with its recorded status and
is not used to infer an attendance history.

Supplied durations remain separate from consultation cost estimates. Untimed
schedules can carry a whole session duration; no ten-minute default or cap is
applied here. `patient_left_at` is appointment detail, not a discharge event.
Slot reassignment prevents reconstruction of a complete cancellation history.

`fct_gp_appointment_clinical_record` contains one appointment, clinical record
type and clinical record ID with a recorded encounter path for the same person.
Join `clinical_record_id` to `fct_gp_clinical_record.clinical_record_id`.
Use `source_record_id` to join the observation, medication order or medication
statement staging table named by `clinical_record_type`. Expanded observations already contain allergies
and referrals; adding their source tables again would duplicate those records.
The relation does not assert attendance or that clinical records occurred at
the appointment time. Unlinked records remain in the clinical detail tables.

`person_id` is consistent across practice registrations. `patient_id` identifies
a patient at a practice and can differ between linked records for the same
person. The relation carries the appointment's patient ID. `sk_patient_id` is
the NHS number hash produced with a salt and pepper. The appointment, booking and direct-link facts reuse `dim_person_pseudo`
for the existing representative person-to-key mapping. The new event and clinical
outputs use the hash approved upstream for the exact patient and person pair.
Both retain rows without an approved hash. Independently refreshed mappings can
differ; use `person_id` for longitudinal OLIDS history.

## Event and clinical outputs

`fct_gp_healthcare_event` reads the person-clustered upstream snapshot of current
bookings, appointment slots and coded referrals. A slot does not prove attendance.
Referral inclusion uses `<<3457005 |Patient referral (procedure)|` across expanded
observations, with supported unambiguous historical successors. The reference
lives in Snowflake, with current preferred labels for current and historical
codes. No build or query calls a terminology server.

`fct_gp_clinical_record` reads a separate person-clustered snapshot containing
expanded observations, medication orders and medication statements. Codes define
clinical meaning; the source record type identifies the detail table. Medications
remain separate from observations, and quantities are not observation results.
Neither test requests nor procedure requests enter these outputs.

Clinical dates keep their source precision. Referral `event_at` is null because
the source supplies dates, not timestamps. Partial, missing, historical and future
clinical dates remain available. The clinical output retains numeric results,
result dates and source unit codes and labels. Globally empty text-result and
mapped-unit columns are omitted.

## Processing and release

The large joins, union, code classification and person/date clustering belong in
dbt-OLIDS conformed and stable. All five analytics facts are views. The normal
upstream deployment publishes the new booking, event, clinical and relationship
objects to `DATA_LAKE.OLIDS`.

The OLIDS schedule starts ahead of analytics but does not guarantee completion.
Publish all four upstream objects before merging this companion. PR #1123 does
not create them and is not a dependency. The referral staging interface exposes the upstream observation link,
classification labels and date precision. No programme or shared observation
macro changes are included. Both PRs remain drafts pending actual stable writes,
clustering performance, publication and downstream DEV builds. The full clinical
snapshot exceeds two billion rows. The required staging and reporting grain
tests repeat uniqueness scans; the exact analytics daily selection also needs
performance validation. Thin views alone do not establish a cheap daily run.

## Validation on 9 September 2026

The appointment fact built in DEV with 78,821,602 rows and unique, non-null
appointment IDs. All five facts and their new raw and staging interfaces compile.
The repository checks for model descriptions, tests and reference boundaries
pass. Lineage shows no existing programme downstream of the new facts. The corrected
existing referral interface reaches Valproate, whose separate issue #1126 covers
its observation/referral duplication.

The earlier booking view over the appointment fact built with 78,207,418 unique
appointment IDs and populated
booking timestamps. Its count and aggregate hash across all 17 columns match
the appointment rows with a booking timestamp. Every booking column is
populated throughout except `sk_patient_id`, which is missing on 5,475 rows.
There are no non-null blank values. All three booking tests pass. The
aggregate-only checks are in `scripts/snowflake/profile_gp_appointment_booking.sql`.
The refactored upstream booking selection has the same 78,207,418 rows; repeat
the downstream build and reconciliation after publication.

All 44 appointment columns were profiled for non-null and blank values.
There are no all-null columns or non-null blank values.

| Fields | Populated rows |
|---|---:|
| Appointment, person and patient IDs; scheduled time; planned duration; all status, booking and contact code-label pairs; blocked flag; local slot type; schedule ID; provider and publisher IDs, codes and names; provider code authority | 78,821,602 |
| Person hash key | 78,816,036 |
| Age at event | 78,821,571 |
| Booking time | 78,207,418 |
| Schedule type | 78,660,352 |
| Principal practitioner role ID | 77,715,757 |
| Principal practitioner ID, role code and role name | 77,527,303 |
| National slot category name and description, context and service setting | 75,263,744 |
| Sent-in time, waiting minutes and delay minutes | 45,364,021 |
| Patient-left time | 44,785,314 |
| Actual duration | 44,784,551 |

All populated practitioner role codes have labels. The missing 5,566 person
hash keys in DEV compare with 4,430 using the current production person mapping;
1,331 appointment rows have different DEV and production mapping results.
These counts depend on the independently refreshed person dimension.

Booking is later than the scheduled timestamp on 9,054,197 records, including
147,563 booked on a later calendar day. The fact preserves these supplied
timestamps, so waiting-time calculations need an explicit rule for retrospective
entry. There are 1,857,682 past slots carrying mapped status 0. No scheduled
dates precede 1900 and no supplied durations are negative in this snapshot.

Upstream read-only validation produced 71,780,736 unique clinical links with
all six upstream columns populated. It preserved 71 links across different
practice-level patient IDs for the same person. The three branches reconcile
to 56,163,795 observations, 11,844,151 medication orders and 3,772,790 medication
statements. The expanded observation input was simulated from PR #298's already
conformed sources before its scheduled publication.

The new event candidate has 78,207,418 bookings, 78,821,602 slots and 22,125,300
coded referrals, each with unique event IDs within type. Type-specific namespaces
separate their keys. Of 23,634,902 referral-request records, 1,871,444 do not meet
the ECL; native observations contain another 361,842 qualifying referrals.
All referral codes have labels, and 2,879 referrals have no clinical date.

The clinical candidate reconciles to 1,588,949,549 expanded observations,
384,524,265 medication orders and 98,440,569 medication statements. Every row
has person and patient IDs, source code and label, source date-precision code
and label, record-entry time and publisher details. No retained output column
is empty throughout. Upstream profiles found no join multiplication.

The upstream PR contains the complete field profile and reference checks.
No immediate production build was triggered.

## Corrected referral interface

`stg_olids_referral_request` consumes the terminology-defined referral table
prepared in conformed. Existing qualifying referral IDs and all original fields
are preserved. Added observation referrals use namespaced IDs; `observation_id`
links both populations to expanded observations. All original clinical content,
including the 1,871,444 source records excluded as non-referrals, remains in
observations. Added rows have no inferred destination, direction or priority.

Some medication `referral_request_id` values now point to excluded source
records: 35,343 orders and 8,714 statements in this snapshot. These supplied
identifiers remain unchanged and the original content remains in observation
provenance. Existing analytics has no joins using those medication IDs.

Valproate's existing duplicate read remains separate in #1126. All 519 ARAF
referral rows for 284 people remain original referrals; this change adds no
further ARAF duplicate rows in the current profile. Programme code is unchanged.
