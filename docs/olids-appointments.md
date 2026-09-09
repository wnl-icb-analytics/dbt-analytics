# GP appointments and recorded clinical links

`fct_gp_appointment` contains one current patient-associated appointment from
the filtered NCL OLIDS patient spine. It retains all current statuses, future
appointments, administrative contexts and the 605 patient-associated blocked
slots in the profiled snapshot. Analysts can select the population they need.
Existing access and costing models retain their narrower clinical population.

`fct_gp_appointment_booking` contains one current recorded booking per appointment
with a supplied `booked_at`. It is a narrow view over `fct_gp_appointment`, with
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
Join the ID to the observation, medication order or medication statement
staging table named by the type. Expanded observations already contain allergies
and referrals; adding their source tables again would duplicate those records.
The relation does not assert attendance or that clinical records occurred at
the appointment time. Unlinked records remain in the clinical detail tables.

`person_id` is consistent across practice registrations. `patient_id` identifies
a patient at a practice and can differ between linked records for the same
person. The relation carries the appointment's patient ID. `sk_patient_id` is
the NHS number hash produced with a salt and pepper. The facts reuse
`dim_person_pseudo` for the established person-to-key mapping and retain rows
when that mapping lacks a key.

## Processing and release

The large encounter and clinical joins belong in dbt-OLIDS conformed. Its stable
snapshot materialises the narrow relationship and the normal publication
exposes `DATA_LAKE.OLIDS.APPOINTMENT_CLINICAL_RECORD`. All three analytics facts are
views, which avoid another daily copy of the appointment and relationship
tables. Provider, publisher and principal practitioner labels come from the
existing staging interfaces. No terminology service is called at query time.

The OLIDS schedule starts ahead of analytics but does not guarantee completion
before it. The analytics companion must wait for the new upstream relationship
object to be built and published. PR #1123 does not create that object and is
not a dependency of these models. No programme or shared observation macro
changes are included.

## Validation on 9 September 2026

The appointment fact built in DEV with 78,821,602 rows and unique, non-null
appointment IDs. All three facts and the new raw and staging models compile.
The repository checks for model descriptions, tests and reference boundaries
pass. Lineage shows no existing programme downstream of the new models.

The booking fact built with 78,207,418 unique appointment IDs and populated
booking timestamps. Its count and aggregate hash across all 17 columns match
the appointment rows with a booking timestamp. Every booking column is
populated throughout except `sk_patient_id`, which is missing on 5,475 rows.
There are no non-null blank values. All three booking tests pass. The
aggregate-only checks are in `scripts/snowflake/profile_gp_appointment_booking.sql`.

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

The upstream stable build, data lake publication and analytics relationship
build remain release checks. No immediate production build was triggered.
