# e-RS appointments and recorded acute links

`REPORTING.ERS.FCT_ERS_APPOINTMENT` has one row per UBRN identifier, service and
recorded scheduled timestamp. Repeated bookings, rebookings, cancellations and
administrative updates for that same slot remain one appointment. A changed
service or scheduled time creates another planned appointment.

Actions without a scheduled time remain in `FCT_ERS_REFERRAL_ACTION`. They are
not assigned an invented appointment. A null service forms its own group when
a timestamp exists. Appointment IDs use an explicit timestamp format, so a
session's display settings do not change the key.

`REL_ERS_APPOINTMENT_ACTION` links every contributing action ID to its appointment.
This keeps the full history accessible without repeating appointments or packing
the action history into a list field.

## Recorded appointment state

`latest_appointment_action_*` identifies the highest sequential action ID among
booking, cancellation, cancellation-not-confirmed, displacement, rebooking,
rebooking instructions, acceptance, rejection and DNA actions. The codes are
1412, 1413, 1414, 1416, 1417, 1419, 1420, 1424, 1431, 1432, 1433, 1435 and 1533.
Labels come from the e-RS reference dictionary. Later printing, letter updates
and other administrative actions do not replace this recorded appointment action.

This is deliberately the recorded action, not an inferred attendance or a
simplified open/closed status. Referral acceptance does not prove attendance.
DNA resolved and cancellation not confirmed remain distinct labels. Appointments
described only by other actions have no appointment-state action populated.

Service and organisation details come together from the latest contributing
action. Different provider codes for the same service and time are not additional
appointments. `recorded_booking_action_count` exposes repeated booking actions.

The source's `rebooked_to_action_id` remains in the action fact. It usually points
to a booking action describing the same slot as the rebooking action. It is not
used to invent an earlier slot or a cancellation. The source has 37 such references
without a matching action in the available active extract; those actions remain.

## Acute links

`REL_ERS_REFERRAL_ACUTE_RECORD` has one row per matched e-RS request, acute source
and acute source record ID. It joins recorded UBRNs after removing display hyphens
and surrounding whitespace. Both original source references remain accessible.
It applies no attendance or discharge restriction and no matching by person/date.

SUS staging now exposes its supplied UBRN and issuer. Existing encounter facts
do not contain these fields, so the relation uses the established staging grains.
SUS outpatient `PRIMARYKEY_ID` and admitted-patient spell `PRIMARYKEY_ID` remain
distinct through `acute_source`. ECDS is excluded because its UBRNs are unpopulated.

The relation exposes both patient keys and `patient_key_agreement`:

- `agree`: both keys are present and equal.
- `mismatch`: both keys are present but differ.
- `missing_key`: at least one key is absent.

For person-level analysis, require `patient_key_agreement = 'agree'`. Even this
does not prove that the referral caused the acute activity. The other states
remain available for source-data investigation and must not be silently accepted
as person-level links.

## Validation on 10 September 2026

Five models and 15 tests passed in DEV. The appointment write took 26 seconds
and the acute relation eight seconds on the tracked Medium warehouse. Additional
normalised-UBRN uniqueness and non-null tests passed.

- 12,675,218 appointments with unique keys.
- All 39,579,692 timestamped actions are represented once in the relation and
  reconcile to the sum of appointment action counts.
- All 37 output columns across the three models were profiled; none is entirely
  empty. All populated provider and site codes have labels. There are 101
  appointments without a service name, 1,933 without a patient key and 11,035
  without a recorded appointment-state action.
- Synthetic checks using compiled SQL cover repeated booking actions, later
  cancellation, administrative updates, changed slots, missing services,
  timestamp-free actions, and agreement/mismatch/missing-key links. All passed.
  Appointment IDs were unchanged after changing timestamp display settings.

| Acute source | Agree | Mismatch | Missing key |
|---|---:|---:|---:|
| SUS outpatient | 18,956,877 | 86,973 | 93,537 |
| SUS admitted-patient spell | 646,593 | 283 | 15,065 |

The 87,256 conflicting links remain conflicting after numeric formatting checks.
They are not repaired by stripping leading zeros or coercing patient keys.
They affect 74,319 outpatient UBRNs and 215 admitted-patient UBRNs.

Only the supplied UBRN and issuer are added to SUS staging. Its selection rules
and existing columns are unchanged. Downstream programme SQL is not modified.
These e-RS outputs share the explicit `large_periodic` refresh of the parent
facts; this change does not add a daily full rebuild or create a schedule.

The repeatable queries in `analyses/ers/` return aggregate coverage and
reconciliation. Read [the referral and action guidance](ers-referrals.md) for
source definitions, label provenance and legacy differences.

## Review checks

The review check found 17,742 requests with both missing and populated action
patient keys, and no requests with conflicting non-null keys. Missing values on
some actions do not invalidate a consistent request-level key. Acute agreement
compares that request key with the acute record key; it does not assert that
every contributing action contains a key.

All 254,832 e-RS organisation dictionary codes were already trimmed and uppercase.
Neither that dictionary nor the 447,926-row combined organisation reference had
duplicate keys after case and whitespace normalisation.

The action-reason provenance and appointment action descriptions were corrected
after review. Both models rebuilt in DEV and their four grain tests passed.
