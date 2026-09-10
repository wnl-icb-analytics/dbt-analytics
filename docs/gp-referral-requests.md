# GP referral requests

`REPORTING.OLIDS_REFERRALS.FCT_GP_REFERRAL_REQUEST` has one row per conformed
OLIDS referral ID. It follows the patient-referral definition prepared in
dbt-OLIDS from expanded observations using `<<3457005 |Patient referral (procedure)|`
and supported historical successor mappings. Non-referral content stays in
observations. No terminology server is queried when building or reading this fact.

Use `observation_id` to find the same clinical record in observations. Counting
both independently would double count it. Original referral IDs remain unchanged;
added observation referrals have namespaced IDs. The referral SNOMED code and its
latest preferred label explain inclusion. Recorded and mapped concepts remain
available separately.

`person_id` is consistent across OLIDS practice registrations. `patient_id` belongs
to the practice patient record. `sk_patient_id` is the shared pseudonymised NHS-number
key from the person dimension. A missing key does not remove the referral.

Requester, recipient, publisher and observation provider have different roles.
The observation provider is not an inferred referral destination. Current retained
organisation names may differ from historical names. Specialty is omitted because
the source does not populate it. Added observation referrals have no inferred
recipient, priority or direction.

## Recorded links and dates

Join encounters on both `encounter_id = id` and `person_id`. The profile found
473 recorded encounter links with a different person and five absent encounters.
The fact preserves source IDs and does not use them to import another person's
encounter details. A matching encounter is not proof of a consultation.

Booking references remain as supplied. A link to another system must check the
recorded reference and patient-key agreement. Similar people and dates do not
establish a referral pathway.

Interpret `clinical_effective_date` with its precision code and label. Partial
dates do not prove a specific day. Recorded time and source transformation time
are separate fields. Implausible supplied dates remain visible.

## Validation on 10 September 2026

The DEV fact has 22,133,989 rows and distinct IDs. It preserves every staging
referral and source value checked by `gp_referral_request_validation`, with no
missing, extra or changed rows. The two models and six tests passed.

- All referrals have an observation link, referral SNOMED code and label, and
  date-precision code and label.
- 22,131,110 have clinical dates; 30 precede 1900 and seven are future dates.
- 322,728 have recorded encounter links and 3,104,336 have booking references.
- 5,098,950 have recorded priority and type, all labelled.
- All recorded requester and observation-provider IDs have names. Of 5,098,950
  recipient IDs, 506 have missing or blank names; blank names are exposed as null.
- 14,131 referrals lack a patient key in the shared DEV person dimension. This
  depends on the dimension refresh and does not remove those referrals.
- All 49 fact columns were profiled for nulls and blanks. No column is entirely null.

The aggregate-only queries in `analyses/olids/gp_referral_request_*` can be rerun
without returning patient records or identifier values.
