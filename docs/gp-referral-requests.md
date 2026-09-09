# GP referral requests

`fct_gp_referral_request` contains one row per retained OLIDS referral request.
`source_record_id` is the original `referral_request.id`, used for direct joins.
The upstream stable feed restricts referrals to NCL publishing practices and
patients in its filtered patient spine. It retains the latest version per
referral ID, ordered by source transform time and then recorded time. Staging
excludes deleted records and records without `person_id`. The fact retains every
staging referral and adds no current-registration, referral-direction or date
restriction. It does not contain every historical version of each referral.

The landing referral record supplies concept identifiers. In `dbt-olids`,
`conformed_referral_request` joins `int_enriched_concept_map` to resolve codes
and displays for the recorded referral, priority, specialty, type and date
precision concepts. These are upstream reference values, not historical
snapshots of text on the referral. The fact preserves them as supplied by the
stable feed.

Mapped referral code, display and coding system remain separate. The upstream
enriched map can replace retired SNOMED targets, repair root targets and backfill
missing EMIS mappings. Those results are already present in the stable feed;
this fact applies no additional mapping or label replacement. Shared terminology
processing remains in `dbt-olids`.

Requester, recipient and publisher identify different organisation roles. Their
names and codes come from the current retained OLIDS organisation lookup. They
are not historical organisation attributes. The publisher code recorded on the
referral remains separate so discrepancies can be investigated.

`sk_patient_id` comes from the established person pseudonym dimension. Referrals
without a matching key remain in the fact. Encounter IDs and booking references
are recorded links. Neither proves that a consultation occurred or caused later
activity in another system. Clinical date precision remains as supplied; this
model does not infer timestamp precision or repair implausible dates.

## Coverage checked on 8 September 2026

The DEV build retained all 23,590,546 staged referrals, with no missing or extra
keys and no differences in the source fields published by the fact. The staging
extension left its original column values and row count unchanged.

- 23,588,144 referrals have a cross-system patient key; 2,402 remain unlinked.
- 3,259,744 have a non-blank booking reference. No cross-source match is inferred.
- No referral currently supplies an encounter ID or specialty concept ID.
- 23,587,061 have a clinical effective date.
- All referrals have source referral and date-precision displays. All 5,386,757
  recorded priority and type concepts have source displays.
- All referrals with recorded referring or publishing organisation IDs have
  names. Of 5,386,757 referrals with a receiving organisation ID, 512 have no
  retained organisation name.

These are source and lookup coverage limits. They do not justify dropping records
or inventing links. Counts change as the underlying sources refresh.

The reproducible aggregate check is
[`gp_referral_request_validation.sql`](../analyses/olids/gp_referral_request_validation.sql).
It returns counts only, including source reconciliation, recorded encounter
coverage, encounter person-key disagreements, booking references and labels.
Run it through `dbt show --target dev -s gp_referral_request_validation --output json`.
The fact's permanent tests check a non-null unique referral key and equal row count
with its staging input.


## Review on 9 September 2026

A fresh DEV build passed both models and all six tests. The fact now reconciles
to all 23,634,902 current staging rows, with no missing or extra keys and no
changed source fields. The earlier DEV table was older than the refreshed
source; rebuilding removed those comparison differences.

All 46 published fields were profiled. Encounter ID and the three specialty
fields remain entirely empty. Referral mode contains six text categories, not
unlabelled numeric codes. The receiving-organisation lookup has 512 absent names
and seven blank names. There are 409,232 referrals with unknown direction,
31 clinical dates before 1900 and 14 after the validation date. Source values are
retained; these dates need explicit treatment before longitudinal sequencing.

The refreshed fact has 13,103 records without a DEV canonical patient key.
A read-only comparison with the production person dimension leaves 2,471
without a key. This difference comes from the dimensions' different refresh
states, not from referrals being dropped by the fact. There are also 24
populated key differences between those dimension versions. The fact uses the
configured target's person dimension rather than hardcoding a production join.

The complete-field profile and date diagnostics are retained in
`analyses/olids/gp_referral_request_field_profile.sql` and
`analyses/olids/gp_referral_request_quality_profile.sql`. The empty columns and
blank names remain review findings on the draft analyst interface.
