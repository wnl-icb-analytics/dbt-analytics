# CSDS referrals and contacts

The CSDS facts retain the latest reported referral or referral/contact pair
across all accepted reporting months. They provide source records for the
shared event work in #1006. They do not infer a care journey or apply a programme
population. #854 covers this part of the domain.

## Source authority

The [CSDS tools and guidance](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/community-services-data-set/implementing-the-community-services-data-set-csds-v1.6-tools-and-guidance)
provide the [v1.6.10 ETOS](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_etos_v1.6.10_final.xlsx)
and [user guidance v1.9](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/community-services/csds_v1.6_user_guidance_v1.9.pdf).
The ETOS defines provider-qualified identifiers and successor rules. Source
profiles below check those rules against the received data, including versions
1.0 and 1.5. UKHFD supplies national code descriptions and definition history.

## Models and grains

| Model | One row represents |
|---|---|
| `stg_csds_activesubmission` | One accepted submission, enriched with its header and dataset version. |
| `stg_csds_referral_history` | One referral in an accepted submission. |
| `stg_csds_care_contact_history` | One contact in an accepted submission. |
| `stg_csds_cyp101referral` | Latest reported state of a provider-qualified referral identifier. |
| `stg_csds_cyp201carecontact` | Latest reported state of a referral/contact identifier pair. |
| `fct_csds_referral` | The latest referral, with source provenance, its own patient link and national labels. |
| `fct_csds_care_contact` | The latest referral/contact pair, with source provenance, its own patient link and national labels. |
| `csds_referral_code_lookup` | Newest definition of a referral reason, priority or professional-group code, including retired codes. |
| `csds_referral_code_lookup_history` | One UKHFD definition revision for a code. |

The existing patient summaries and currency model have narrower purposes. They
cannot supply complete event records or same-submission child links. The history
and latest models form one staging interface for each source table. Only the
history reads raw rows; it first keeps submissions listed in ActiveSubmission.
The person bridge is an independent identity feed.

Latest selection orders by reporting-period end, file receipt time, submission
identifier and submitted-row identifier, descending with missing dates last.
The last two fields make ties deterministic. `record_number` identifies a
patient within a submission; it is not source row order and is not used to
choose a later correction.

Referral identifiers occasionally move between people in source history. The
latest fact retains the newest reported state, not a complete lifetime history
of each person who previously carried that identifier. Contact identifiers are
also reused across referrals. Contact keys therefore include the referral;
activity links must use the accepted contact history and submission identifier.

## Analyst-facing rules

Dates and times remain those supplied by CSDS. Date-only timestamps use midnight
and carry `date` precision; supplied times carry `timestamp` precision. Receipt
time and reporting months remain separate provenance fields. No arbitrary early
date threshold is imposed. Age is source-derived at the referral/contact date,
including source special values.

Both facts use their own person identifier to obtain the cross-system patient
key. Missing links remain in the population. The contact's recorded referral
key is retained with separate existence and person-agreement flags. A matching
referral key does not establish that both records describe the same person.
Current referral details are not copied onto a conflicting contact.

`attendance_code` uses the original submitted attended-or-did-not-attend field.
The later `attendance_status` field is retained separately as
`source_attendance_status_code`. Where both are populated they agree after
zero-padding is removed. The later field is absent throughout version 1.0 and
mostly absent in 1.5. The new fact retains unknown, cancelled and did-not-attend
records. Existing currency and encounter attendance rules are unchanged.

Submitted consultation mechanism and older consultation medium remain separate.
No cross-version mapping is guessed. National labels use the newest definition,
including retired codes, rather than implying a historically effective label.
Unmatched codes remain visible with a null description. Shared national contact
code definitions reuse the existing MHSDS-named reference interface; they are
not MHSDS-specific clinical rules.

Commissioner fields distinguish submitted commissioner from source-derived ICB,
sub-ICB and derivation reason. The facts apply no current-footprint or GP-practice
fallback. Programme attribution belongs downstream.

## Validation on 8 September 2026

All warehouse outputs used for validation were non-identifying aggregates.
The accepted feed covered October 2017 through July 2026.

| Check | Result |
|---|---:|
| Accepted submissions, headers and provider-months | 15,116 each; no duplicate or missing header period |
| Accepted referral occurrences | 99,880,448 |
| Latest referral identifiers | 15,325,459 |
| Referral identifiers with different people across history | 31,284 |
| Accepted contact occurrences | 61,495,117 |
| Latest referral/contact pairs | 61,490,763 |
| Contact identifiers attached to multiple referrals | 830,644 |
| Referral/contact pairs with different people across history | 5 |
| Duplicate submission/referral keys | 0 |
| Duplicate submission/contact keys | 0 |
| Missing current referral links from contact fact | 0 |
| Contact links with a different current referral person | 48,947 |

The bridge contains 8,304,841 unique non-null person identifiers. New facts
preserve every selected staging row. Field population checks found no wholly
empty columns in the new facts. Older/current provider, referring staff-group
and discharge-letter aliases agreed wherever populated across dataset versions.

Compared with receipt-first selection, period-first selection changes 338,251
referral rows and 21 contact rows. It selects a different discharge date for
37,381 referrals. The source correction reaches existing consumers:

| Production versus rebuilt DEV | Change |
|---|---:|
| Referral summary rows | None: 4,131,614 |
| Referral count | None |
| Open referrals | -36,450 |
| Discharged referrals | +36,450 |
| Currency contact rows | None: 61,490,763 |
| Currency code | 214 contacts |
| Costed attendance eligibility | 5 contacts |
| Proxy cost | 197 contacts; net +£4,711.13 |
| Contact date | 21 contacts |
| Contact person or patient bridge | None |

These are comparisons with the available production tables, not a claim that
all reference inputs were frozen. The newer-period source records explain why
selection can change dates, discharge status, referral reason and currency.
The currency formula and attendance policy have not been edited.

Legacy `REPORTING.MAIN_DATA` facts start on 1 April 2021. The new referral fact
has 9,918,636 records from that date, exactly the legacy referral count, plus
5,406,823 earlier referrals. The contact fact has 39,567,181 records from that
date versus 39,567,175 in the legacy table, plus 21,923,582 earlier contacts.
Contact/person/date reconciliation matches 39,567,160 rows on each side. The
remaining 21 new and 15 legacy rows explain the six-row difference across the
cutoff after latest-period selection. All 9,918,636 legacy referral identifiers
match, with 16 changed person identifiers and 30,196 changed discharge dates;
referral start dates agree. Contact IDs alone are reused, so a join on that
identifier would multiply rows.

Permanent tests cover model grains, latest reporting-period selection, complete
fact populations including unknown attendance, and date/time precision. The
aggregate analyses in `analyses/community/csds_referral_contact_*` retain the
source, selection and output profiling queries for future deliveries. All three
profiles were compiled and executed against DEV. Singular tests return only the
keys and context needed for diagnosis inside Snowflake; their results must not
be exported into public logs or issues. Reconciliation checks individual keys
and attendance values, so balanced losses and additions cannot hide behind equal
row counts.

Targeted DEV builds passed for accepted histories, latest staging, references,
the two facts and existing currency, encounter and patient-summary models.
The downstream build also passed for monthly cost/activity, trajectories,
C-LTCS outputs and its membership snapshot: 8 models, 1 snapshot and 30 tests,
with the existing EPD prescribing-coverage warning. Compile configuration and
expired optional platform-credential warnings did not prevent Snowflake builds.

Builds use the tracked `dev` target and existing shared DEV layers. No task
schema or target override was added; `dbt_project.yml` is unchanged.

## Analyst interface and data profile, 9 September 2026

The facts keep their original populations and keys: 15,325,459 referrals and
61,490,763 contacts. Reporting identifiers now use `referral_id`, `contact_id`,
`local_referral_id`, `local_contact_id`, `team_id`, `local_team_id` and
`submission_id`. Staging retains the source field names. `source_record_id`
remains the standard key for the cross-source models. Contact `referral_id`
joins the referral fact's `source_record_id`; the duplicate parent-key alias
has been removed.

Organisation codes now have names beside them. The shared `organisation`
reference exposes 447,213 codes with their latest available names: 340,689 from
UKHFD ODS API, 89,500 Dictionary fallbacks and 17,024 further codes from the
UKHFD archive of organisations closed since 1991. The archive supplies only codes
absent from both other sources, preserving all existing selected names. The
reference retains closed organisations and historical codes. The reference also states
the name source and definition timestamp. These are current definitions of
historical codes, not names as they stood on the event date.

Every referral and contact provider is labelled. All source ICB and sub-ICB
codes match the reference. Some supplied codes still have no authoritative
match:

| Field | Records with a code but no name |
|---|---:|
| Referral referring organisation | 162,172 |
| Referral submitted commissioner | 92,008 |
| Contact submitted commissioner | 32,356 |
| Contact site | 12,207,871 |
| Source of referral | 9,928 |
| Primary referral reason | 17,563 |
| Referring staff group | 99,289 |
| Contact attendance | 111,640 |
| Contact subject | 44,577 |
| Group therapy | 11,406 |

The later `source_attendance_status_code` now has its own name column, with
109,098 populated codes unmatched. `group_therapy_code` and `group_therapy_name`
make clear that the source indicator is a category, not a derived boolean.
The source commissioner derivation reason remains without a verified label
mapping. Codes are preserved when a label is unavailable.

The audit checked all historical codes and latest definitions in the shared
categorical references. It found no lost codes, duplicate current keys, blank
labels or stale selections. Arbitrary code padding and case changes are not
used to turn unrecognised values into recognised categories.

No fact column is wholly empty and no non-null string is blank. Exploratory
counts include seven negative ages across the two facts, three referral ages
above 120, 325 contacts longer than 1,440 minutes, 50,755 discharge letters
dated before discharge and 65,015 contacts before the submitted earliest
clinically appropriate date. These observations do not establish exclusion
rules. The facts preserve supplied values for source-quality review. No
discharge precedes its referral date.

The new organisation dependency build passed five models and ten tests. The
two facts and seven tests passed, including exact source-key reconciliation,
time precision and a regression for organisation names, the later attendance
label and latest-referral link flags. The retained quality-profile analysis
was compiled and executed against DEV. Existing staging and currency rules
are unchanged by this profiling pass.

## Submitted referral context and identity changes

The contact fact exposes `referral_source_row_id` for its same-submission CYP101
parent, with `is_submitted_referral_linked` and
`is_submitted_referral_person_consistent`. The existing `is_referral_*` flags
continue to describe the latest referral fact. Missing parents or person
identifiers produce null person agreement, not false. The contact keeps its
own person and patient bridge.

Use the submitted row key when historical referral context is required. This
join reads the referral delivered with the contact:

```sql
select
    contact.source_record_id,
    contact.is_submitted_referral_person_consistent,
    submitted.referral_request_received_date as submitted_referral_received_date
from {{ ref('fct_csds_care_contact') }} as contact
left join {{ ref('stg_csds_referral_history') }} as submitted
    on contact.referral_source_row_id = submitted.cyp101_unique_id
    and contact.submission_id = submitted.unique_submission_id
    and contact.referral_id = submitted.unique_service_request_identifier
```

The September 2026 snapshot contains 48,947 contacts whose person differs from
the latest referral. Every one agrees with its same-submission referral and
occurs before the latest referral reporting period. Their earlier person does
not appear in that referral's latest period. Of these contacts, 23,699 have
different known cross-system patient keys across the two versions; 25,248 have
at least one missing bridge. None resolves to the same known patient key.
However, 30,173 retain the same provider-local patient identifier and 18,774
change it. These are mixed source identity histories, not grounds to merge
people or discard earlier contacts.

The same investigation found 16,414 service relationships with different latest
referral persons, all agreeing with their submitted parent. There were 5,739
different known patient keys and 10,675 comparisons with a missing bridge.
Across referral history, 31,284 referral identifiers have multiple Person_IDs;
1,221 return to an earlier Person_ID after another value. Qualifying all referral
keys by Person_ID would also split histories that may contain identity
corrections, so this change preserves the existing latest-referral grain and
provides the exact historical parent link.

The retained aggregate-only analysis
[`csds_referral_contact_identity_profile.sql`](../analyses/community/csds_referral_contact_identity_profile.sql)
rechecks submitted-person agreement, patient bridges, local identifiers and
reporting-period differences by dataset version. It returns counts only.

The production `MAIN_DATA` SQL was reviewed through Snowflake query history.
The 1 September 2026 contact build
(`01c6c782-0001-eb92-0001-51c21584368a`) joins patient and referral context by
referral identifier alone. The referral build
(`01c6c77d-0001-eb64-0001-51c21583f796`) does the same for patient context,
although its additional CYP101 age join includes the source row and Person_ID.
Its CYP102 helper selects the largest source row identifier per referral,
without a person or submission key. Those joins do not resolve historical
identity changes and are not copied into the new historical parent link.

## Interpreting age, duration and date checks

The [CSDS technical output specification](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/community-services-data-set/specifications-and-guidance)
v1.6.10 defines contact and activity duration in minutes, with a maximum
four-digit numeric format. It does not impose a 1,440-minute cap. The 325
contacts and 318 activities above that exploratory threshold all remain within
the source format. No automatic cap or unit conversion is justified by this
check.

The three referral ages above 120 are all between 121 and 129. The specification's
age-group derivation includes ages through 129 and returns null for negative
ages or ages of 130 and above. That grouping rule does not authorise rewriting
the supplied numeric age. The legacy patient helper treats negative or missing
ages as unknown for age bands while retaining the source value. The seven
negative referral/contact ages remain visible for review.

CYP102 closure and rejection dates after the reporting-period end trigger
specification warnings CYP10210 and CYP10220. They are not automatically invalid:
the September profile found 33,621 closure dates and 8,615 rejection dates in this
category. The specification instead rejects dates after file creation, before
period start or referral receipt, or after the parent discharge date. The
reporting-period warning alone does not justify truncating a date or removing
a service relationship.

The submitted-parent contact build passed its grain, source-key reconciliation,
time-precision and reference/parent regression checks: one model and five tests.
It retained all 61,490,763 contacts, each with a submitted referral row. No known
submitted person identifiers conflict; 1,182 comparisons remain unknown because
a person identifier is absent. The identity profile compiled and reproduced the
48,947 latest-parent conflicts and bridge totals above.

## Organisation codes recorded in the site field

The contact fact preserves `site_code` and its exact-reference `site_name`.
It also distinguishes missing codes, exact reference matches, supported padded
organisation codes and unrecognised codes through `site_code_status`. An exact
reference match can include a national default and does not by itself prove
that the submitted value identifies a physical treatment site.

The [NHS administrative coding rule](https://v2.datadictionary.nhs.uk/web_site_content/pages/codes/administrative_codes/nhs_administrative_codes.asp%40shownav%3D0.html)
allows an NHS trust's three-character organisation code to be extended with
`00` where a five-character organisation code is required. It states that
`00` is not a site suffix. The
[historical organisation coding table](https://v2.datadictionary.nhs.uk/web_site_content/pages/codes/administrative_codes/nhs_organisation_codes_tables.asp%40shownav%3D0.html)
also documents padding for care trusts and primary care trusts. For an unmatched
`Rxx00`, `Txx00` or `5xx00` value, the fact requires a known three-character
organisation code and the corresponding latest Dictionary organisation role.
It supplies `site_organisation_code`,
`site_organisation_name` and `site_organisation_name_source`. These identify
only the organisation. `site_name` remains null and the submitted provider
code remains unchanged. An exact reference match always takes precedence.

The source profile found 82 NHS trust codes on 10,344,050 contacts. All 82 prefixes
have the ODS NHS Trust role, RO197. The resolved organisation differs from the
submitted provider on 9,778 contacts; both remain visible. The rule does not
claim a physical site or infer that an organisation owns a particular site.

The applied roles are RO197 for NHS trusts, RO107 for care trusts and RO179
for primary care trusts. One NHS trust has the
Dictionary type Unknown on 483 contacts, so its confirmed RO197 role supplies
the trust classification. The organisation-role guard uses one latest
Dictionary row per normalised organisation code, ordered by last-updated or
created timestamp, then organisation surrogate key. Names continue to come from
the shared organisation reference. Two care-trust prefixes add 4,717 contacts
and eight primary-care-trust prefixes add 19,197.

Other unmatched values have commissioner or independent-provider prefixes.
These are known-prefix candidates, not confirmed padding. The
[modern NHS coding frames](https://archive.datadictionary.nhs.uk/DD%20Release%20April%202022/supporting_information/organisation_coding_frames.html)
place CCG codes in
the M organisation frame and K site frame. The historical B-frame rule does not
authorise removing their suffix. Removing `00` from every code would apply an
unsupported interpretation.

The commissioner-prefix bucket contains 1,304,371 contacts across 180 known
CCG or successor commissioner codes. Hounslow and Richmond CCG prefixes account
for 776,136 and 500,807 contacts respectively. No contact has the value `00000`.
A separate 1,122 contacts have a value ending in `00` whose three-character
prefix matches the national overseas-visitor default. The coding evidence does
not establish that this supplied value has the same meaning as the exact
default code.

The organisation-role guard excludes a local-authority prefix on 687 contacts
and an ICB prefix on 12 contacts that would otherwise match the broad `5xx00` pattern.

The retained quality profile reports padded organisation resolutions,
unrecognised site values and differences from the submitted provider separately.

The lookup investigation also checked the production `MAIN_DATA` site join,
four analyst snapshots, the ODS archive of organisations closed since 1991 and
three other ODS trust archives. The closed-organisation archive supplies one
additional exact site name on 20 contacts. The other archives and analyst
snapshots add no missing site names. The archive adds 17,024 reference codes;
all 430,189 existing codes, names and name sources remain unchanged. Its build
passed four models and eight tests, including source-code retention.

For the same period, the production MAIN_DATA join left 3,173,933 site names
unmatched, compared with 3,173,939 in the new fact before the archive addition.
A further 9,033,952 unmatched contacts predate April 2021. The legacy join
supplied no additional names. National treatment-site defaults and the
UKHFD default-site lookup also add no matches. These checks explain why the
remaining gap requires source-specific evidence rather than another general
label fallback.

The final contact build and all seven selected tests passed. The identity and
quality profiles compiled and ran successfully, with all 61,490,763 contacts
retained and unchanged historical-person comparison totals.

| Site code status | Contacts |
|---|---:|
| Exact reference match | 19,298,112 |
| Padded organisation code | 10,367,964 |
| Unrecognised supplied code | 1,839,907 |
| Missing code | 29,984,780 |

Organisation-only interpretations have no exact site name. The original count
of unlabelled site names therefore remains 12,207,871 after the 20 archive
matches, while 10,367,964 of those rows now have a supported organisation label.
Across the supported R/T/5 interpretations, 28,998 organisation codes differ
from the submitted provider; both remain visible. The final role-guarded build
passed the contact model and seven tests in 85 seconds. Aggregate checks
confirmed all 61,490,763 distinct contact keys and same-submission parent links,
zero known submitted-person conflicts, 1,182 unknown comparisons and the
unchanged 48,947 conflicts against the latest referral.
