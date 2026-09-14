# MHSDS Models: A Walkthrough

Models that classify **MHSDS** (Mental Health Services Data Set) activity into the NHSE 2026/27 mental health currencies, price it, and expose the domain (referrals, people, bed occupancy) as reusable facts. This doc walks the pipeline in the order data flows, linking each transformation's code.

Source logic for the currencies: NHSE "MH Currencies 26-27" grouping SQL (provider version). The NHSE code classifies but does not cost; pricing follows the "Basis of Price" column of the NHSE Non-Acute Collection Template price schedule (bed day for inpatient, contact for community/crisis).

## The currency framework in one minute

A currency code has three parts: `MAA98A` = population group (`MAA`) + family (`98`) + setting (`A`).

| Population group | Meaning |
|---|---|
| MAA | Adult – Psychosis and Bipolar Disorders |
| MAB | Adult – Mood and Anxiety Disorders |
| MAE | Adult – Neurocognitive Disorders |
| MAF | Adult – Personality Disorders |
| MAZ | Adult – Cross-cutting crisis |
| MBC | All Age – Eating and Feeding Disorders |
| MBY | All Age – Neurodevelopmental Disorders |
| MCS | CYP – Mental Health Support Teams |
| MCG | CYP – Other |
| MBU | Other / Unclassified |

| Family | Unit | Settings (final letter) |
|---|---|---|
| 98 | Inpatient bed day | A Acute & PICU, B Rehab, C Specialist, D Forensic, Z unknown |
| 96 | Community contact | A Community & Neighbourhood (CMHT), B Specialist, C Forensic, D Day Hospitals & Community Rehab, Z unknown |
| 97 | Crisis contact | A Core Services, B Alternatives, C MH Crisis Assessment Centres, D A&E Linked, Z unknown |
| 99 | Cross-cutting activity | MAZ99A–D by crisis setting (contacts only), MAZ99Z, MCS99Z |

## 1. Select accepted records

MHSDS is a monthly resubmission feed. [`stg_mhsds_activesubmission`](../models/staging/commissioning/mhsds/stg_mhsds_activesubmission.sql) identifies the accepted file for each provider and reporting period. This is an input filter, not a published grain. Versioned models use [`select_latest_mhsds_record`](../macros/transformations/select_latest_mhsds_record.sql) to retain the newest reported version of each logical record. Period snapshots use [`select_accepted_mhsds_period_records`](../macros/transformations/select_accepted_mhsds_period_records.sql), then resolve and test their own grain. MHS204 activity is restricted to its activity month, so the accepted file is authoritative for that period.

Two data facts shape everything downstream:

- **Local IDs are not globally unique.** Providers reuse local care-contact IDs across referrals, so the contact grain everywhere is `(uniq_serv_req_id, uniq_care_cont_id)`.
- **Undischarged spells are usually orphans.** Most spells with no discharge date simply stop being submitted (system cutovers, the 2024 BEH/C&I → NLFT merger). [`int_mhsds_spell_encounters`](../models/modelling/mental_health/encounters/int_mhsds_spell_encounters.sql) classifies each spell's end as `discharged`, `open`, or `last_submission`; the currency models reuse that derivation rather than re-deriving it.

The staging models:

- [`stg_mhsds_referral.sql`](../models/staging/commissioning/mhsds/stg_mhsds_referral.sql) — one row per referral: received/closure dates, referral reason, priority.
- [`stg_mhsds_carecontact.sql`](../models/staging/commissioning/mhsds/stg_mhsds_carecontact.sql) — one row per (referral, contact): date, attendance, consultation mechanism.
- [`stg_mhsds_spell.sql`](../models/staging/commissioning/mhsds/stg_mhsds_spell.sql) — one row per hospital spell.
- [`stg_mhsds_mhs502wardstay.sql`](../models/staging/commissioning/mhsds/stg_mhsds_mhs502wardstay.sql) — ward stays per spell (bed type, dates).
- [`stg_mhsds_servicetype.sql`](../models/staging/commissioning/mhsds/stg_mhsds_servicetype.sql) — one team type per referral, resolved MHS102 → MHS902 → MHS101-v6 (~23% of referrals only carry the last).
- [`int_mhsds_currency_primary_diagnosis.sql`](../models/modelling/mental_health/currencies/int_mhsds_currency_primary_diagnosis.sql) holds primary diagnosis history by referral and diagnosis timestamp. Repeated source versions and equal-timestamp rows use the grouper's ordering. ICD-10-coded rows pass through and SNOMED-coded rows map through the UK complex-map refset. The clinical-record fact keeps a broader history and does not apply currency supersession.
- [`stg_mhsds_mhactperiod.sql`](../models/staging/commissioning/mhsds/stg_mhsds_mhactperiod.sql) — Mental Health Act legal status periods (MHS401).
- [`stg_mhsds_patientindicators.sql`](../models/staging/commissioning/mhsds/stg_mhsds_patientindicators.sql) — child protection / looked-after status (MHS005).
- [`stg_mhsds_bridging.sql`](../models/staging/commissioning/mhsds/stg_mhsds_bridging.sql) — person → pseudonymised patient id.

## 2. The mapping rules as data (seeds)

All classification lookups are CSVs an analyst can read or amend without SQL:

- [`nhse_mh_currency_population_groups_2627.csv`](../seeds/nhse_mh_currency_population_groups_2627.csv) — category letter → currency group + whether under-18s can take it.
- [`nhse_mh_currency_referral_reasons_2627.csv`](../seeds/nhse_mh_currency_referral_reasons_2627.csv) — `PrimReasonReferralMH` → category.
- [`nhse_mh_currency_team_types_2627.csv`](../seeds/nhse_mh_currency_team_types_2627.csv) — `ServTeamTypeRefToMH` → category, crisis flag, contact setting.
- [`nhse_mh_currency_bed_types_2627.csv`](../seeds/nhse_mh_currency_bed_types_2627.csv) — `MHAdmittedPatientClass` (v5 and v6 code sets coexist; they never collide) → category + inpatient setting.
- [`nhse_mh_currency_icd10_groups_2627.csv`](../seeds/nhse_mh_currency_icd10_groups_2627.csv) — 3-character ICD-10 ranges → category.
- [`nhse_currency_prices_2627.csv`](../seeds/nhse_currency_prices_2627.csv) — every code in the NHSE price schedule → 26/27 indicative price (NULL = specialised, out of NCC scope).

## 3. Spell classification — [`int_mhsds_spell_currency.sql`](../models/modelling/mental_health/currencies/int_mhsds_spell_currency.sql)

One row per hospital spell. Reading it CTE by CTE:

- **`deduplicated` / `uncontained` / `base`** enforce **single occupancy** — a person occupies at most one MH bed per night, but provider-scoped spell ids duplicate admissions (merger re-registration, NHS + independent-sector dual submission, shifted-date copies). Three rules restore it: one spell per person + admission date (latest submission evidence wins), contained spells dropped, and discharge-forward supersession (a later admission ends any spell still open). These rules removed ~13% of bed-day history that was double counted.
- **`latest_ward_stay`** picks the spell's current ward, whose admitted-patient class gives the bed-type category and the inpatient setting (98A–D).
- **`latest_diagnosis`** takes the latest primary diagnosis on or before the spell's derived end date, categorised by ICD-10 range.
- **`classified`** runs the NHSE cascade: **diagnosis → bed type → referral reason**, each tier consulted only when earlier tiers cannot classify. Children (under 18 at admission) can only land in the all-age groups (MBC/MBY); a child whose diagnosis says an adult-only group goes to `MCG`, not through the cascade. Unclassifiable adults go to `MBU`.
- Currency code = group + `98` + ward setting (`Z` if unknown). The national grouper keeps cross-cutting crisis spells in `MAZ99`, so those retain family `99`. They publish as `MAZ99Z`: the `MAZ99A–D` suffixes name crisis service settings, not inpatient bed types, so a bed setting must not be carried into them. The bed setting stays in `setting_code`. `winning_tier` and the per-tier categories are kept on every row so each classification is explainable.

## 4. Contact classification — [`int_mhsds_contact_currency.sql`](../models/modelling/mental_health/currencies/int_mhsds_contact_currency.sql)

One row per (referral, contact), excluding contacts inside an inpatient spell window for the same referral. Same cascade with team type as the middle tier, plus:

- CYP contacts on MH Support Teams classify to `MCS` first.
- The crisis flag: crisis-team referrals count as crisis; A18 (single point of access) only for urgent/emergency priority.
- Family + setting: community teams → `96A–D`, crisis teams → `97A–D`, MAZ → `99A–D` by crisis setting, MHSTs → `MCS99Z`; teams with no setting fall to `96Z`/`97Z` by the crisis flag.

## 5. Price resolution — [`int_nhse_currency_price_resolution.sql`](../models/modelling/contracting/int_nhse_currency_price_resolution.sql)

One row per currency code any classifier can emit, with the fallback chain resolved once: exact code → the population's `Z` price → MBU for the setting → MBU `Z`. Needed because specialised settings are out of NCC scope (NULL prices) and some derivable codes have no published price. Its `not_null` test guarantees no fact row can be unpriced. `MAZ99` has a contact price but no bed-day price, so the bed-day fact records the published code and uses the matching `MBU98` setting as `pricing_currency_code`.

## 6. Costing — the reporting facts

- [`fct_mhsds_currency_bed_days.sql`](../models/reporting/mental_health/currencies/fct_mhsds_currency_bed_days.sql) — one row per spell × fiscal year. Nights are attributed to the year they start in (`bed_days_from_date`/`bed_days_to_date` give each row's exact window); the resolved price is rebased to that year with the GDP deflator ([`uk_cost_indices`](../seeds/uk_cost_indices.csv)) and adjusted by the provider MFF ([`provider_market_forces_factor_2026_27`](../models/reference/finance/provider_market_forces_factor_2026_27.sql)). Open spells accrue cost only to their last submission evidence — the active feed runs ~6 weeks behind, so accruing to today would cost unevidenced nights.
- [`fct_mhsds_currency_contacts.sql`](../models/reporting/mental_health/currencies/fct_mhsds_currency_contacts.sql) — one row per (referral, contact). Attended contacts (status 5/6/missing) are costed; DNAs and cancellations are kept at zero cost so activity counts stay complete. (DNA cost is already smeared into attended unit prices by the NCC's construction — pricing them would double count.)

## 7. The domain facts

- [`fct_mhsds_current_inpatients.sql`](../models/reporting/mental_health/currencies/fct_mhsds_current_inpatients.sql) — who is in an MH bed now: one row per **person** (single occupancy is enforced upstream), with admission date, days/months in bed, setting, currency, and the spell's last submission evidence date. "Now" means as of the active feed (~6 weeks behind).
- [`fct_mhsds_referral_episodes.sql`](../models/reporting/mental_health/fct_mhsds_referral_episodes.sql) — one row per referral: team and reason categories, crisis flag, contact aggregates (attended/DNA/cancelled + MHS204 indirect activity), wait to first attended contact (pre-referral contacts flagged as data quality, not negative waits), spell linkage, rejection, and episode status (rejected/closed/open).
- [`dim_person_mh_profile.sql`](../models/reporting/mental_health/dim_person_mh_profile.sql) — one row per person: referral counts and dates, contact recency (12m/90d windows), crisis contact in 12 months, current/ever inpatient (reconciles exactly with the census), latest diagnosis category, MHA detention history, looked-after-child flag and raw CPP status code (code semantics unverified against the TOS, so no boolean).

## 8. Cost-index roll-up — [`int_cost_index_mhsds_activity_monthly.sql`](../models/modelling/cross_system/cost_index/int_cost_index_mhsds_activity_monthly.sql)

Person × month: bed days apportioned from the spell × fiscal-year fact to calendar months (per-night rate carries the deflator and MFF), contacts split into MH Crisis / MH Community. Feeds [`fct_person_cost_index_monthly`](../models/reporting/cross_system/cost_index/fct_person_cost_index_monthly.sql) as the `MHSDS` proxy-cost source.

## Caveats analysts should know

- **These are proxy costs** — indicative national prices on activity, for comparative and distributional analysis, not contract reconciliation.
- **~27% of spells and ~25% of contacts are unclassified** (`MBU`), consistent with national MHSDS completeness; they still cost at MBU prices.
- **Provider-submitted currencies can't validate this**: MHS013 is empty in our feed.
- **NHSE provider totals expose source differences.** Against the rounded April–May 2026 national extracts, total contacts are about 2% lower for North London and CNWL and 8–9% lower for West London; inpatient episodes are within 0–3%. The accepted West London source files contain fewer contacts than the national provider totals. About 12,000 CNWL contacts per month have no resolvable team type in this extract, so community/crisis splits differ even where the total is close.
- **Legacy long-stay spells** (admissions back to the 1970s) accrue decades of bed days; filter on dates if they distort a cut.
- The FY2022/23 contact-volume dip is a source completeness artefact (two providers' submissions), not a real activity change.
- Recorded referral rejections (~1%) look under-reported against national rates — a data finding, not corrected.
