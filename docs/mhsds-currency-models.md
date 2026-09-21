# MHSDS currency models

These models classify Mental Health Services Data Set (MHSDS) activity into the NHSE 2026/27 mental health currencies and price it. They consume shared occupancy rules and recorded care evidence. General referral, activity and person analysis uses the separate [reporting domain](mhsds-domain-models.md).

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

MHSDS is a monthly resubmission feed. [`stg_mhsds_activesubmission`](../models/staging/commissioning/mhsds/stg_mhsds_activesubmission.sql) identifies the accepted file for each provider and reporting period. This is an input filter, not a published grain. Versioned models use [`select_latest_mhsds_record`](../macros/transformations/select_latest_mhsds_record.sql) to retain the newest reported version of each logical record. Period snapshots use [`select_accepted_mhsds_period_records`](../macros/transformations/select_accepted_mhsds_period_records.sql), then resolve records within their stated row meaning. MHS204 activity is restricted to its activity month, so the accepted file is authoritative for that period.

Two data facts shape everything downstream:

- **Local IDs are not globally unique.** Providers reuse local care-contact IDs across referrals, so the contact grain everywhere is `(uniq_serv_req_id, uniq_care_cont_id)`.
- Missing discharge dates do not establish current occupancy. [`int_mhsds_inpatient_occupancy`](../models/modelling/mental_health/inpatient/int_mhsds_inpatient_occupancy.sql) derives each retained interval's end as `discharged`, `open`, `last_submission` or `superseded`. Both encounter and currency models consume those intervals.

Source and classification inputs:

- [`stg_mhsds_referral.sql`](../models/staging/commissioning/mhsds/stg_mhsds_referral.sql) — one row per referral: received/closure dates, referral reason, priority.
- [`stg_mhsds_carecontact.sql`](../models/staging/commissioning/mhsds/stg_mhsds_carecontact.sql) retains accepted submission history. The contact reporting and currency models select the latest referral/contact pair.
- [`stg_mhsds_spell.sql`](../models/staging/commissioning/mhsds/stg_mhsds_spell.sql) — one row per hospital spell.
- [`stg_mhsds_mhs502wardstay.sql`](../models/staging/commissioning/mhsds/stg_mhsds_mhs502wardstay.sql) — ward stays per spell (bed type, dates).
- [`int_mhsds_currency_referral_service_type.sql`](../models/modelling/mental_health/currencies/int_mhsds_currency_referral_service_type.sql) — one team type per referral, resolved MHS102 → MHS902 → MHS101-v6 (~23% of referrals only carry the last).
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

One row per retained occupancy interval. Its main steps are:

- `base` consumes `int_mhsds_inpatient_occupancy`. That shared model retains one spell per person and admission date, removes strictly contained spells and truncates an earlier interval at a later admission. The classification model does not repeat interval selection.
- `latest_ward_stay` picks the ward with the latest end date, treating a missing end as open, then latest start date. Equal dates use reporting period, source-file receipt, submission identifier and ward-stay identifier, all descending. This makes ties repeatable; it does not establish which of two simultaneous ward records is clinically correct. The admitted-patient class supplies the bed-type category and inpatient setting (98A–D).
- **`latest_diagnosis`** takes the latest primary diagnosis on or before the spell's derived end date, categorised by ICD-10 range.
- **`classified`** runs the NHSE cascade: **diagnosis → bed type → referral reason**, each tier consulted only when earlier tiers cannot classify. Children (under 18 at admission) can only land in the all-age groups (MBC/MBY); a child whose diagnosis says an adult-only group goes to `MCG`, not through the cascade. Unclassifiable adults go to `MBU`.
- Currency code = group + `98` + ward setting (`Z` if unknown). The national grouper keeps cross-cutting crisis spells in `MAZ99`, so those retain family `99`. They publish as `MAZ99Z`: the `MAZ99A–D` suffixes name crisis service settings, not inpatient bed types, so a bed setting must not be carried into them. The bed setting stays in `setting_code`. `winning_tier` and the per-tier categories are kept on every row so each classification is explainable.

## 4. Contact classification — [`int_mhsds_contact_currency.sql`](../models/modelling/mental_health/currencies/int_mhsds_contact_currency.sql)

One row per (referral, contact), excluding contacts inside an inpatient spell window for the same referral. Same cascade with team type as the middle tier, plus:

- CYP contacts on MH Support Teams classify to `MCS` first.
- The crisis flag: crisis-team referrals count as crisis; A18 (single point of access) only for urgent/emergency priority.
- Family + setting: community teams → `96A–D`, crisis teams → `97A–D`, MAZ → `99A–D` by crisis setting, MHSTs → `MCS99Z`; teams with no setting fall to `96Z`/`97Z` by the crisis flag.

## 5. Price resolution — [`int_nhse_currency_price_resolution.sql`](../models/modelling/contracting/int_nhse_currency_price_resolution.sql)

One row per currency code any classifier can emit, with the fallback chain resolved once: exact code → the population's `Z` price → MBU for the setting → MBU `Z`. Needed because specialised settings are out of NCC scope (NULL prices) and some derivable codes have no published price. `MAZ99` has a contact price but no bed-day price, so the bed-day fact records the published code and uses the matching `MBU98` setting as `pricing_currency_code`.

## 6. Costing — the reporting facts

- [`fct_mhsds_currency_bed_days.sql`](../models/reporting/mental_health/currencies/fct_mhsds_currency_bed_days.sql) — one row per spell × fiscal year. Nights are attributed to the year they start in (`bed_days_from_date`/`bed_days_to_date` give each row's exact window); the resolved price is rebased to that year with the GDP deflator ([`uk_cost_indices`](../seeds/uk_cost_indices.csv)) and adjusted by the provider MFF ([`provider_market_forces_factor_2026_27`](../models/reference/finance/provider_market_forces_factor_2026_27.sql)). Open spells accrue cost only to their last submission evidence — the active feed runs ~6 weeks behind, so accruing to today would cost unevidenced nights.
- [`fct_mhsds_currency_contacts.sql`](../models/reporting/mental_health/currencies/fct_mhsds_currency_contacts.sql) — one row per (referral, contact). Attended contacts (status 5/6/missing) are costed; DNAs and cancellations are kept at zero cost so activity counts stay complete. (DNA cost is already smeared into attended unit prices by the NCC's construction — pricing them would double count.)

## 7. Currency reporting and general domain models

- `fct_mhsds_currency_current_inpatients` retains the currency-enriched census,
  with currency-selected diagnosis, setting and prices available downstream.
- `fct_mhsds_currency_referral_summary` adds the 2026/27 service, setting and
  population classifications to the general referral summary. Its crisis flag
  is explicitly named `is_currency_crisis_referral`.
- `fct_mhsds_current_inpatients` exposes general occupancy evidence without
  currency categories. The shared `int_mhsds_inpatient_occupancy` supplies both
  the census and currency classification.
- `fct_mhsds_person_summary` uses recorded diagnosis evidence and separates
  recorded spells from inferred occupancy. It does not consume currency models.

See the [MHSDS reporting guide](mhsds-domain-models.md) for analytical grains,
joining rules, observation dates and the interface migration.

## 8. Cost-index roll-up — [`int_cost_index_mhsds_activity_monthly.sql`](../models/modelling/mental_health/cost_index/int_cost_index_mhsds_activity_monthly.sql)

Person × month: bed days apportioned from the spell × fiscal-year fact to calendar months (per-night rate carries the deflator and MFF), contacts split into MH Crisis / MH Community. Feeds [`fct_person_cost_index_monthly`](../models/reporting/cross_system/cost_index/fct_person_cost_index_monthly.sql) as the `MHSDS` proxy-cost source.

## Caveats analysts should know

- **These are proxy costs** — indicative national prices on activity, for comparative and distributional analysis, not contract reconciliation.
- Source coverage and missing team details can affect comparisons with national provider totals and community/crisis splits.
- **Legacy long-stay spells** (admissions back to the 1970s) accrue decades of bed days; filter on dates if they distort a cut.
- The FY2022/23 contact-volume dip is a source completeness artefact (two providers' submissions), not a real activity change.
