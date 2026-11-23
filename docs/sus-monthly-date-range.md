# SUS monthly date-range models

These models expose the supplied monthly SUS date-range data without reproducing the legacy consolidated-table transformations.

## Models and grain

- `fct_sus_op_monthly_attendance` has one row per outpatient source encounter.
- `fct_sus_ae_monthly_attendance` has one row per A&E source encounter.
- `fct_sus_apc_monthly_episode` has one row per admitted-patient source encounter.
- `fct_sus_apc_monthly_spell` has one row per admitted-patient source encounter where `dv_is_spell = 1`. The source `sk_encounter_id` is the key because `spell_identifier` is not unique.

The facts retain the full history supplied by the date-range sources. Validation on 20 November 2026 found the latest activity date was 30 September 2026 in all four facts. The earliest retained activity varies by dataset. These sources do not expose separate Current, Reconciliation or Post-Reconciliation populations, and they are not rolling two-month tables.

The four reporting facts are physical tables, inheriting the reporting folder's materialisation. They refresh when dbt builds them. Raw and staging models remain views.

All four facts retain every administrative category. The shared facts do not apply the legacy private-activity exclusion. Applying that rule would remove 1.2189% of inspected outpatient rows and 1.9205% of inspected admitted-patient rows. A report that needs this population should apply an agreed filter downstream.

The spell fact left joins `PBR_Preprocess_SP_DateRange` by `sk_encounter_id`. This keeps spells without a PBR match. The PBR source was one row per encounter in the inspected data and matched 87.6238% of spell rows.

## Field boundary

The first release exposes source-backed date-range fields. It does not reproduce fields derived only in the legacy SQL.

No `z` fields are implemented. Ten PBR fields that were entirely null in the inspected source are also omitted from staging and reporting:

- `bpt_indicator_2` and `bpt_indicator_2_action`
- `bpt_indicator_3` and `bpt_indicator_3_action`
- `bpt_indicator_4` and `bpt_indicator_4_action`
- `bpt_indicator_5` and `bpt_indicator_5_action`
- `specialised_service_code_4`
- `specialised_service_code_5`

The remaining comparison with the legacy consolidated outputs is deliberately separate. See [SUS monthly legacy field assessment](sus-monthly-legacy-field-assessment.md) for the classification and evidence needed. Discovery found the following non-`z` legacy fields which were not direct date-range matches:

| Dataset | Direct source matches | Candidate fields in another source | Ambiguous candidates | Operational omissions | Not found in date-range source |
|---|---:|---:|---:|---:|---:|
| OP | 112 | 0 | 0 | 2 | 158 |
| A&E | 91 | 0 | 0 | 2 | 212 |
| APC spell | 63 | 77 | 0 | 2 | 64 |
| APC episode | 117 | 88 | 2 | 2 | 267 |

Each unmatched legacy field needs classification as a required derivation, a field available from an agreed additional source, or a retired legacy field before it is added.

## Source definitions awaiting confirmation

The source column metadata inspected on 23 November 2026 has no descriptions for the reviewed measures, period fields or spell indicator. The `ENGINEER` role cannot inspect the underlying `SUS` database definitions. Numeric types and observed values do not establish units or business meanings.

The YAML descriptions therefore mark these definitions as unconfirmed rather than assigning guessed units, date formats or code meanings:

| Fields | Evidence required from the source owner |
|---|---|
| RTT length, A&E waits and duration, APC lengths of stay | Units, calculation boundaries, adjustments, special values and null meanings for each source field. |
| Numeric tariff figures | Currency and scaling, adjustment sequence, applicability by year, special values and null meanings. |
| `dv_is_spell` | The upstream rule for assigning value 1 and the valid meanings of other values or null. The implemented selection remains value 1 only. |
| `dv_activity_period_date` | Numeric encoding, activity-period basis, special values and null meanings. |
| PBR `activity_period` | Encoding and period basis, including its relationship to `applicable_date`, `extract_date` and encounter activity dates. |

The [NHS SUS extract specification and guidance](https://digital.nhs.uk/services/secondary-uses-service-sus/payment-by-results-guidance) describe national extracts. A confirmed mapping from those definitions to the local date-range derivations is still needed. Until then, these source values support reconciliation, not an assertion that their measures are approved for threshold or financial analysis.

## Validation results

The models test `sk_encounter_id` as the row key. The PBR staging model tests the same key because the spell enrichment assumes a one-to-one join. `analyses/sus_monthly/sus_monthly_reconciliation.sql` compares monthly source and fact row counts and returns discrepancies only.

On 20 November 2026, all four reporting tables built successfully in DEV, all eight reporting key tests passed, and the monthly reconciliation returned no discrepancies. This validates preservation of the supplied source counts; it does not establish record-level equivalence with ERNI or resolve the legacy field decisions above.

ERNI comparison identified an August 2026 OP coverage gap for provider RV3 already present in the Snowflake source. Its ingestion or extract cause remains unconfirmed. This source investigation is separate from the dbt implementation and must be resolved before claiming full ERNI replacement readiness.
