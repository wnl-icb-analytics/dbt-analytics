# SUS monthly legacy field assessment

This assessment compares the 1,259 non-`z` fields in the four legacy SQL Server consolidated tables with the monthly SUS dbt facts. The field-level working report was generated from the legacy schema, Snowflake metadata and the implemented model YAML.

The legacy evidence proves that the final consolidation views copied fields from Current, Reconciliation or Post-Reconciliation fact tables. It does not show how the upstream loads populated most fields. A name match therefore proves availability, not identical meaning.

## Current coverage

| Decision | Fields | Treatment |
|---|---:|---|
| Implemented from date-range sources | 383 | Already present in the four facts. |
| Implemented from PBR enrichment | 65 | Present in the APC spell fact through the one-to-one PBR join. |
| Omit legacy placeholders | 36 | Spare or future-use fields with no supplied value. |
| Omit all-null PBR fields | 20 | Ten PBR fields occur in both legacy APC outputs and were 100% null in the inspected source. |
| Omit legacy operational fields | 8 | SQL Server consolidation keys which are not stable SUS business keys. |
| Needs upstream lineage | 319 | No safe source or derivation is proved by the available evidence. |
| Needs a current pricing source | 154 | Legacy pricing fields need an agreed current source and meaning. |
| Needs a child-grain rule | 120 | Candidate clinical or billing sources are one-to-many by encounter. |
| Deferred PBR enrichment | 62 | Available for APC episode from the PBR source, but not yet justified in the episode interface. |
| Needs a derivation definition | 55 | The legacy name suggests a calculation, but the rule and owner are not proved. |
| Review legacy provenance | 35 | Legacy load or audit fields need either a current replacement or removal. |
| Review duplicate legacy field | 2 | Two APC episode names map to one Snowflake field. |

The implemented facts currently cover 448 legacy fields. A further 64 field occurrences have evidence for omission. The remaining 747 need evidence or a business decision before implementation.

## Dataset result

| Dataset | Legacy non-`z` fields | Implemented | Evidence-backed omission | Still needs review |
|---|---:|---:|---:|---:|
| OP | 272 | 112 | 11 | 149 |
| A&E | 305 | 91 | 11 | 203 |
| APC spell | 206 | 128 | 21 | 57 |
| APC episode | 476 | 117 | 21 | 338 |

`GPPracticeCodeDerived` and `GPPracticeCodeDerived1` in the legacy APC episode table both map to `gp_practice_code_derived`. Publish one field unless upstream evidence proves that the duplicate names have different meanings.

## Recommended evidence order

1. Obtain the legacy SSIS fact-load mappings. This is the only direct route to resolving the 319 upstream-lineage fields and confirming whether same-name fields retain the same meaning.
2. Ask the commissioning or finance owner which legacy pricing outputs remain required and which Snowflake pricing source is authoritative. Do not recreate 154 historical pricing fields from their names.
3. Agree an encounter-level aggregation for each required critical-care or billing field before adding the 120 child-source fields. Direct joins would multiply episode or spell rows.
4. Confirm which of the 62 PBR fields add value to the APC episode fact. Their source is available, but copying them into both APC facts would widen the interface without a stated use.
5. Define and approve each of the 55 derived fields. Add shared definitions once, rather than copying opaque legacy calculations into each fact.

The detailed CSV contains only schema metadata and decisions. It contains no patient-level values.
