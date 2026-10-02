# QOF register calculation macros

Each `calculate_<register>_register` macro evaluates membership at one or more reference dates.
Non-QOF and clinical NDH macros live in `macros/ltc_registers/`.

## Pattern and callers

- `reference_date_expr` supplies one date and defaults to `CURRENT_DATE()`.
  `reference_dates` instead supplies a query returning a `reference_date` column.
- Events count once their clinical or order date and any recorded date are on or before the reference date.
  A null recorded date does not block an event (`ltc_register_known_by`).
- Age-restricted rules derive age at the reference date from the approximate birth date.
- Outputs have one row per person and reference date with returned evidence, including `is_on_register` and condition fields.
  Filter the membership flag when counting register members.

PIT views in `models/reporting/olids/disease_registers/qof_pit/` call the macros with `get_reference_date()`,
a deprecated wrapper around `qof_reference_date()`. Its default is the `2025-11-04` EMIS extract date in `dbt_project.yml`.
Monthly models in `history/` pass the last 60 completed month-ends from `ltc_register_history_month_ends()`.
`tests/ltc_register_fct_pit_reconciliation.sql` compares the macros with live facts at their build-date reference.

Macros read modelling event models (`int_*_all`) through `ref()`, with demographic inputs where used.
They do not query staging observations directly. `calculate_cvd_register` composes the CHD and stroke/TIA macros.
Live `fct_person_*_register` facts hold a second copy of each rule and do not call these macros.
Change both copies together; live facts also retain future-dated evidence and use current age where required.

See [LTC and QOF registers](../../docs/ltc-registers.md) for the version inventory and upgrade checklist,
[condition definitions](../../docs/model_documentation/olids_ltc_condition_definitions.md) for membership,
and [monthly history](../../docs/ltc-register-history.md) for population and date limits.
