# QOF Register Calculation Macros

Register rules evaluated as at one or more reference dates.

## Pattern

Each `calculate_<register>_register` macro holds the register rules as at a
reference date. QOF registers live here; other registers in `macros/ltc_registers/`.
Each macro:
1. Accepts `reference_date_expr` (a single date, default `CURRENT_DATE()`) or
   `reference_dates` (a query returning a `reference_date` column; every date is evaluated)
2. Counts an event once its clinical date and recorded date are on or before the
   reference date (`ltc_register_known_by`)
3. Computes age at the reference date where a rule uses age
4. Returns one row per person and reference date: `reference_date, person_id,
   register_name, is_on_register, earliest_diagnosis_date, latest_diagnosis_date`
   plus register-specific columns

Callers:
- `qof_pit/pit_*_register` views: one date from the `qof_reference_date` var
- `history/fct_person_*_register_by_month`: the 60 completed month-ends from
  `ltc_register_history_month_ends()`
- `tests/ltc_register_fct_pit_reconciliation.sql`: today's date, compared with the
  live `fct_person_*_register` facts

Macros read modelling event models (`int_*_all`), never staging observations.

## Register Types

### Simple (Diagnosis Only, Lifelong)
- CHD, Cancer, Stroke/TIA, PAD, Heart Failure, Atrial Fibrillation, Palliative Care
- Logic: Presence of diagnosis = on register
- No resolution codes or age restrictions

### Age Restricted
- Diabetes (≥17), Asthma (≥6), CKD (≥18), Depression (≥18), Epilepsy (≥18), Rheumatoid Arthritis (≥16)
- Hypertension (≤79) - upper age limit
- Logic: Age threshold + active diagnosis

### External Validation Required
- Asthma (requires medication in last 12 months)
- COPD (complex spirometry rules)
- Logic: Diagnosis + supporting data

### Complex Business Rules
- COPD (Rules 1-4 with date bifurcation)
- Diabetes (Type classification)
- Obesity (BMI-based, not diagnosis codes)
