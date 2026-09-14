# SUS Models

Models built from **SUS** (Secondary Uses Service) data — the national activity feed for secondary care. Three dataset types flow through three layers (staging → modelling → reporting).

## Dataset types

| Type | Meaning | Grain |
|------|---------|-------|
| **APC** | Admitted Patient Care (inpatient) | Spell / episode |
| **AE / ECDS** | Emergency Care (A&E attendances) | Attendance |
| **OP** | Outpatient | Appointment |

## Staging — `models/staging/commissioning/sus/`

Clean, deduplicated pass-throughs of the raw SUS tables. One row per record of the named grain.

**APC**
- `stg_sus_apc_spell` — one row per admission spell (locations, dates, diagnoses, procedures, HRG)
- `stg_sus_apc_spell_episodes` — one row per episode per spell (latest transaction wins)
- `stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd` — ICD-10 diagnoses per episode
- `stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs` — OPCS-4 procedures per episode
- `stg_sus_apc_spell_episodes_commissioning_grouping_unbundled_hrg` — unbundled HRGs per episode

**AE / ECDS**
- `stg_sus_ecds_emergency_care` — one row per A&E attendance
- `stg_sus_ecds_clinical_diagnoses_snomed` — SNOMED diagnoses per attendance
- `stg_sus_ecds_clinical_investigations_snomed` — SNOMED investigations per attendance
- `stg_sus_ecds_clinical_treatments_snomed` — SNOMED treatments per attendance
- `stg_sus_ecds_clinical_coded_findings` — SNOMED coded findings per attendance
- `stg_sus_ecds_clinical_comorbidities` — SNOMED comorbidities per attendance

**OP**
- `stg_sus_op_appointment` — one row per outpatient appointment
- `stg_sus_op_appointment_clinical_coding_diagnosis_icd` — ICD-10 diagnoses per appointment
- `stg_sus_op_appointment_clinical_coding_procedure_opcs` — OPCS-4 procedures per appointment
- `stg_sus_op_appointment_commissioning_grouping_unbundled_hrg` — unbundled HRGs per appointment

## Modelling — `models/modelling/acute/` and `models/modelling/population/demographics/`

Normalises the three dataset types into shared shapes: encounters, diagnoses, procedures, HRGs, demographics.

**Encounters** (`encounters/`)
- `int_sus_apc_encounter` — event history per admission spell (demographics, specialty, HRG, cost)
- `int_sus_op_encounter` / `int_sus_op_appointment` — event history per attended outpatient appointment
- `int_sus_uec_encounter` — event history per A&E attendance (chief complaint, acuity, SNOMED→ICD-10 mappings)

**Diagnoses** (`diagnosis/`)
- `int_sus_apc_diagnosis` — ICD-10 diagnoses (secondary-care diagnosis table / phenolab)
- `int_sus_op_diagnosis` — ICD-10 diagnoses for outpatient records
- `int_sus_uec_diagnosis` — A&E SNOMED diagnoses mapped to ICD-10

**Procedures** (`procedure/`)
- `int_sus_apc_procedure` / `int_sus_op_procedure` — OPCS-4 procedures
- `int_sus_uec_procedure` — A&E observations (investigations, treatments, comorbidities, findings) as SNOMED
- `int_sus_apc_procedure_hrg` / `int_sus_op_procedure_hrg` — core + unbundled HRGs for commissioning

**Demographics** (`demographics/`)
- `int_person_pmi_dataset_sus` — one row per patient (gender, DOB, ethnicity, LSOA, GP practice)

## Reporting — `models/reporting/acute/`

Person-level rolling-window summaries (one row per patient).

- `fct_person_sus_apc_recent` — recent inpatient spell counts + length of stay (12/3/1-month)
- `fct_person_sus_op_recent` — recent outpatient appointment counts (12-month: attendances, first attendances, specialties, providers)
- `fct_person_sus_uec_recent` — recent A&E attendance counts (12-month: illness/injury/Type 1)

## Lineage

```
raw_sus_{apc,ae,op}_*  →  stg_sus_*  →  int_sus_*  →  fct_person_sus_*_recent
```

Clinical codes (ICD-10, OPCS-4, SNOMED) are extracted at staging and enriched
with vocabulary mappings in the modelling layer. HRG (tariff/commissioning)
codes are carried separately for commissioning analysis.
