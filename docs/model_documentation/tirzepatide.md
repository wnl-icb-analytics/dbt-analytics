# Tirzepatide candidates and QOF OBES2

The tirzepatide models select candidates from shared contracts already on
main. They do not maintain a second obesity register or re-select QOF BMI
observations.

| Need | Source |
| --- | --- |
| Candidate population, age, QOF ethnicity flag and the five comorbidities | `fct_person_obesity2_register` (QOF v51 OBES2, including later #1184 corrections) |
| Numeric BMI and NICE NG246 class | `int_bmi_latest` |
| Current registration, living and non-test status | `dim_person_active_patients` |
| GLP-1 order history | `int_glp1_medications_all` |
| Cohort 1 / Cohort 2 / BMI assessment needed | Programme mapping in `int_tirzepatide_candidates` |

`int_dyslipidaemia_diagnoses_all` and `int_obstructive_sleep_apnoea_diagnoses_all`
are the shared diagnosis observations. OBES2 already consumes the QOF clusters
and lipid evidence behind those comorbidities, so the programme reads the
register flags rather than rebuilding them.

One row represents a living, currently registered OBES2 adult. This is a current
candidate population, not a record of everyone who qualified when treatment
started. People can leave the population as their recorded BMI, diagnoses or
registration change.

## Alignment with the national programme

The [QOF v51 business rules](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/primary-care-business-rules/business-rules/quality-and-outcomes-framework-qof-business-rules-v51-2026/)
define OBES2 for adults with BMI at least 35, or 32.5 for specified ethnic
backgrounds, and at least four of five comorbidities. OBES005 explicitly refers
to the NICE TA1026 funding variation cohorts. The obesity rules in this release
are version 51.3, dated 4 June 2026.

The [NHS England commissioning guidance](https://www.england.nhs.uk/long-read/interim-commissioning-guidance-nice-ta1026-tirzepatide/)
sets these primary care rollout groups:

| Cohort | BMI | Comorbidities | First rollout year |
| --- | --- | --- | --- |
| 1 | At least 40 | At least four of the five | 2025/26 |
| 2 | At least 35 and below 40 | At least four of the five | 2026/27 |
| 3 | At least 40 | Three of the five | 2027/28 |

BMI thresholds are reduced by 2.5 for South Asian, Chinese, other Asian,
Middle Eastern, Black African and African-Caribbean family backgrounds.
Those ethnicity-adjusted bands are the NICE NG246 Obese Class III and Class II
categories already held on `int_bmi_latest`. The programme maps Class III to
Cohort 1 and Class II to Cohort 2 when that latest valid BMI is in the
preceding 12 months.

The models cover Cohorts 1 and 2. OBES2 requires four comorbidities, so it cannot
supply the future three-comorbidity Cohort 3 population.

## Assessment still required

OBES2 supplies ASCVD, unresolved hypertension, dyslipidaemia, obstructive sleep
apnoea and unresolved type 2 diabetes. Dyslipidaemia includes recent lipid
therapy or qualifying lipid results, rather than requiring a diagnosis code.

The commissioning guidance requires hypertension needing blood pressure
treatment, and sleep-clinic-confirmed apnoea for which CPAP or equivalent
treatment is indicated. The QOF diagnosis flags do not establish those facts.
Clinical assessment must confirm programme eligibility and prescribing
suitability, including current BMI and the required support programme.

The OBES2 implementation uses journal ethnicity. The national patient-table
ethnicity fallback is unavailable in the OLIDS source, so some people who
qualify for lower BMI thresholds may be missed. Cohort banding uses the shared
NICE BMI class, whose ethnicity adjustment comes from
`int_ethnicity_cardiometabolic_risk`. That can differ from the QOF journal flag
on the register. The outputs carry both: `requires_lower_bmi_thresholds`
explains the cohort band and `has_lower_bmi_threshold_ethnicity` explains
register entry.

## BMI evidence and prescribing status

OBES2 accepts any qualifying BMI evidence within 12 months, including `BMI35_COD`
without a measurement. The programme does not reuse that coded evidence to split
cohorts. It uses the latest valid numeric BMI from `int_bmi_latest` when that
date is in the preceding 12 months. That value can be a recorded `BMIVAL_COD`
or a BMI calculated from height and weight. It can differ from the BMI that
placed the person on OBES2.

Candidates without a recent Class II or Class III numeric BMI remain in the
population with `cohort` and `bmi_category` set to `BMI assessment needed` and
both cohort flags false. They remain candidates for review through
`is_actionable` when they have no recent GLP-1 order.

`is_currently_treated_glp1` means a GLP-1 order exists within six months. It does
not confirm dispensing or adherence. `latest_glp1_indication` is inferred from
BNF coding (6.1 diabetes, 4.5 obesity, otherwise Unknown), not a recorded
prescribing decision. `is_actionable` identifies
candidates without a recent GLP-1 order for review; it does not authorise
treatment. Obesity commissioning and prescribing for type 2 diabetes have
different eligibility rules.
