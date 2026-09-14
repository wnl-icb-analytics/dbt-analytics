# Tirzepatide candidates and QOF OBES2

The tirzepatide models use `fct_person_obesity2_register` from QOF v51 to
identify candidates for primary care assessment. They reuse its age, BMI,
ethnicity and comorbidity rules. They do not maintain separate disease
registers for the programme.

One row represents a living, currently registered OBES2 adult with qualifying
BMI evidence in the preceding 12 months. This is a current candidate population,
not a record of everyone who qualified when treatment started. People can leave
the population as their recorded BMI, diagnoses or registration change.

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
qualify for lower BMI thresholds may be missed.

## BMI evidence and prescribing status

OBES2 accepts any qualifying BMI evidence within 12 months. The programme model
uses the most recent qualifying record in that window to split the cohorts;
this may differ from the most recent BMI overall. Future-dated or
future-recorded BMI evidence is excluded from this programme selection.

`BMI35_COD` establishes a BMI of at least 35 without supplying a measurement.
It does not establish whether the higher Cohort 1 threshold was met.
Candidates without numeric evidence establishing either BMI band remain in the
population with `cohort` and `bmi_category` set to `BMI assessment needed` and
both cohort flags false. They remain candidates for review through
`is_actionable` when they have no recent GLP-1 order.
`bmi_category` contains the historical ethnicity-adjusted rollout labels; it is
not a measured WHO obesity classification.

`is_currently_treated_glp1` means a GLP-1 order exists within six months. It does
not confirm dispensing or adherence. `latest_glp1_indication` is inferred from
BNF coding, not a recorded prescribing decision. `is_actionable` identifies
candidates without a recent GLP-1 order for review; it does not authorise
treatment. Obesity commissioning and prescribing for type 2 diabetes have
different eligibility rules.
