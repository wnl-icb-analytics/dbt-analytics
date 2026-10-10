# OLIDS LTC condition register definitions

Membership reference for the 39 condition codes in `fct_person_ltc_summary`: what each register means, which code clusters drive it and where each cluster comes from.

## How the registers work

The [maintainer guide](../ltc-registers.md) maps live facts, rule macros, PIT views, spec versions and upgrade steps.
The [monthly history reference](../ltc-register-history.md) explains population, dates and the known-by rule.

Definitions below describe current membership. Ages are at the reference date, or from `dim_person_age` in live facts.
Live FH, gestational diabetes and NAFLD filter to current active patients; monthly history requires registration and living status at month-end.
The seed `ltc_register_denominator_rules.csv` supplies summary labels and denominator ages. It does not set membership.
The QOF indicator column records the identifier in model metadata, not a list of current payment indicators.

## Cluster sources

Clusters resolve through `stg_reference_combined_codesets`, which reads `DATA_LAKE__NCL.TERMINOLOGY.COMBINED_CODESETS`.
Checked against `COMBINED_CODESETS.SOURCE` on 2 October 2026, every cluster below is a PCD refset cluster except:

| Mark | Source | Clusters |
|---|---|---|
| `(ECL)` | `ECL_CACHE`: ECL definitions refreshed weekly with `REFRESH_ECL_CLUSTER`. `ASTTRT_COD` and `EPILDRUG_COD` are the QOF v51 dm+d drug refsets; the others are local | `ASTTRT_COD`, `EPILDRUG_COD`, `LIT_COD`, `HYPOTHY_COD`, `MASLD_DX_CODES`, `OA_COD` |
| `(LTC_LCS)` | `LTC_LCS`: LTC LCS value sets | `SYSBP_COD`, `DIASBP_COD` (hypertension BP staging only) |

QOF register inputs pass `source='PCD'` for every PCD cluster. This matters because `AST_COD`, `CHD_COD`, `CKD_COD` and `DMRES_COD` also exist under UKHSA sources with different content.
Some inputs use `include_history=true` to add retired SNOMED predecessors.

## Conditions (39)

Rules were checked against the SQL. See the [spec version table](../ltc-registers.md#spec-versions) for the QOF version each register follows.

| Condition | is_qof | QOF ind. | Definition | Clusters (role) | Cluster source |
|---|---|---|---|---|---|
| Asthma (AST) | true | AST006 | Aged 5 or over, latest diagnosis later than any resolved code, plus an asthma medication order in the preceding 12 months | `AST_COD` (diagnosis), `ASTRES_COD` (resolved), `ASTTRT_COD` (ECL) (medication) | PCD; medication drug refset (ECL) |
| Atrial Fibrillation (AF) | true | AF006 | AF diagnosis later than any resolved code; no age limit | `AFIB_COD` (diagnosis), `AFIBRES_COD` (resolved) | PCD |
| Cancer (CAN) | true | CAN001 | Latest first/new-episode cancer diagnosis on or after 1 April 2003; no resolution or age rule | `CAN_COD` (diagnosis) | PCD |
| CHD | true | CHD003 | Any CHD diagnosis; no age or resolution rule | `CHD_COD` (diagnosis) | PCD |
| CKD | true | CKD005 | Aged 18 or over with CKD stage 3 to 5, and no later stage 1 to 2 or resolved code | `CKD_COD` (stage 3–5), `CKD1AND2_COD` (downstage), `CKDRES_COD` (resolved) | PCD |
| COPD | true | COPD007 | Unresolved COPD evidence. Disorder codes count at any date; administrative codes within two years before the reference date. Diagnoses before 1 April 2023 use Rule 1; later ones use Rules 2 to 4, and Rule 4 admits remaining patients without spirometry confirmation | `COPDDIAG_COD` (disorder), `COPDPROC_COD` (administrative), `COPDRES_COD` (resolved), `FEV1FVC_COD` + `FEV1FVCL70_COD` (spirometry), `SPIRPU_COD` (live descriptive fields only) | PCD |
| Dementia (DEM) | true | DEM001 | Any dementia diagnosis; no age or resolution rule | `DEM_COD` (diagnosis) | PCD |
| Depression (DEP) | true | DEP001 | Aged 18 or over, latest first/new episode on or after 1 April 2006, later than any resolved code | `DEPR_COD` (diagnosis), `DEPRES_COD` (resolved) | PCD |
| Diabetes (DM) | true | DM017 | Aged 17 or over with an unresolved diagnosis. Latest type code sets Type 1 or Type 2; Type 1 wins ties | `DM_COD` (diagnosis), `DMTYPE1_COD` / `DMTYPE2_COD` (type), `DMRES_COD` (resolved) | PCD |
| Epilepsy (EP) | true | EPIL001 | Aged 18 or over, diagnosis later than any resolved code, plus an anti-epileptic drug order in the preceding six months | `EPIL_COD` (diagnosis), `EPILRES_COD` (resolved), `EPILDRUG_COD` (ECL) (medication) | PCD; medication drug refset (ECL) |
| Heart Failure (HF) | true | HF001 | Unresolved HF diagnosis; no age limit. HF3 flag also requires reduced ejection fraction; LVSD alone does not qualify | `HF_COD` (diagnosis), `HFRES_COD` (resolved), `REDEJCFRAC_COD` (HF3); `HFLVSD_COD` in descriptive fields | PCD |
| Hypertension (HTN) | true | HTN005 | Unresolved hypertension diagnosis; no age limit. BP staging (NICE thresholds) reported alongside but not a register criterion | `HYP_COD` (diagnosis), `HYPRES_COD` (resolved); staging only: `BP_COD`, `ABPM_COD`, `HOMEAMBBP_COD`, `HOMEBP_COD`, `SYSBP_COD` (LTC_LCS), `DIASBP_COD` (LTC_LCS) | PCD; BP value clusters LTC_LCS (staging only) |
| Learning Disability (LD) | true | LD005 | LD diagnosis later than any removal code; no age limit. Age 14-or-over flag for health checks | `LD_COD` (diagnosis), `LDREM_COD` (removal) | PCD |
| NDH | true in seed; false in live YAML | - | Clinical cohort aged 18 or over with NDH, IGT or pre-diabetes evidence and no unresolved diabetes. GDM alone never qualifies; QOF NDH/GDM is a separate model | `NDH_COD`, `IGT_COD`, `PRD_COD` (diagnosis); `DM_COD` / `DMRES_COD` (diabetes exclusion) | PCD |
| Obesity (OB) | true | OB001 | Aged 18 or over, latest valid BMI (5 to 400 kg/m²) at any date of at least 30 kg/m², or 27.5 kg/m² when a lower-threshold ethnicity is recorded on the date of the latest ethnicity record. Not QOF's 12-month window | `BMI30_COD` (coded BMI ≥ 30), `BMIVAL_COD` (BMI value), `ETHALL*_COD` groups and `ETHNICITYND_COD` (ethnicity) | PCD |
| Osteoporosis (OST) | true | OST004 | Aged 50 to 74: diagnosis, fragility fracture on or after 1 April 2012, and DXA scan or T-score of -2.5 or lower. Aged 75 or over: diagnosis and fracture on or after 1 April 2014, no DXA needed | `OSTEO_COD` (diagnosis), `FF_COD` (fragility fracture), `DXA_COD` (scan), `DXA2_COD` (T-score) | PCD |
| PAD | true | PAD002 | Any PAD diagnosis; no age or resolution rule | `PAD_COD` (diagnosis) | PCD |
| Palliative Care (PC) | true | PC001 | Palliative care code on or after 1 April 2008, with no later "no longer indicated" code | `PALCARE_COD` (inclusion), `PALCARENI_COD` (no longer indicated) | PCD |
| Rheumatoid Arthritis (RA) | true | RA002 | RA diagnosis, aged 16 or over; no resolution rule | `RARTH_COD` (diagnosis) | PCD |
| SMI | true | MH003 | Any `MH_COD` diagnosis, even in remission, including codes also in `MHREM_COD`; `MHREM_COD`-only codes do not qualify. Lithium alone never qualifies; lithium fields are descriptive | `MH_COD` (diagnosis), `MHREM_COD` (remission flag); descriptive: `LIT_COD` (ECL) (lithium), `LITSTP_COD` (stopped) | PCD; lithium local ECL |
| Stroke / TIA (STIA) | true | STIA001 | Any stroke or TIA diagnosis; no age or resolution rule | `STRK_COD` (stroke), `TIA_COD` (TIA) | PCD |
| ADHD | false | - | ADHD diagnosis later than any remission code | `ADHD_COD` (diagnosis), `ADHDREM_COD` (remission) | PCD |
| Anxiety (ANX) | false | - | Unresolved anxiety diagnosis | `ANX_COD` (diagnosis), `ANXRES_COD` (resolved) | PCD |
| Autism | false | - | Any autism spectrum diagnosis; no age or resolution rule | `AUTISM_COD` (diagnosis) | PCD |
| Cerebral Palsy (CEREBRALP) | false | - | Any cerebral palsy diagnosis; no age or resolution rule | `CEREBRALP_COD` (diagnosis) | PCD |
| Chronic Liver Disease (CLD) | false | - | Any CLD or cirrhosis diagnosis; cirrhosis flag when a `CIRRHOSIS_COD` code exists | `CLDATRISK1_COD` (diagnosis), `CIRRHOSIS_COD` (cirrhosis flag) | PCD |
| CYP Asthma (CYP_AST) | false | - | Asthma rule for under-18s with no lower age bound. Overlaps AST at ages 5 to 17 | `AST_COD` (diagnosis), `ASTRES_COD` (resolved), `ASTTRT_COD` (ECL) (medication) | PCD; medication drug refset (ECL) |
| Familial Hypercholesterolaemia (FH) | false | - | Any FH diagnosis at any age; no resolution rule | `FHYP_COD` (diagnosis) | PCD |
| Frailty (FRAIL) | false | - | Any coded mild, moderate or severe frailty. Latest finding sets severity; tied dates prefer greater severity. Not eFI/eFI2 | `MILDFRAIL_COD`, `MODFRAIL_COD`, `SEVFRAIL_COD` (severity) | PCD |
| Gestational Diabetes (GESTDIAB) | false | - | Any gestational diabetes diagnosis at any age; no resolution rule | `GESTDIAB_COD` (diagnosis) | PCD |
| Hypothyroidism (THY) | false | - | Any hypothyroidism diagnosis; no age or resolution rule | `HYPOTHY_COD` (ECL) (diagnosis), `THY_COD` (legacy diagnosis) | Local ECL; legacy cluster PCD |
| Learning Disability Under 14 (LD_U14) | false | - | LD rule restricted to age under 14 | `LD_COD` (diagnosis), `LDREM_COD` (removal) | PCD |
| Motor Neurone Disease (MND) | false | - | Any MND diagnosis; no age or resolution rule | `MND_COD` (diagnosis) | PCD |
| Multiple Sclerosis (MS) | false | - | Any MS diagnosis; no age or resolution rule | `MS_COD` (diagnosis) | PCD |
| NAFLD / MASLD | false | - | Any MASLD/NAFLD diagnosis, including retired SNOMED predecessors | `MASLD_DX_CODES` (ECL) (diagnosis) | Local ECL |
| Osteoarthritis (OA) | false | - | Any osteoarthritis diagnosis; no age or resolution rule | `OA_COD` (ECL) (diagnosis) | Local ECL |
| Parkinson's (PD) | false | - | Any Parkinson's diagnosis; no age or resolution rule | `PD_COD` (diagnosis) | PCD |
| Sickle Cell Disease (SCD) | false | - | Any sickle cell diagnosis | `SICKLE_COD` (diagnosis) | PCD |
| Thalassaemia (THAL) | false | - | Any thalassaemia diagnosis | `THAL_COD` (diagnosis) | PCD |

## Known discrepancies (YAML metadata vs SQL)

The SQL is authoritative.

| Register | YAML metadata | SQL behaviour |
|---|---|---|
| Hypertension | Aged 18 or over; `HTN_COD` / `HTNRES_COD` | No membership age limit; `HYP_COD` / `HYPRES_COD` |
| Asthma / COPD | `code_clusters` omits medication and spirometry inputs | `ASTTRT_COD`; `FEV1FVC_COD`, `FEV1FVCL70_COD` |
| NDH | Seed says `is_qof=true` | Live YAML says `is_qof=false`; QOF NDH/GDM is a separate model |
