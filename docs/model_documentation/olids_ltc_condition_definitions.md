# OLIDS LTC Condition Register Definitions

Handover reference for the 39 LTC condition registers in OLIDS: what each register means clinically, which code clusters drive it, and where each cluster comes from.

## How the registers work

- Each condition has a person-level register model at `models/reporting/olids/disease_registers/` (`qof/` subfolder for QOF conditions). These aggregate observation-level intermediate models under `models/modelling/olids/` (diagnoses, medications, observations).
- Intermediate models fetch coded events via the `get_observations('CLUSTER_ID', ...)` / `get_medication_orders(...)` macros, which resolve cluster IDs against `stg_reference_combined_codesets` → `DATA_LAKE__NCL.TERMINOLOGY.COMBINED_CODESETS`.
- The canonical condition list (codes, names, domains, QOF flag, denominator age rules) is the seed `seeds/ltc_register_denominator_rules.csv`. `fct_person_ltc_summary` unions all registers; `fct_person_ltc_register_status` adds denominator eligibility per person per condition.
- QOF registers follow QOF v50 business rules (explicitly cited for CKD, COPD, LD, RA, SMI, hypertension, epilepsy).

## Cluster sources

`COMBINED_CODESETS.SOURCE` records where every cluster comes from. Three sources are used by the LTC registers:

| Source | What it is |
|---|---|
| **PCD refset** (`SOURCE = 'PCD'`) | NHS England **Primary Care Domain SNOMED reference sets** — the nationally published QOF business-rules clusters (e.g. `AST_COD`, `CHD_COD`, `DM_COD`). Loaded from the PCD refset release into `COMBINED_CODESETS`. This is the source for **all QOF register diagnosis/resolution clusters** and for most non-QOF registers, which reuse published PCD clusters (ADHD, anxiety, autism, cerebral palsy, CLD, FH, gestational diabetes, MND, MS, Parkinson's, sickle cell, thalassaemia). |
| **Custom ECL** (`SOURCE = 'ECL_CACHE'`) | Locally defined clusters expanded from SNOMED ECL expressions on the terminology server (maintained via `UPSERT_ECL_CLUSTER` / `REFRESH_ECL_CLUSTER` in `DATA_LAKE__NCL.TERMINOLOGY`). Used where no suitable PCD cluster exists — chiefly medication clusters (`ASTTRT_COD`, `EPILDRUG_COD`, `LIT_COD`) and four diagnosis clusters (`FRAILTY_DX`, `HYPOTHY_COD`, `MASLD_DX_CODES`, `OA_COD`). |
| **LTC_LCS** (`SOURCE = 'LTC_LCS'`) | LTC LCS programme codesets. Only `SYSBP_COD` / `DIASBP_COD`, used for hypertension BP staging (not register membership). |

Notes:
- QOF intermediate models pass `source='PCD'` to `get_observations`, pinning them to the PCD refset variant. This matters because `AST_COD`, `CHD_COD` and `CKD_COD` also exist under UKHSA sources with different content.
- Every cluster/source pairing below was verified against `COMBINED_CODESETS.SOURCE` (July 2026).

## Conditions (39)

Clusters are **PCD refset** unless marked `(ECL)` = custom ECL or `(LTC_LCS)`. The Cluster source column summarises provenance per condition.

| Condition | is_qof | QOF ind. | Definition | Clusters (role) | Cluster source |
|---|---|---|---|---|---|
| Asthma (AST) | true | AST006 | Age ≥ 6, active diagnosis (latest diagnosis after any resolved code) plus asthma medication order in last 12 months | `AST_COD` (diagnosis), `ASTRES_COD` (resolved), `ASTTRT_COD` (ECL) (medication) | PCD refset; medication cluster custom ECL |
| Atrial Fibrillation (AF) | true | AF006 | Active AF diagnosis; latest diagnosis after any resolved code. No age limit | `AFIB_COD` (diagnosis), `AFIBRES_COD` (resolved) | PCD refset |
| Cancer (CAN) | true | CAN001 | First/new-episode cancer diagnosis on/after 1 Apr 2003; no resolution; excludes non-melanotic skin cancer (in cluster) | `CAN_COD` (diagnosis) | PCD refset |
| CHD | true | CHD003 | Any CHD diagnosis (MI, angina, coronary atherosclerosis); permanent, no age limit | `CHD_COD` (diagnosis) | PCD refset |
| CKD | true | CKD005 | Age ≥ 18 with CKD stage 3–5, not downstaged to stage 1–2 or resolved since | `CKD_COD` (stage 3–5), `CKD1AND2_COD` (downstage), `CKDRES_COD` (resolved) | PCD refset |
| COPD | true | COPD007 | Unresolved COPD diagnosis; diagnoses from 1 Apr 2023 follow v50 spirometry confirmation rules (FEV1/FVC < 0.7 near diagnosis or registration) | `COPD_COD` (diagnosis), `COPDRES_COD` (resolved), `FEV1FVC_COD` + `FEV1FVCL70_COD` (spirometry), `SPIRPU_COD` (unable — analytics only) | PCD refset |
| Dementia (DEM) | true | DEM001 | Any dementia diagnosis; permanent, no age limit | `DEM_COD` (diagnosis) | PCD refset |
| Depression (DEP) | true | DEP001 | Age ≥ 18, latest first/new episode on/after 1 Apr 2006, unresolved | `DEPR_COD` (diagnosis), `DEPRES_COD` (resolved) | PCD refset |
| Diabetes (DM) | true | DM017 | Age ≥ 17 with active diabetes diagnosis; type classified Type 1 vs Type 2 from latest type-specific code (Type 1 wins ties) | `DM_COD` (diagnosis), `DMTYPE1_COD` / `DMTYPE2_COD` (type), `DMRES_COD` (resolved) | PCD refset |
| Epilepsy (EP) | true | EPIL001 | Age ≥ 18, active diagnosis plus anti-epileptic drug order in last 6 months | `EPIL_COD` (diagnosis), `EPILRES_COD` (resolved), `EPILDRUG_COD` (ECL) (medication) | PCD refset; medication cluster custom ECL |
| Heart Failure (HF) | true | HF001 | Active HF diagnosis; sub-register for HF with LVSD / reduced EF. No age limit | `HF_COD` (diagnosis), `HFRES_COD` (resolved), `HFLVSD_COD` + `REDEJCFRAC_COD` (sub-register) | PCD refset |
| Hypertension (HTN) | true | HYP008/HTN005 | Unresolved hypertension diagnosis; no age limit for register. BP staging (NICE thresholds) reported alongside but not a register criterion | `HYP_COD` (diagnosis), `HYPRES_COD` (resolved); staging only: `BP_COD`, `ABPM_COD`, `HOMEAMBBP_COD`, `HOMEBP_COD`, `SYSBP_COD` (LTC_LCS), `DIASBP_COD` (LTC_LCS) | PCD refset; BP value clusters LTC_LCS (staging only) |
| Learning Disability (LD) | true | LD005 | LD diagnosis not followed by a removal code; no age limit (age ≥ 14 flag exposed for health checks) | `LD_COD` (diagnosis), `LDREM_COD` (removal) | PCD refset |
| NDH | true | NDH001 | Age ≥ 18 with NDH / IGT / pre-diabetes code, and no unresolved diabetes | `NDH_COD`, `IGT_COD`, `PRD_COD` (diagnosis); `DM_COD` / `DMRES_COD` (diabetes exclusion) | PCD refset |
| Obesity (OB) | true | OB001 | Age ≥ 18 with BMI ≥ 30, or BMI ≥ 27.5 if BAME ethnicity | `BMI30_COD` (coded BMI ≥ 30), `BMIVAL_COD` (BMI value), `ETH2016*_COD` set (BAME classification) | PCD refset |
| Osteoporosis (OST) | true | OST004 | Age 50–74: osteoporosis diagnosis + fragility fracture since Apr 2012 + DXA confirmation (scan or T-score ≤ −2.5). Age 75+: diagnosis + fracture since Apr 2014, no DXA needed | `OSTEO_COD` (diagnosis), `FF_COD` (fragility fracture), `DXA_COD` (scan), `DXA2_COD` (T-score) | PCD refset |
| PAD | true | PAD002 | Any PAD diagnosis; permanent, no age limit | `PAD_COD` (diagnosis) | PCD refset |
| Palliative Care (PC) | true | PC001 | Palliative care code on/after 1 Apr 2008, not since marked "no longer indicated" | `PALCARE_COD` (inclusion), `PALCARENI_COD` (no longer indicated) | PCD refset |
| Rheumatoid Arthritis (RA) | true | RA002 | RA diagnosis, age ≥ 16 at reference date; permanent | `RARTH_COD` (diagnosis) | PCD refset |
| SMI | true | MH003 | Ever-diagnosed SMI (remission does not remove), OR lithium order in last 6 months not since stopped | `MH_COD` (diagnosis), `MHREM_COD` (remission flag), `LIT_COD` (ECL) (lithium), `LITSTP_COD` (lithium stopped) | PCD refset; lithium cluster custom ECL |
| Stroke / TIA (STIA) | true | STIA001 | Any stroke or TIA diagnosis; permanent, no age limit | `STRK_COD` (stroke), `TIA_COD` (TIA) | PCD refset |
| ADHD | false | — | ADHD diagnosis not in remission (latest diagnosis after any remission code) | `ADHD_COD` (diagnosis), `ADHDREM_COD` (remission) | PCD refset |
| Anxiety (ANX) | false | — | Anxiety diagnosis, unresolved | `ANX_COD` (diagnosis), `ANXRES_COD` (resolved) | PCD refset |
| Autism | false | — | Any autism spectrum diagnosis; lifelong | `AUTISM_COD` (diagnosis) | PCD refset |
| Cerebral Palsy (CEREBRALP) | false | — | Any cerebral palsy diagnosis; lifelong | `CEREBRALP_COD` (diagnosis) | PCD refset |
| Chronic Liver Disease (CLD) | false | — | Any CLD diagnosis; cirrhosis flag when a cirrhosis code exists | `CLDATRISK1_COD` (diagnosis), `CIRRHOSIS_COD` (cirrhosis flag) | PCD refset |
| CYP Asthma (CYP_AST) | false | — | Under 18 with active asthma diagnosis plus asthma medication order in last 12 months (QOF asthma logic restricted to children) | `AST_COD` (diagnosis), `ASTRES_COD` (resolved), `ASTTRT_COD` (ECL) (medication) | PCD refset; medication cluster custom ECL |
| Familial Hypercholesterolaemia (FH) | false | — | FH diagnosis with age ≥ 20 at first diagnosis; genetic, no resolution | `FHYP_COD` (diagnosis) | PCD refset |
| Frailty (FRAIL) | false | — | Any frailty diagnosis code; latest severity (mild/moderate/severe) tracked from concept codes. Not eFI/eFI2-based | `FRAILTY_DX` (ECL) (diagnosis + severity) | Custom ECL |
| Gestational Diabetes (GESTDIAB) | false | — | Any gestational diabetes code ever (kept for future diabetes risk) | `GESTDIAB_COD` (diagnosis) | PCD refset |
| Hypothyroidism (THY) | false | — | Any hypothyroidism diagnosis; chronic | `HYPOTHY_COD` (ECL: `<< 40930008`) (diagnosis), `THY_COD` (legacy, kept for compatibility) | Custom ECL; legacy cluster PCD refset |
| Learning Disability Under 14 (LD_U14) | false | — | QOF LD logic restricted to age < 14 (paediatric cohort) | `LD_COD` (diagnosis), `LDREM_COD` (removal) | PCD refset |
| Motor Neurone Disease (MND) | false | — | Any MND diagnosis; progressive | `MND_COD` (diagnosis) | PCD refset |
| Multiple Sclerosis (MS) | false | — | Any MS diagnosis; chronic | `MS_COD` (diagnosis) | PCD refset |
| NAFLD / MASLD | false | — | Any MASLD/NAFLD diagnosis (modern MASLD plus legacy NAFLD/NASH concepts, with retired-code history) | `MASLD_DX_CODES` (ECL) (diagnosis) | Custom ECL |
| Osteoarthritis (OA) | false | — | Any osteoarthritis diagnosis; degenerative | `OA_COD` (ECL: `<< 396275006` + history supplement) (diagnosis) | Custom ECL |
| Parkinson's (PD) | false | — | Any Parkinson's diagnosis; progressive | `PD_COD` (diagnosis) | PCD refset |
| Sickle Cell Disease (SCD) | false | — | Any sickle cell diagnosis | `SICKLE_COD` (diagnosis) | PCD refset |
| Thalassaemia (THAL) | false | — | Any thalassaemia diagnosis | `THAL_COD` (diagnosis) | PCD refset |

## Known discrepancies (yml metadata vs SQL)

The SQL is authoritative; several `.yml` blocks carry placeholder cluster names:

| Register | yml says | SQL actually uses |
|---|---|---|
| Depression | `DEP_COD` (does not exist in COMBINED_CODESETS) | `DEPR_COD` |
| Hypertension | `HTN_COD` / `HTNRES_COD` | `HYP_COD` / `HYPRES_COD` |
| Obesity | `BMI_COD`, `ETH2016AI_COD` only | `BMI30_COD` + `BMIVAL_COD`, full `ETH2016*` BAME set |
| Palliative care | `PALLCARE_COD` | `PALCARE_COD` (+ `PALCARENI_COD`) |
| Rheumatoid arthritis | `RA_COD`; omits age ≥ 16 rule | `RARTH_COD` |
| Stroke/TIA | `STIA_COD` / `STIARES_COD` | `STRK_COD` + `TIA_COD`; no resolution cluster queried |
| Osteoporosis | documents 50–74 rule only | also implements the 75+ rule (no DXA) |
| Asthma / COPD | medication and spirometry clusters missing from yml `code_clusters` | `ASTTRT_COD`; `FEV1FVC_COD`, `FEV1FVCL70_COD`, `SPIRPU_COD` |
