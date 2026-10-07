# NICE indicator benchmarks

We compared the 149 NICE indicator measures with published data for North Central London (NCL)
practices in October 2026, using our monthly history (31 March 2022 to 2026) and the current build.
This page records what was checked, how closely we agree and what changed as a result. Indicator
definitions are in [nice-indicators.md](nice-indicators.md).

## Summary

| Result | Measures |
|---|---:|
| Validated: within 3 points of a published figure with the same or close definition | 94 |
| Gap explained: comparator exists; the difference is plausibly explained but not reproduced | 8 |
| Loose comparator: related figure only (retired indicator, pilot, study) | 44 |
| No comparator | 3 |

The 27 NICE registers in the catalogue are validated against QOF prevalence.

Confidence in the 94 validations, from checks practice by practice, by PCN and by borough:

| Confidence | Measures | Meaning |
|---|---:|---|
| High | 56 | Practice-level comparison; all five boroughs within 3 points; at least 80% of PCNs within 3 points; at least 70% of practices within 5 points; holds in a second year where available |
| Medium | 21 | Pooled rate and median practice agree, but one finer check misses, or the comparator is sub-ICB only |
| Low | 17 | London or England figures only, a small sample, or finer checks weaker than the pooled agreement |

## How we compared

- **Sources:** QOF 2021/22 to 2025/26 (practice level) and indicators no longer in QOF (INLIQ),
  CVDPREVENT, National Diabetes Audit (NDA), physical health checks for people with severe mental
  illness (PHSMI), Learning Disabilities Health Check Scheme (LDHC), Primary Care Dementia Data
  (PCDD), childhood vaccination coverage (COVER), UKHSA flu and shingles uptake, cervical screening
  coverage, GP Contract Services (GPCS), Network Contract DES and Investment and Impact Fund
  (NCD, IIF), and NICE's own indicator testing reports.
- **Practice matching:** published practice figures were joined to ours by practice code for the
  same period end. We report the pooled rate on common practices and the spread of practice gaps.
- **Like with like:** QOF and similar sources remove personalised care adjustments (PCAs); we do not
  apply them. We compare with the gross rate: numerator divided by (denominator plus PCAs).
- **Rules aligned:** where definitions differ in a known way (thresholds, windows, age bands,
  register rules), we recomputed our measure under the comparator's rules. If that closed the gap
  to within 3 points, the explanation is proven and the measure counts as validated.

## Why our rates differ from QOF

These differences are deliberate: our measures follow NICE's wording. Each was reproduced.

| Difference | Typical effect | Examples |
|---|---|---|
| NICE blood pressure targets are strict (below 140/90); QOF's are inclusive (140/90 or less) | 6 to 8 points lower | IND239, IND241, IND243, IND249 |
| NICE excludes people with a contraindication; QOF's gross rate keeps them | 4 to 10 points higher | IND132, IND133, IND134 |
| NICE requires a separate NYHA functional assessment code | about 50 points lower | IND195 |
| NICE asthma populations have no recent-treatment rule | 20 to 30 points lower | IND189, IND273 |
| Different age bands or windows | 3 to 6 points | IND112 (40+ vs 45+), IND82 to IND84 (15 vs 12 months) |
| QOF lipid indicators add CKD (and FH for DM035) and CHOL003 leaves out diabetes | 4 to 10 points higher | IND230, IND276 |

## Fixes made after benchmarking

| Measure | Change | 31 March 2026 |
|---|---|---|
| IND101 | Count a pulmonary rehabilitation offer made on the same day as the MRC score | 28.9% to 69.4% |
| IND139 | Use the NICE register (hypothyroidism treated with levothyroxine), not any hypothyroidism code | 75.4% to 88.7% |
| IND154, IND156 | Add the ex-smoker allowance: ex-smoker recorded in three consecutive financial years | +1.2 and +2.7 points |
| IND131, IND141, IND152, IND163, IND164 | On 31 March, report the flu season that ends that day | 31 March month-ends only |
| IND137 | Add the national screening attendance cluster (`CVDRETSCRN_COD`) | 31.6% to 41.8% |
| IND270 | Use recorded hypercholesterolaemia (`FNFHYP_COD`) instead of a total cholesterol above 5 mmol/L | 50.2% to 45.5% |

The retinal screening change also applies to the diabetes 9 care processes measure, whose retinal
process rises from about 23% to 30%.

## Choices kept

- **Alcohol brief intervention** (IND197, IND199, IND200, IND202) counts the national alcohol
  intervention and advice codes (`ALCOHOLINT_COD`). Both brief intervention and education codes
  follow negative screens as often as positive ones, so these are recording measures.
- **Falls** (IND208) counts being asked or assessed, not a recorded fall. It matches GPCS falls risk
  assessment.
- **Lithium** (IND87) keeps NICE's 4-month window; current guidance allows 6 months for stable
  patients.
- **Registers that follow QOF:** the asthma arm of the smoking measures uses the treated asthma
  register, AF measures exclude resolved AF, and SMI measures exclude remission. These match the QOF
  populations the indicators came from.

## Known gaps

- Our dementia register is about 4% larger than QOF and PCDD across many practices, with the same
  care plan numerator; no rule difference explains it.
- IND104 (depression review) does not reproduce QOF's population; under QOF's rules our denominator
  is about 25% larger.
- Flu risk-group uptake is published only for London; NCL comparisons are estimates.
- INLIQ figures after 2019/20 do not include NCL practices; those comparisons use London.
- Retinal screening (IND137) remains well below the screening programme's own uptake; many results
  do not reach GP records as codes.

## Results by indicator

Abbreviations: see "How we compared". "Rules aligned" means the comparison recomputed our measure
under the comparator's rules.

| ID | Indicator | Result | Confidence | Main comparator |
|---|---|---|---|---|
| IND78 | Contraception: advice for people taking anti-seizure medication | Loose comparator |  | Retired QOF EP003 (London) |
| IND79 | Learning disabilities: annual TSH test | Validated | Low | Retired QOF LD002B (INLIQ) (London) |
| IND80 | Dementia: target organ damage (new diagnoses) | Validated | Low | QOF DEM005 |
| IND81 | Diabetes: annual foot exam and risk classification | Validated | High | NDA foot surveillance |
| IND82 | Bipolar, schizophrenia and other psychoses: annual record of alcohol consumption | Validated | High | PHSMI PHS002 |
| IND83 | Bipolar, schizophrenia and other psychoses: annual BMI recording | Validated | High | PHSMI PHS006 |
| IND84 | Bipolar, schizophrenia and other psychoses: annual blood pressure | Validated | High | PHSMI PHS005 |
| IND85 | Bipolar, schizophrenia and other psychoses: cervical screening | Validated | Low | Retired QOF MH008 (INLIQ) (London) |
| IND86 | Bipolar, schizophrenia and other psychoses: target organ damage | Gap explained |  | Retired QOF MH009 (2018/19) |
| IND87 | Bipolar, schizophrenia and other psychoses: lithium levels in therapeutic range | Gap explained |  | NCD SMR36 (monitoring only); OpenSAFELY |
| IND88 | Diabetes: referral for structured education | Validated | Medium | QOF DM014 |
| IND89 | Diabetes: annual dietary review | Loose comparator |  | Retired QOF DM013 (2013/14) |
| IND91 | Osteoporosis: bone sparing agents (50-74 years) | Gap explained |  | Retired QOF OST002 (2018/19) |
| IND92 | Osteoporosis: bone sparing agents (75 years and over) | Gap explained |  | Retired QOF OST005 (2018/19) |
| IND94 | Peripheral arterial disease: antiplatelets | Validated | Low | Retired QOF PAD004 (INLIQ) |
| IND97 | Smoking: smoking status for people with long-term conditions | Validated | High | QOF SMOK002, rules aligned |
| IND98 | Smoking: support and treatment for people with long-term conditions or SMI | Validated | High | QOF SMOK005 |
| IND99 | Smoking: support and treatment (all patients) | Validated | High | CVDPREVENT CVDP002SMOK |
| IND101 | COPD: offered pulmonary rehabilitation | Validated | High | QOF COPD008, rules aligned |
| IND104 | Depression and anxiety: review within 10 to 35 days | Gap explained |  | QOF DEP004; retired DEP003 (London) |
| IND108 | Rheumatoid arthritis: cardiovascular risk assessment | Validated | Low | Retired QOF RA003 (INLIQ) (London) |
| IND110 | Rheumatoid arthritis: annual review | Validated | Medium | RA002 |
| IND111 | Diabetes: annual albumin creatinine test | Validated | High | NDA urine albumin |
| IND112 | Cardiovascular disease prevention: blood pressure measurement every 5 years | Validated | High | QOF BP002, rules aligned |
| IND113 | Cancer: 3-month review | Validated | High | QOF CAN004, rules aligned |
| IND115 | Hypertension: confirming diagnosis with HBPM or ABPM | Loose comparator |  | CPRD study; NICE pilot |
| IND116 | Contraception: advice for people with diabetes | Loose comparator |  | NICE pilot |
| IND117 | Contraception: advice for people with epilepsy | Loose comparator |  | Retired QOF EP003 (London) |
| IND118 | Dementia: target organ damage (all patients) | Loose comparator |  | Retired QOF DEM005 (2018/19) |
| IND120 | Diabetes: annual general practice checks | Validated | High | NDA eight care processes |
| IND121 | Hypertension: urinary albumin for target organ damage | Loose comparator |  | CVDPREVENT CVDP009HYP (all hypertension); NICE pilot |
| IND122 | Hypertension: haematuria for target organ damage | Loose comparator |  | NICE pilot |
| IND123 | Hypertension: ECG for target organ damage | Loose comparator |  | NICE pilot |
| IND124 | Contraception: advice for people with bipolar, schizophrenia or other psychoses | Loose comparator |  | NICE pilot |
| IND125 | Myocardial infarction: medication for MI in preceding 12 months | Loose comparator |  | MINAP (hospital discharge) |
| IND126 | Myocardial infarction: medication for MI more than 12 months ago | Loose comparator |  | Retired QOF CHD006 (2013/14) |
| IND127 | Atrial fibrillation: annual stroke risk assessment | Validated | High | QOF AF006 |
| IND128 | Atrial fibrillation: current treatment with anticoagulation | Validated | High | CVDPREVENT CVDP002AF |
| IND130 | Kidney conditions: CKD and renin–angiotensin system antagonists | Validated | Medium | CVDPREVENT CVDP005CKD |
| IND131 | Immunisation: flu vaccine for people with CHD | Validated | Low | UKHSA Chronic Heart Disease (London) |
| IND132 | Angina and coronary heart disease: anti-platelet or anticoagulation | Validated | High | QOF CHD005, rules aligned |
| IND133 | Stroke and ischaemic attack: anti-platelet or anticoagulation | Validated | High | QOF STIA007, rules aligned |
| IND134 | Diabetes: ACEi or ARBs | Validated | High | QOF DM006, rules aligned |
| IND135 | Diabetes: IFCC-HbA1c 64mmol/mol or less | Validated | High | QOF DM020, rules aligned |
| IND136 | Diabetes: IFCC-HbA1c 75mmol/mol or less | Validated | High | NDA HbA1c <=75 |
| IND137 | Diabetes: annual retinal screening | Gap explained |  | Retired QOF DM011 (London); NHS diabetic eye screening |
| IND139 | Hypothyroidism: annual thyroid function test | Validated | Low | Retired QOF THY002 (INLIQ) (London) |
| IND140 | COPD: FEV1 | No comparator |  |  |
| IND141 | Immunisation: flu vaccine for people with COPD | Validated | Low | UKHSA Chronic Respiratory Disease (London) |
| IND142 | Dementia: care planning | Validated | Medium | PCDD care plan |
| IND143 | Bipolar, schizophrenia and other psychoses: care planning | Validated | Medium | QOF MH002 |
| IND144 | Kidney conditions: CKD urine albumin:creatinine ratio | Validated | Medium | CVDPREVENT CVDP004CKD |
| IND148 | Contraception: LARC for people on oral or patch contraceptives | Loose comparator |  | Retired QOF CON002 (2018/19) |
| IND149 | Contraception: LARC for people using emergency contraception | Loose comparator |  | Retired QOF CON003 (2018/19) |
| IND150 | Cardiovascular disease prevention: cardiovascular risk assessment for people with bipolar, schizophrenia or other psychoses | Loose comparator |  | NICE pilot |
| IND152 | Immunisation: flu vaccine for people with long-term conditions | Validated | Medium | UKHSA total at risk (sub-ICB) |
| IND154 | Smoking: smoking status of people with bipolar, schizophrenia and other psychoses | Validated | High | PHSMI PHS007 |
| IND155 | Smoking: support and treatment for people with bipolar, schizophrenia and other psychoses | Loose comparator |  | QOF SMOK005 (wider population) |
| IND156 | Smoking: smoking status of people with long-term conditions | Validated | High | QOF SMOK002, rules aligned |
| IND157 | Smoking: support and treatment for people with long term conditions | Validated | High | QOF SMOK005 |
| IND158 | Bipolar, schizophrenia and other psychoses: annual cholesterol | Validated | High | QOF MH011, rules aligned |
| IND159 | Bipolar, schizophrenia and other psychoses: annual blood glucose or HbA1c | Validated | High | PHSMI PHS003 |
| IND160 | Diabetes: annual examination of foot sensation | Gap explained |  | QOF DM012; NDA (different numerator) |
| IND161 | Cardiovascular disease prevention: cardiovascular risk assessment for people newly diagnosed with hypertension or T2DM | Loose comparator |  | NCD CVD-07 (related cohort) |
| IND163 | Immunisation: flu vaccine for people with diabetes | Validated | Low | UKHSA Diabetes (London) |
| IND164 | Immunisation: flu vaccine for people with stroke or TIA | Validated | Low | UKHSA Chronic Neurological Disease (London) |
| IND165 | Diabetes: IFCC-HbA1c 58mmol/mol or less | Validated | High | NDA HbA1c <=58 |
| IND169 | Atrial fibrillation: review of anticoagulation | Loose comparator |  | NICE pilot |
| IND171 | Diabetes: NDH diabetes prevention programme | Gap explained |  | NDA diabetes prevention offers (all NDH) |
| IND172 | Diabetes: NDH annual HbA1c or FPG test | Validated | High | NDA NDH HbA1c/FPG |
| IND173 | Diabetes: gestational diabetes annual HbA1c test | Loose comparator |  | CPRD studies |
| IND176 | Screening: cervical screening (25 to 49 years) | Validated | High | QOF CS005, rules aligned |
| IND177 | Screening: cervical screening (50 to 64 years) | Validated | High | QOF CS006, rules aligned |
| IND178 | Pregnancy and neonates: postnatal mental health | Loose comparator |  | NICE pilot |
| IND179 | Diabetes: HbA1c 58 mmol/mol | Validated | Medium | QOF DM020 |
| IND180 | Diabetes: HbA1c 75 mmol/mol | Validated | Medium | QOF DM021 |
| IND181 | Diabetes: CVD risk assessment | Loose comparator |  | NICE CPRD testing report (related group) |
| IND189 | Asthma: smoking status (under 19) | Validated | High | QOF AST008, rules aligned |
| IND191 | COPD: annual review | Validated | Low | QOF COPD010 |
| IND192 | Heart failure: confirmation of diagnosis | Validated | Low | QOF HF008 |
| IND195 | Heart failure: annual review | Validated | High | QOF HF007, rules aligned |
| IND196 | Alcohol use: risk assessment for people with hypertension | Loose comparator |  | NICE pilot; CPRD studies |
| IND197 | Alcohol use: brief intervention for people with hypertension | Loose comparator |  | NICE pilot; CPRD studies |
| IND198 | Alcohol use: risk assessment for people with depression or anxiety | Loose comparator |  | NICE pilot; CPRD studies |
| IND199 | Alcohol use: brief intervention for people with depression or anxiety | Loose comparator |  | NICE pilot; CPRD studies |
| IND200 | Alcohol use: brief intervention for people with SMI | Loose comparator |  | NICE pilot; CPRD studies |
| IND201 | Alcohol use: risk assessment for people with a long-term condition | Loose comparator |  | NICE pilot; CPRD studies |
| IND202 | Alcohol use: brief intervention for people with a long-term condition | Loose comparator |  | NICE pilot; CPRD studies |
| IND207 | Multiple long-term conditions: medication review | Validated | High | Network Contract DES structured medication reviews |
| IND208 | Multiple long-term conditions: asking about falls | Validated | High | GP Contract Services falls risk assessment |
| IND210 | HIV: testing at registration | Loose comparator |  | NICE pilot; UKHSA |
| IND212 | COPD: oxygen saturation recording | No comparator |  |  |
| IND213 | Bipolar, schizophrenia and other psychoses: cervical screening (25 to 49 years) | Loose comparator |  | Retired QOF MH008 (London, all ages) |
| IND214 | Bipolar, schizophrenia and other psychoses: cervical screening (50 to 64 years) | Loose comparator |  | Retired QOF MH008 (London, all ages) |
| IND215 | Immunisation: DTaP (8 months) | Validated | High | COVER DTaP |
| IND216 | Immunisation: MMR (18 months) | Validated | High | COVER MMR1 |
| IND217 | Immunisation: DTaP/IPV and MMR (5 years) | Validated | High | QOF VI003 |
| IND218 | Immunisation: MMR (5 years) | Validated | Medium | COVER MMR1 |
| IND219 | Immunisation: shingles | Validated | High | QOF VI004, rules aligned |
| IND220 | Weight management: referral to weight management programmes for obesity | Loose comparator |  | NHS Payments weight management service |
| IND221 | Weight management: referral to weight management programmes for obesity (co-existing hypertension or diabetes) | Loose comparator |  | NHS Payments weight management service |
| IND222 | Cancer: review within 3 months | Validated | Medium | QOF CAN005 |
| IND223 | Cancer: review within 12 months | Validated | High | QOF CAN004, rules aligned |
| IND224 | Immunisation: rotavirus (24 weeks) | Validated | High | COVER Rotavirus |
| IND225 | Immunisation: meningitis B (8 months) | Validated | High | COVER MenB |
| IND226 | Immunisation: meningitis B (18 months) | Validated | Medium | COVER MenB booster |
| IND229 | Cardiovascular disease prevention: primary prevention with lipid lowering therapies | Validated | High | CVDPREVENT CVDP006CHOL |
| IND230 | Cardiovascular disease prevention: secondary prevention with lipid lowering therapies | Validated | High | CVDPREVENT CVDP009CHOL |
| IND231 | Kidney conditions: CKD and lipid lowering therapies | Validated | High | CVDPREVENT CVDP010CHOL |
| IND232 | Kidney conditions: eGFR for long-term NSAID use | No comparator |  |  |
| IND233 | Kidney conditions: CKD and eGFR | Loose comparator |  | CVDPREVENT CVDP006CKD |
| IND234 | Kidney conditions: CKD – eGFR and ACR | Loose comparator |  | CVDPREVENT CVDP004CKD |
| IND235 | Kidney conditions: CKD and blood pressure when ACR less than 70 | Validated | High | CVDPREVENT CVDP007CKD |
| IND239 | Hypertension: blood pressure (79 years and under) | Validated | High | CVDPREVENT CVDP002HYP |
| IND240 | Hypertension: blood pressure (80 years and over) | Validated | High | CVDPREVENT CVDP003HYP |
| IND241 | Angina and coronary heart disease: blood pressure (79 years and under) | Validated | High | QOF CHD015, rules aligned |
| IND242 | Angina and coronary heart disease: blood pressure (80 years and over) | Validated | Medium | CVDPREVENT CVDP002CHD age 80+ (sub-ICB) |
| IND243 | Stroke and ischaemic attack: blood pressure (79 years and under) | Validated | High | QOF STIA014, rules aligned |
| IND244 | Stroke and ischaemic attack: blood pressure (80 years and over) | Validated | Medium | CVDPREVENT CVDP002STRK age 80+ (sub-ICB) |
| IND245 | Peripheral arterial disease: blood pressure (79 years and under) | Loose comparator |  | Retired QOF PAD002 (London, all ages) |
| IND246 | Peripheral arterial disease: blood pressure (80 years and over) | Loose comparator |  | Retired QOF PAD002 (London, all ages) |
| IND247 | Atrial fibrillation: DOACs and Vitamin K antagonists | Validated | High | CVDPREVENT CVDP005AF |
| IND248 | Bipolar, schizophrenia and other psychoses: 6 physical health checks | Validated | High | QOF MH021 |
| IND249 | Diabetes: blood pressure (without moderate or severe frailty) | Validated | High | NDA BP <=140/90 |
| IND260 | Lipid disorders: FH assessment and diagnosis (historical readings) | Validated | Low | NCD CVD-04 |
| IND261 | Lipid disorders: FH assessment and diagnosis (new readings) | Loose comparator |  | NCD CVD-04; CVDPREVENT CVDP004FH |
| IND263 | Kidney conditions: CKD - ACEi and ARB | Loose comparator |  | CVDPREVENT CVDP005CKD |
| IND264 | Kidney conditions: CKD and blood pressure when ACR 70 or more | Loose comparator |  | Retired QOF CKD002 |
| IND265 | Learning disabilities: health checks and action plans | Validated | Medium | LDHC check + plan |
| IND266 | Learning disabilities: health checks, action plans and ethnicity | Validated | Medium | NCD HI03 |
| IND267 | Cancer: faecal immunochemical testing | Validated | Medium | IIF CAN-04 |
| IND269 | Cardiovascular disease prevention: risk assessment (general population) | Validated | High | NCD CVD-07 |
| IND270 | Cardiovascular disease prevention: risk assessment (modifiable risk factors) | Validated | Low | NICE CPRD IND2023-166 (England) |
| IND272 | Asthma: objective tests | Validated | Medium | QOF AST012, rules aligned |
| IND273 | Asthma: annual review | Validated | High | QOF AST007, rules aligned |
| IND274 | Diabetes: lipid-lowering therapies for primary prevention of CVD (T2DM and 10% risk) | Loose comparator |  | NDA primary-prevention statins |
| IND275 | Diabetes: lipid-lowering therapies for primary prevention of CVD (40 years and over) | Validated | High | NDA primary-prevention statins |
| IND276 | Diabetes: lipid-lowering therapies for secondary prevention of CVD | Validated | High | QOF DM035/DM023, rules aligned |
| IND277 | Diabetes: T1DM and lipid-lowering therapies | Validated | Medium | NDA T1 primary-prevention statins (NCL ICB) |
| IND278 | Cardiovascular disease prevention: cholesterol treatment target (secondary prevention) | Validated | High | CVDPREVENT CVDP012CHOL |
| IND287 | Cardiovascular disease prevention: lipid lowering therapy for people newly diagnosed with hypertension or T2DM | Loose comparator |  | NCD CVD-13 (related cohort) |
| IND315 | Asthma: annual review (higher risk patients) | Validated | Medium | QOF AST007 |
| IND316 | Asthma: MART (higher risk patients) | Validated | Low | NICE CPRD asthma report (England) |
| IND317 | Heart failure: 4 pillars (HFrEF) | Validated | Low | CVDPREVENT CVDP003HF |
| IND318 | Heart failure: ejection fraction category | Loose comparator |  | NICE CPRD report |
| IND319 | Weight management: advice for people living with overweight (18 to 39 years) | Loose comparator |  | Published studies |
| IND320 | Weight management: BMI recording (long term conditions) | Validated | Medium | NDA BMI |
| IND321 | Screening: cervical (25 to 64 years) | Validated | High | QOF CS005+CS006, rules aligned |
| IND324 | Kidney conditions: CKD and SGLT2 inhibitors | Validated | Low | CVDPREVENT CVDP008CKD |
