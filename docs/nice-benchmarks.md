# NICE indicator benchmarks

How our 149 NICE indicator measures compare with published figures for North Central London (NCL)
practices, and why they differ where they do. Rates are as built at the period shown, from the
monthly history (31 March 2022 to 2026) and the current build. Indicator definitions are in
[nice-indicators.md](nice-indicators.md).

## Summary

| Alignment | Measures | Meaning |
|---|---:|---|
| Matches | 45 | Within 3 percentage points of a published figure with the same or close definition |
| Gap explained and reproduced | 49 | Further apart as built; recomputing ours under the comparator's rules brings it within 3 points |
| Gap explained, not reproduced | 7 | A known difference in definition or recording, but the published data do not allow a recomputation |
| Related figure only | 45 | Retired indicators outside our history, NICE pilots or published studies |
| No comparator | 3 | Nothing published to compare with |

QOF-equivalent registers match QOF prevalence. IND185, IND205, IND206 and IND256 follow NICE's
own definitions: IND185 (including resolved atrial fibrillation) is about 105% of the QOF AF
register and IND256 (adding people on lithium) about 101% of the QOF SMI register. IND205 and
IND206 have no QOF equivalent.

## How we compared

- **Sources:** QOF 2021/22 to 2025/26 and indicators no longer in QOF (INLIQ), CVDPREVENT,
  National Diabetes Audit (NDA), physical health checks for people with severe mental illness
  (PHSMI), Learning Disabilities Health Check Scheme (LDHC), Health and Care of People with
  Learning Disabilities (HCLD), Primary Care Dementia Data (PCDD), childhood vaccination coverage
  (COVER), UKHSA flu and shingles uptake, cervical screening coverage, GP Contract Services (GPCS),
  Network Contract DES and Investment and Impact Fund (NCD, IIF), and NICE's own indicator testing
  reports.
- **Level:** `practice` means published practice figures were joined to ours by practice code and
  pooled over the practices in both; we also checked the spread across practices, PCNs and
  boroughs. `NCL`, `London` and `England` mean only an area figure was available; London and
  England figures are a regional check of our NCL rate.
- **Exceptions:** QOF and similar sources remove personalised care adjustments or exceptions; we do
  not. We compare with their gross rate: numerator divided by (denominator plus exceptions).
- **Rules aligned:** where definitions differ in a known way (thresholds, windows, age bands,
  registers), we recomputed our measure under the comparator's rules. That gap is shown separately.

## Why rates differ from published figures

These differences come from following NICE's wording rather than the comparator's rules.

| Difference | Typical effect | Examples |
|---|---|---|
| NICE blood pressure targets are strict (below 140/90); QOF and CVDPREVENT use 140/90 or less | 6 to 8 points lower | IND239, IND241, IND243, IND249 |
| NICE excludes people with a contraindication; QOF's gross rate keeps them | 4 to 10 points higher | IND132, IND133, IND134 |
| NICE requires a separate NYHA functional assessment code | about 50 points lower | IND195 |
| NICE asthma populations have no recent-treatment rule | 20 to 30 points lower | IND189, IND273 |
| NICE uses 15-month windows where QOF uses 12 | 3 to 4 points higher | IND82, IND83, IND84 |
| Different age bands | 2 to 10 points | IND112 (40+ vs 45+), IND265 (all ages vs 14+) |
| QOF lipid indicators add CKD (and FH for DM035); CHOL003 leaves out diabetes | 4 to 10 points higher | IND230, IND276 |

## Where our definitions are deliberate

- **Alcohol brief intervention** (IND197, IND199, IND200, IND202) counts the national alcohol
  intervention and advice codes (`ALCOHOLINT_COD`). Both brief intervention and education codes
  follow negative screening results as often as positive ones, so these measure recording rather
  than targeted intervention.
- **Falls** (IND208) counts being asked or assessed, not a recorded fall; it matches GPCS falls
  risk assessment.
- **Lithium** (IND87) keeps NICE's 4-month window; current guidance allows 6 months for stable
  patients.
- **Registers that follow QOF:** the asthma arm of the smoking measures uses the treated asthma
  register, AF measures exclude resolved AF, and SMI measures exclude remission, as in the QOF
  populations these indicators came from.

## Known discrepancies

- **Dementia:** our register is about 4% larger than QOF and PCDD across many practices, with the
  same care plan numerator; no rule difference explains it. IND142 sits about 2.7 points below PCDD.
- **IND104 (depression review):** under QOF's rules our pooled rate is close, but our denominator is
  about 25% larger; the episode selection differs in a way we have not reproduced.
- **IND191 (COPD review):** matches overall, but Enfield practices sit 5 to 9 points below QOF.
- **IND324 (CKD and SGLT2 inhibitors):** CVDPREVENT's eligibility follows NICE TA775; the closest
  variant we can compute leaves a 3.6-point gap and wide practice variation.
- **IND137 (retinal screening):** remains well below the screening programme's uptake; many results
  do not reach GP records as codes.
- **Flu risk groups** are published only for London, and **INLIQ** after 2019/20 has no NCL
  practices, so those comparisons are regional.

## Results by indicator

Rates and gaps are percentages and percentage points; gap is ours minus the comparator. "Rules
aligned" is the gap after recomputing ours under the comparator's rules. Blank figures mean no
like-for-like value.

| ID | Indicator | Comparator | Level | Period | Ours | Comparator | Gap | Rules aligned | Alignment | Main difference |
|---|---|---|---|---|---:|---:|---:|---:|---|---|
| IND78 | Contraception: advice for people taking anti-seizure medication | Retired QOF EP003 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND79 | Learning disabilities: annual TSH test | Retired QOF LD002B (INLIQ) | London | Mar 2024 | 54.8 | 52.6 | 2.2 | -5.6 | Matches | 15 vs 12-month TSH window |
| IND80 | Dementia: target organ damage (new diagnoses) | Retired QOF DEM005 | NCL | 2018/19 |  |  |  |  | Related figure only | Comparator predates our history |
| IND81 | Diabetes: annual foot exam and risk classification | NDA foot surveillance | practice | Mar 2026 | 82.1 | 77.4 | 4.8 | 1.1 | Gap explained and reproduced | 15 vs 12-month foot window |
| IND82 | Bipolar, schizophrenia and other psychoses: annual record of alcohol consumption | PHSMI PHS002 | practice | Jun 2026 | 85.6 | 78.0 | 7.6 | -0.2 | Gap explained and reproduced | 15 vs 12-month alcohol window |
| IND83 | Bipolar, schizophrenia and other psychoses: annual BMI recording | PHSMI PHS006 | practice | Jun 2026 | 84.1 | 78.3 | 5.8 | -1.1 | Gap explained and reproduced | 15 vs 12-month BMI window |
| IND84 | Bipolar, schizophrenia and other psychoses: annual blood pressure | PHSMI PHS005 | practice | Jun 2026 | 84.4 | 78.2 | 6.2 | -0.6 | Gap explained and reproduced | 15 vs 12-month BP window |
| IND85 | Bipolar, schizophrenia and other psychoses: cervical screening | Retired QOF MH008 (INLIQ) | London | Mar 2023 | 64.4 | 66.3 | -1.9 |  | Matches | Register extraction and screening evidence differ |
| IND86 | Bipolar, schizophrenia and other psychoses: target organ damage | Retired QOF MH009 | NCL | 2018/19 |  |  |  |  | Gap explained, not reproduced |  |
| IND87 | Bipolar, schizophrenia and other psychoses: lithium levels in therapeutic range | NCD lithium monitoring (SMR36) | practice | Mar 2025 |  |  |  |  | Gap explained, not reproduced |  |
| IND88 | Diabetes: referral for structured education | QOF DM014 | practice | 2025/26 | 67.7 | 66.9 | 0.8 |  | Matches |  |
| IND89 | Diabetes: annual dietary review | Retired QOF DM013 | NCL | 2013/14 |  |  |  |  | Related figure only |  |
| IND91 | Osteoporosis: bone sparing agents (50-74 years) | Retired QOF OST002 | NCL | 2018/19 |  |  |  |  | Gap explained, not reproduced |  |
| IND92 | Osteoporosis: bone sparing agents (75 years and over) | Retired QOF OST005 | NCL | 2018/19 |  |  |  |  | Gap explained, not reproduced |  |
| IND94 | Peripheral arterial disease: antiplatelets | Retired QOF PAD004 (INLIQ) | practice | Mar 2021 / Mar 2022 | 73.8 | 73.3 | 0.5 |  | Matches |  |
| IND97 | Smoking: smoking status for people with long-term conditions | QOF SMOK002 | practice | 2025/26 | 88.9 | 93.0 | -4.1 | 0.5 | Gap explained and reproduced | QOF adds SMI and the three-year ex-smoker allowance |
| IND98 | Smoking: support and treatment for people with long-term conditions or SMI | CVDPREVENT CVDP002SMOK | practice | Mar 2026 | 80.1 | 77.3 | 2.8 | 3.4 | Matches | Smoking population and recording windows differ |
| IND99 | Smoking: support and treatment (all patients) | CVDPREVENT CVDP002SMOK | practice | Mar 2026 | 93.4 | 77.3 | 16.1 | -0.3 | Gap explained and reproduced | Smoking population and recording windows differ |
| IND101 | COPD: offered pulmonary rehabilitation | QOF COPD008 | practice | 2022/23 | 90.1 | 56.8 | 33.3 | 0.4 | Gap explained and reproduced | NICE offers/declines vs QOF referral; MRC window differs |
| IND104 | Depression and anxiety: review within 10 to 35 days | QOF DEP004 | practice | 2025/26 | 18.8 | 22.0 | -3.3 | -1.6 | Gap explained, not reproduced | 10-35 vs 10-56 days; episode cohort differs |
| IND108 | Rheumatoid arthritis: cardiovascular risk assessment | Retired QOF RA003 (INLIQ) | London | Mar 2023 | 31.1 | 30.3 | 0.8 | -2.8 | Matches |  |
| IND110 | Rheumatoid arthritis: annual review | QOF RA002 | practice | 2022/23 | 86.2 | 85.3 | 0.9 |  | Matches |  |
| IND111 | Diabetes: annual albumin creatinine test | NDA urine albumin | practice | Mar 2026 | 76.6 | 69.9 | 6.7 | 0.8 | Gap explained and reproduced | 15 vs 12-month ACR window |
| IND112 | Cardiovascular disease prevention: blood pressure measurement every 5 years | QOF BP002 | practice | 2025/26 | 79.4 | 85.2 | -5.8 | -0.4 | Gap explained and reproduced | Age 40+ vs 45+ |
| IND113 | Cancer: 3-month review | QOF CAN004 | practice | 2025/26 | 38.9 | 71.6 | -32.7 | 0.1 | Gap explained and reproduced | 3 vs 12-month review deadline; diagnosis cohort differs |
| IND115 | Hypertension: confirming diagnosis with HBPM or ABPM | NICE pilot / CPRD ABPM-HBPM | England | 2016/17 |  |  |  |  | Related figure only |  |
| IND116 | Contraception: advice for people with diabetes | NICE contraception pilot | England | 2013/14 |  |  |  |  | Related figure only |  |
| IND117 | Contraception: advice for people with epilepsy | Retired QOF EP003 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND118 | Dementia: target organ damage (all patients) | Retired QOF DEM005 | NCL | 2018/19 |  |  |  |  | Related figure only |  |
| IND120 | Diabetes: annual general practice checks | NDA eight care processes | practice | Mar 2026 | 58.6 | 59.1 | -0.5 |  | Matches |  |
| IND121 | Hypertension: urinary albumin for target organ damage | CVDPREVENT CVDP009HYP | practice | Mar 2026 |  |  |  |  | Related figure only |  |
| IND122 | Hypertension: haematuria for target organ damage | NICE pilot | England | c. 2015 |  |  |  |  | Related figure only |  |
| IND123 | Hypertension: ECG for target organ damage | NICE pilot | England | c. 2015 |  |  |  |  | Related figure only |  |
| IND124 | Contraception: advice for people with bipolar, schizophrenia or other psychoses | NICE contraception pilot | England | 2013/14 |  |  |  |  | Related figure only |  |
| IND125 | Myocardial infarction: medication for MI in preceding 12 months | MINAP secondary-prevention discharge drugs | England | 2022/23 |  |  |  |  | Related figure only |  |
| IND126 | Myocardial infarction: medication for MI more than 12 months ago | Retired QOF CHD006 | NCL | 2013/14 |  |  |  |  | Related figure only |  |
| IND127 | Atrial fibrillation: annual stroke risk assessment | QOF AF006 | practice | 2025/26 | 94.5 | 94.6 | -0.1 |  | Matches |  |
| IND128 | Atrial fibrillation: current treatment with anticoagulation | CVDPREVENT CVDP002AF | practice | Mar 2026 | 90.9 | 89.6 | 1.3 |  | Matches | NICE excludes anticoagulant contraindications |
| IND130 | Kidney conditions: CKD and renin–angiotensin system antagonists | CVDPREVENT CVDP005CKD | practice | Mar 2026 | 77.7 | 72.9 | 4.8 | 0.0 | Gap explained and reproduced | NICE excludes contraindications |
| IND131 | Immunisation: flu vaccine for people with CHD | UKHSA flu chronic heart disease (NCL estimate) | London | 2024/25 | 61.1 | 31.5 | 29.6 | 0.9 | Gap explained and reproduced | All ages vs under-65 risk group; London-derived estimate |
| IND132 | Angina and coronary heart disease: anti-platelet or anticoagulation | QOF CHD005 | practice | 2025/26 | 93.2 | 89.2 | 4.0 | -0.2 | Gap explained and reproduced | NICE excludes contraindications |
| IND133 | Stroke and ischaemic attack: anti-platelet or anticoagulation | QOF STIA007 | practice | 2025/26 | 93.0 | 88.2 | 4.8 | -0.3 | Gap explained and reproduced | NICE excludes contraindications |
| IND134 | Diabetes: ACEi or ARBs | QOF DM006 | practice | 2025/26 | 88.5 | 78.2 | 10.4 | -0.1 | Gap explained and reproduced | NICE excludes contraindications |
| IND135 | Diabetes: IFCC-HbA1c 64mmol/mol or less | QOF DM020 | practice | 2025/26 | 71.0 | 59.5 | 11.5 | 0.1 | Gap explained and reproduced | 64 vs 58 mmol/mol target; QOF excludes moderate or severe frailty |
| IND136 | Diabetes: IFCC-HbA1c 75mmol/mol or less | NDA HbA1c <=75 | practice | Mar 2026 | 80.1 | 86.6 | -6.5 | -0.1 | Gap explained and reproduced | Whole register vs valid-HbA1c denominator |
| IND137 | Diabetes: annual retinal screening | Retired QOF DM011 (INLIQ) | London | Mar 2023 | 48.1 | 66.8 | -18.7 |  | Gap explained, not reproduced | Different retinal screening code scope |
| IND139 | Hypothyroidism: annual thyroid function test | Retired QOF THY002 (INLIQ) | London | Mar 2024 | 88.1 | 88.2 | -0.1 |  | Matches |  |
| IND140 | COPD: FEV1 |  |  |  |  |  |  |  | No comparator |  |
| IND141 | Immunisation: flu vaccine for people with COPD | UKHSA flu chronic respiratory disease (NCL estimate) | London | 2024/25 | 59.8 | 33.9 | 25.9 | 0.9 | Gap explained and reproduced | All ages vs under-65 risk group; London-derived estimate |
| IND142 | Dementia: care planning | PCDD care plan | practice | Mar 2026 | 73.4 | 76.0 | -2.6 |  | Matches | Larger NICE dementia register; cause unresolved |
| IND143 | Bipolar, schizophrenia and other psychoses: care planning | QOF MH002 | practice | 2025/26 | 75.9 | 77.5 | -1.6 |  | Matches | Care-plan coding and gross PCA population differ |
| IND144 | Kidney conditions: CKD urine albumin:creatinine ratio | CVDPREVENT CVDP004CKD | practice | Mar 2026 | 65.6 | 64.6 | 1.0 |  | Matches |  |
| IND148 | Contraception: LARC for people on oral or patch contraceptives | Retired QOF CON002 (INLIQ) | NCL | 2018/19 |  |  |  |  | Related figure only |  |
| IND149 | Contraception: LARC for people using emergency contraception | Retired QOF CON003 | NCL | 2018/19 |  |  |  |  | Related figure only |  |
| IND150 | Cardiovascular disease prevention: cardiovascular risk assessment for people with bipolar, schizophrenia or other psychoses | NICE SMI risk-assessment pilot | England | 2014/15 |  |  |  |  | Related figure only |  |
| IND152 | Immunisation: flu vaccine for people with long-term conditions | UKHSA flu total at risk under 65 | NCL | 2025/26 | 54.2 | 31.7 | 22.5 | 1.4 | Gap explained and reproduced | Four NICE registers, all ages vs all at-risk under-65s |
| IND154 | Smoking: smoking status of people with bipolar, schizophrenia and other psychoses | PHSMI PHS007 | practice | Mar 2026 | 90.8 | 83.9 | 6.9 | -0.4 | Gap explained and reproduced | NICE accepts historical never-smoker and ex-smoker routes |
| IND155 | Smoking: support and treatment for people with bipolar, schizophrenia and other psychoses | QOF SMOK005 (LTC population) | practice | 2025/26 |  |  |  |  | Related figure only |  |
| IND156 | Smoking: smoking status of people with long-term conditions | QOF SMOK002 | practice | 2025/26 | 91.7 | 93.0 | -1.3 |  | Matches |  |
| IND157 | Smoking: support and treatment for people with long term conditions | QOF SMOK005 | practice | 2025/26 | 80.8 | 80.4 | 0.4 |  | Matches |  |
| IND158 | Bipolar, schizophrenia and other psychoses: annual cholesterol | PHSMI PHS004 | practice | Jun 2026 | 64.6 | 70.4 | -5.8 | -1.7 | Gap explained and reproduced | TC:HDL ratio vs any lipid test; CVD excluded |
| IND159 | Bipolar, schizophrenia and other psychoses: annual blood glucose or HbA1c | PHSMI PHS003 | practice | Jun 2026 | 68.2 | 70.8 | -2.6 | 0.4 | Matches | NICE excludes established diabetes and under-18s |
| IND160 | Diabetes: annual examination of foot sensation | QOF DM012 | practice | 2025/26 | 64.8 | 78.7 | -13.9 | 0.0 | Gap explained and reproduced | Monofilament testing vs foot-risk classification |
| IND161 | Cardiovascular disease prevention: cardiovascular risk assessment for people newly diagnosed with hypertension or T2DM | NCD CVD-07 | practice | Mar 2025 |  |  |  |  | Related figure only |  |
| IND163 | Immunisation: flu vaccine for people with diabetes | UKHSA flu diabetes (NCL estimate) | London | 2024/25 | 53.0 | 41.2 | 11.8 | -0.9 | Gap explained and reproduced | All ages vs under-65 risk group; London-derived estimate |
| IND164 | Immunisation: flu vaccine for people with stroke or TIA | UKHSA flu chronic neurological disease (NCL estimate) | London | 2024/25 | 57.9 | 32.9 | 25.0 | 1.7 | Gap explained and reproduced | All ages vs under-65 risk group; London-derived estimate |
| IND165 | Diabetes: IFCC-HbA1c 58mmol/mol or less | NDA HbA1c <=58 | practice | Mar 2026 | 62.7 | 65.8 | -3.0 | -0.3 | Matches | Whole register vs valid-HbA1c denominator |
| IND169 | Atrial fibrillation: review of anticoagulation | NICE NCCID anticoagulation pilot | England | c. 2016 |  |  |  |  | Related figure only |  |
| IND171 | Diabetes: NDH diabetes prevention programme | NDA NDH cumulative DPP offers | practice | Mar 2026 | 36.3 | 63.6 | -27.3 | 2.1 | Gap explained and reproduced | New-NDH referrals vs all-NDH offers ever, including invitations |
| IND172 | Diabetes: NDH annual HbA1c or FPG test | NDA NDH HbA1c/FPG | practice | Mar 2026 | 83.7 | 83.3 | 0.3 |  | Matches |  |
| IND173 | Diabetes: gestational diabetes annual HbA1c test | CPRD-HES annual testing after GDM | England | 2000-18 |  |  |  |  | Related figure only |  |
| IND176 | Screening: cervical screening (25 to 49 years) | NHSE cervical coverage 25-49 | practice | Mar 2024 | 67.7 | 58.3 | 9.4 | 1.9 | Gap explained and reproduced | NICE excludes pregnancy and invitation non-response |
| IND177 | Screening: cervical screening (50 to 64 years) | QOF CS006 | practice | 2025/26 | 78.3 | 73.7 | 4.5 | 0.4 | Gap explained and reproduced | NICE excludes non-responders; QOF gross rate keeps them |
| IND178 | Pregnancy and neonates: postnatal mental health | NICE postnatal mental-health pilot | England |  |  |  |  |  | Related figure only |  |
| IND179 | Diabetes: HbA1c 58 mmol/mol | QOF DM020 | practice | 2025/26 | 63.0 | 59.5 | 3.5 | 0.1 | Gap explained and reproduced | NICE excludes maximum-treatment and fructosamine PCAs |
| IND180 | Diabetes: HbA1c 75 mmol/mol | QOF DM021 | practice | 2025/26 | 86.8 | 83.1 | 3.7 | 0.1 | Gap explained and reproduced | NICE excludes maximum-treatment and fructosamine PCAs |
| IND181 | Diabetes: CVD risk assessment | NICE testing report comorbidity group | England | Mar 2023 |  |  |  |  | Related figure only |  |
| IND189 | Asthma: smoking status (under 19) | QOF AST008 | practice | 2025/26 | 47.1 | 70.0 | -23.0 | -0.1 | Gap explained and reproduced | NICE has no recent asthma-treatment restriction |
| IND191 | COPD: annual review | QOF COPD010 | practice | 2025/26 | 77.4 | 80.3 | -2.9 |  | Matches | Review coding or register extraction; cause unresolved |
| IND192 | Heart failure: confirmation of diagnosis | QOF HF008 | practice | 2025/26 | 84.6 | 86.6 | -2.1 |  | Matches | Whole HF register vs new-diagnosis cohort |
| IND195 | Heart failure: annual review | QOF HF007 | practice | 2025/26 | 35.0 | 84.7 | -49.6 | -0.6 | Gap explained and reproduced | NICE requires a separate NYHA assessment |
| IND196 | Alcohol use: risk assessment for people with hypertension | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND197 | Alcohol use: brief intervention for people with hypertension | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND198 | Alcohol use: risk assessment for people with depression or anxiety | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND199 | Alcohol use: brief intervention for people with depression or anxiety | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND200 | Alcohol use: brief intervention for people with SMI | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND201 | Alcohol use: risk assessment for people with a long-term condition | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND202 | Alcohol use: brief intervention for people with a long-term condition | Retired QOF CVD-PP002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND207 | Multiple long-term conditions: medication review | NCD structured medication reviews | practice | Mar 2026 | 14.8 | 21.2 | -6.4 | 0.0 | Gap explained and reproduced | NICE multimorbidity vs NCD severe-frailty cohort |
| IND208 | Multiple long-term conditions: asking about falls | GPCS falls risk assessment | practice | Mar 2026 | 1.4 | 5.2 | -3.8 | 0.1 | Gap explained and reproduced | NICE non-frail cohort vs severe frailty coded in year |
| IND210 | HIV: testing at registration | UKHSA HIV testing report | England | 2024 |  |  |  |  | Related figure only |  |
| IND212 | COPD: oxygen saturation recording |  |  |  |  |  |  |  | No comparator |  |
| IND213 | Bipolar, schizophrenia and other psychoses: cervical screening (25 to 49 years) | Retired QOF MH008 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND214 | Bipolar, schizophrenia and other psychoses: cervical screening (50 to 64 years) | Retired QOF MH008 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND215 | Immunisation: DTaP (8 months) | COVER DTaP 12 months | practice | Mar 2026 | 82.4 | 84.9 | -2.5 | 0.4 | Matches | NICE vaccination by 8 vs COVER 12 months |
| IND216 | Immunisation: MMR (18 months) | COVER MMR1 24 months | practice | Mar 2026 | 73.4 | 76.8 | -3.4 | 0.9 | Gap explained and reproduced | NICE vaccination by 18 vs COVER 24 months |
| IND217 | Immunisation: DTaP/IPV and MMR (5 years) | QOF VI003 | practice | 2025/26 | 68.8 | 69.0 | -0.2 |  | Matches |  |
| IND218 | Immunisation: MMR (5 years) | COVER MMR1 5 years | practice | Mar 2026 | 85.5 | 84.5 | 1.0 |  | Matches |  |
| IND219 | Immunisation: shingles | UKHSA shingles age-75 cohort | NCL | 2023/24 (doses to Sep 2023) | 64.1 | 64.2 | -0.1 | 0.3 | Matches |  |
| IND220 | Weight management: referral to weight management programmes for obesity | NHS GP weight-management payments | NCL | 2024/25 |  |  |  |  | Related figure only |  |
| IND221 | Weight management: referral to weight management programmes for obesity (co-existing hypertension or diabetes) | NHS GP weight-management payments | NCL | 2024/25 |  |  |  |  | Related figure only |  |
| IND222 | Cancer: review within 3 months | QOF CAN005 | practice | 2025/26 | 39.5 | 36.9 | 2.6 |  | Matches | Closed NICE cohort vs QOF financial-year cohort |
| IND223 | Cancer: review within 12 months | QOF CAN004 | practice | 2025/26 | 83.6 | 71.6 | 11.9 | 0.1 | Gap explained and reproduced | Lagged NICE cohort vs QOF financial-year exclusions |
| IND224 | Immunisation: rotavirus (24 weeks) | COVER rotavirus 12 months | practice | Mar 2026 | 83.4 | 83.0 | 0.5 | 0.3 | Matches |  |
| IND225 | Immunisation: meningitis B (8 months) | COVER MenB 12 months | practice | Mar 2026 | 83.6 | 84.8 | -1.2 | 0.3 | Matches | NICE vaccination by 8 vs COVER 12 months |
| IND226 | Immunisation: meningitis B (18 months) | COVER MenB booster 24 months | practice | Mar 2026 | 70.4 | 75.5 | -5.1 | 1.2 | Gap explained and reproduced | NICE booster by 18 vs COVER 24 months |
| IND229 | Cardiovascular disease prevention: primary prevention with lipid lowering therapies | CVDPREVENT CVDP006CHOL | practice | Mar 2026 | 56.1 | 56.2 | -0.1 |  | Matches |  |
| IND230 | Cardiovascular disease prevention: secondary prevention with lipid lowering therapies | CVDPREVENT CVDP009CHOL | practice | Mar 2026 | 85.9 | 86.1 | -0.2 |  | Matches |  |
| IND231 | Kidney conditions: CKD and lipid lowering therapies | CVDPREVENT CVDP010CHOL | practice | Mar 2026 | 73.1 | 73.3 | -0.1 |  | Matches |  |
| IND232 | Kidney conditions: eGFR for long-term NSAID use |  |  |  |  |  |  |  | No comparator |  |
| IND233 | Kidney conditions: CKD and eGFR | CVDPREVENT CVDP006CKD | practice | Mar 2026 |  |  |  |  | Related figure only |  |
| IND234 | Kidney conditions: CKD – eGFR and ACR | Retired QOF CKD004 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND235 | Kidney conditions: CKD and blood pressure when ACR less than 70 | CVDPREVENT CVDP007CKD | practice | Mar 2026 | 69.5 | 76.1 | -6.6 | -0.4 | Gap explained and reproduced | Strict BP thresholds; NICE excludes frailty |
| IND239 | Hypertension: blood pressure (79 years and under) | CVDPREVENT CVDP002HYP | practice | Mar 2026 | 66.2 | 73.8 | -7.6 | 0.3 | Gap explained and reproduced | Strict vs inclusive BP threshold; age scope differs |
| IND240 | Hypertension: blood pressure (80 years and over) | CVDPREVENT CVDP003HYP | practice | Mar 2026 | 81.0 | 84.2 | -3.3 | -0.3 | Gap explained and reproduced | Strict vs inclusive BP threshold |
| IND241 | Angina and coronary heart disease: blood pressure (79 years and under) | CVDPREVENT CVDP002CHD age 18-79 | NCL | Mar 2026 | 78.3 | 83.9 | -5.6 | 0.2 | Gap explained and reproduced | Strict vs inclusive BP threshold |
| IND242 | Angina and coronary heart disease: blood pressure (80 years and over) | CVDPREVENT CVDP002CHD age 80+ | NCL | Mar 2026 | 86.4 | 89.1 | -2.7 | -0.3 | Matches | Strict vs inclusive BP threshold |
| IND243 | Stroke and ischaemic attack: blood pressure (79 years and under) | CVDPREVENT CVDP002STRK age 18-79 | NCL | Mar 2026 | 74.3 | 81.0 | -6.7 | 0.1 | Gap explained and reproduced | Strict vs inclusive BP threshold |
| IND244 | Stroke and ischaemic attack: blood pressure (80 years and over) | CVDPREVENT CVDP002STRK age 80+ | NCL | Mar 2026 | 85.6 | 88.5 | -2.9 | -0.2 | Matches | Strict vs inclusive BP threshold |
| IND245 | Peripheral arterial disease: blood pressure (79 years and under) | Retired QOF PAD002 (INLIQ) | London | Mar 2024 |  |  |  |  | Related figure only |  |
| IND246 | Peripheral arterial disease: blood pressure (80 years and over) | Retired QOF PAD002 (INLIQ) | London | Mar 2024 |  |  |  |  | Related figure only |  |
| IND247 | Atrial fibrillation: DOACs and Vitamin K antagonists | CVDPREVENT CVDP005AF | practice | Mar 2026 | 87.7 | 88.0 | -0.2 |  | Matches |  |
| IND248 | Bipolar, schizophrenia and other psychoses: 6 physical health checks | PHSMI all six checks | practice | Mar 2026 | 66.7 | 67.0 | -0.3 |  | Matches |  |
| IND249 | Diabetes: blood pressure (without moderate or severe frailty) | QOF DM036 | practice | 2025/26 | 73.2 | 80.6 | -7.4 | -0.1 | Gap explained and reproduced | Strict vs inclusive BP threshold |
| IND260 | Lipid disorders: FH assessment and diagnosis (historical readings) | NCD CVD-04 | practice | Mar 2025 | 49.7 | 42.4 | 7.3 | 2.4 | Gap explained and reproduced | NICE adds a clinical FH diagnosis-only route |
| IND261 | Lipid disorders: FH assessment and diagnosis (new readings) | NCD CVD-04 | practice | Mar 2025 |  |  |  |  | Related figure only |  |
| IND263 | Kidney conditions: CKD - ACEi and ARB | Retired QOF NM84 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND264 | Kidney conditions: CKD and blood pressure when ACR 70 or more | Retired QOF CKD002 (INLIQ) | London | Mar 2023 |  |  |  |  | Related figure only |  |
| IND265 | Learning disabilities: health checks and action plans | NCD learning disability health checks (HI01) | practice | Mar 2025 | 72.0 | 83.5 | -11.5 | -1.6 | Gap explained and reproduced | NICE includes under-14s; plan must follow check |
| IND266 | Learning disabilities: health checks, action plans and ethnicity | LDHC check and plan x HCLD ethnicity recorded | NCL | Mar 2026 | 68.7 | 78.0 | -9.3 | 0.5 | Gap explained and reproduced | NICE includes under-14s; the health check scheme starts at 14 |
| IND267 | Cancer: faecal immunochemical testing | IIF CAN-04 | practice | Mar 2026 | 78.2 | 80.7 | -2.5 |  | Matches | FIT and referral recording; residual cause unresolved |
| IND269 | Cardiovascular disease prevention: risk assessment (general population) | NCD CVD-07 | practice | Mar 2025 | 45.8 | 30.0 | 15.8 | -1.4 | Gap explained and reproduced | Age, CVD/LLT exclusions and prior-test requirements differ |
| IND270 | Cardiovascular disease prevention: risk assessment (modifiable risk factors) | NICE testing report (CPRD) | England | Mar 2023 | 33.4 | 33.3 | 0.1 |  | Matches |  |
| IND272 | Asthma: objective tests | QOF AST012 | practice | 2025/26 | 67.8 | 78.2 | -10.5 | -0.6 | Gap explained and reproduced | Diagnosis cohort, treatment and objective-test rules differ |
| IND273 | Asthma: annual review | QOF AST007 | practice | 2025/26 | 34.6 | 67.1 | -32.5 | -0.3 | Gap explained and reproduced | NICE has no recent asthma-treatment restriction |
| IND274 | Diabetes: lipid-lowering therapies for primary prevention of CVD (T2DM and 10% risk) | NDA T2 primary-prevention statins | practice | Mar 2026 |  |  |  |  | Related figure only |  |
| IND275 | Diabetes: lipid-lowering therapies for primary prevention of CVD (40 years and over) | NDA primary-prevention statins | practice | Mar 2026 | 76.5 | 75.2 | 1.3 | -0.5 | Matches | Any LLT vs statins; age and frailty exclusions differ |
| IND276 | Diabetes: lipid-lowering therapies for secondary prevention of CVD | NDA secondary-prevention statins | practice | Mar 2026 | 91.7 | 91.0 | 0.8 | -2.4 | Matches |  |
| IND277 | Diabetes: T1DM and lipid-lowering therapies | NDA T1 primary-prevention statins | NCL | Mar 2026 | 71.5 | 69.1 | 2.4 | -1.3 | Matches | NICE includes CVD, over-80s and non-statin LLT |
| IND278 | Cardiovascular disease prevention: cholesterol treatment target (secondary prevention) | CVDPREVENT CVDP012CHOL | practice | Mar 2026 | 51.4 | 51.6 | -0.2 |  | Matches |  |
| IND287 | Cardiovascular disease prevention: lipid lowering therapy for people newly diagnosed with hypertension or T2DM | NCD CVD-13 | practice | Mar 2025 |  |  |  |  | Related figure only |  |
| IND315 | Asthma: annual review (higher risk patients) | NICE asthma testing report | England | 2023/24 | 67.1 | 66.3 | 0.8 |  | Matches |  |
| IND316 | Asthma: MART (higher risk patients) | NICE asthma testing report | England | 2023/24 | 1.5 | 2.3 | -0.8 |  | Matches |  |
| IND317 | Heart failure: 4 pillars (HFrEF) | CVDPREVENT CVDP003HF | practice | Mar 2026 | 45.5 | 46.1 | -0.6 |  | Matches |  |
| IND318 | Heart failure: ejection fraction category | NICE HF ejection-fraction testing report | England | Mar 2024 |  |  |  |  | Related figure only |  |
| IND319 | Weight management: advice for people living with overweight (18 to 39 years) | Weight-advice literature | England |  |  |  |  |  | Related figure only |  |
| IND320 | Weight management: BMI recording (long term conditions) | NDA BMI recording (diabetes subgroup) | practice | 2025/26 | 85.7 | 85.7 | 0.0 |  | Matches | Compared on the diabetes subgroup only |
| IND321 | Screening: cervical (25 to 64 years) | NHSE cervical coverage 25-64 | practice | Mar 2024 | 68.4 | 61.8 | 6.6 | 2.2 | Gap explained and reproduced | 66 vs 42 months in younger band; exclusions differ |
| IND324 | Kidney conditions: CKD and SGLT2 inhibitors | CVDPREVENT CVDP008CKD | practice | Mar 2026 | 44.2 | 48.6 | -4.3 | 3.6 | Gap explained, not reproduced | Broader NICE eligibility vs TA775 renal and RAS criteria |
