# NICE indicators: how they compare with published figures

We compared our 149 NICE indicator measures with published national figures for North Central
London (NCL) practices. **94 of the 149 line up with a published figure.** 46 match within 3
percentage points as they stand. For the other 48, the difference comes from a known
difference in definition, and we tested it: when we recalculated our figure using the publisher's
rules, it matched within 3 points. 7 have a plausible explanation we could not test, and 48
have no like-for-like published figure.

The 27 NICE registers were also checked against QOF and other published register sizes. Four
follow NICE's own definitions and differ from QOF by design (described below).

Where the comparison found an error in our indicators, we fixed it before publishing these figures.
Indicator definitions are in [nice-indicators.md](nice-indicators.md).

## How to read this

- **Why figures rarely match exactly.** Published figures such as QOF follow their own rules. They
  let practices exclude some patients (exception reporting), measure over different time windows
  and sometimes use different targets. NICE indicators follow NICE's wording, which often differs.
  A difference does not mean either figure is wrong, but it should have an explanation.
- **Matches:** within 3 percentage points of a published figure that measures the same thing.
- **Explained:** further apart, but the reason is a known difference in definition, and
  recalculating our figure with the publisher's rules brings it within 3 points.
- **Explained, not tested:** a plausible reason, but the published data do not let us recalculate.
- **Related figure only:** the closest published figure measures something related (a retired QOF
  indicator, a NICE pilot or a study). It gives context, not a check.
- **By practice** means we matched practices one to one and compared the combined rates. We also
  checked that the agreement holds across practices, PCNs and boroughs. **NCL**, **London** or
  **England** means only an area figure exists; London and England figures are a regional check.

## Why NICE figures differ from QOF and other published figures

These are the most common reasons. Each was confirmed by recalculating our figure.

| Reason | Typical effect on our figure | Examples |
|---|---|---|
| NICE blood pressure targets count readings below the target; QOF also counts readings exactly at it, and many readings are recorded as exactly 140/90 | 6 to 8 points lower | IND239, IND241, IND243, IND249 |
| NICE leaves out people who cannot take a medicine; QOF's published figure keeps them | 4 to 10 points higher | IND132, IND133, IND134 |
| NICE requires a separately recorded symptom severity (NYHA class) | about 50 points lower | IND195 |
| NICE includes everyone with asthma; QOF only those treated in the last year | 20 to 30 points lower | IND189, IND273 |
| NICE allows 15 months where the published figure allows 12 | 3 to 8 points higher | IND82, IND83, IND84 |
| Different age ranges | 2 to 10 points | IND112 (40+ vs 45+), IND265 (all ages vs 14+) |

## Things to know when using these indicators

- **Alcohol brief intervention** (IND197, IND199, IND200, IND202) counts the national alcohol advice
  and intervention codes. These are recorded as often after a negative alcohol screen as after a
  positive one, so treat these indicators as measures of recording rather than of targeted advice.
- **Falls** (IND208) counts people asked about or assessed for falls, not people with a recorded fall.
- **Lithium** (IND87) uses NICE's 4-month interval; current guidance allows 6 months for stable
  patients, so the figure understates good practice.
- **Some indicators follow QOF's population** because that is where NICE took them from: smoking
  measures use the treated asthma register, atrial fibrillation measures leave out resolved AF, and
  severe mental illness measures leave out people in remission.
- **Four NICE registers differ from QOF by design.** Atrial fibrillation (IND185) includes resolved
  AF and is about 105% of the QOF register. Severe mental illness (IND256) adds people on lithium and
  is about 101%. Multimorbidity (IND205) and moderate or severe frailty (IND206) have no QOF
  equivalent.

## Differences we have not explained

- **Dementia:** our dementia register is about 4% larger than the national figure in many
  practices, with the same number of care plans, so IND142 is about 2.7 points lower.
- **Depression review (IND104):** under QOF's rules our rate is close, but our group of new episodes
  is about 25% larger than QOF's.
- **COPD review (IND191):** matches overall, but Enfield practices are 5 to 9 points below QOF.
- **Retinal screening (IND137):** well below the screening programme's own uptake, because many
  screening results do not reach GP records as codes.
- **Flu in risk groups** is published only for London, and **retired QOF indicators** after 2019/20
  do not include NCL practices, so those comparisons are regional.

## Results by clinical area

Figures are percentages. "Difference" is our figure minus the published one, in percentage points.
"With their rules" is the difference after recalculating our figure using the publisher's rules.
Related figures are shown for context; their difference is not calculated because they measure
something else.

### Cardiovascular

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND94 | Peripheral arterial disease: antiplatelets | Retired QOF PAD004 (Mar 2021 / Mar 2022, by practice) | 73.8% | 73.3% | +0.5 |  | Matches | The published figure is from March 2021; ours is from March 2022. |
| IND112 | Cardiovascular disease prevention: blood pressure measurement every 5 years | QOF BP002 (2025/26, by practice) | 79.4% | 85.2% | -5.8 | -0.4 | Explained | NICE starts at age 40, QOF at 45. |
| IND115 | Hypertension: confirming diagnosis with HBPM or ABPM | Network DES CVD-01 (Mar 2025, NCL) | 21.9% | 16.7% |  |  | Related figure only | The national figure covers follow-up of a raised reading, not confirmation of a new diagnosis. |
| IND121 | Hypertension: urinary albumin for target organ damage | CVDPREVENT CVDP009HYP (Mar 2026, NCL) | 40.3% | 38.0% |  |  | Related figure only | The national figure covers everyone with hypertension; NICE looks at the time of a new diagnosis. |
| IND122 | Hypertension: haematuria for target organ damage | NICE pilot (Oct 2013-Mar 2014, pilot) | 12.5% (Mar 2026) | 24.0% |  |  | Related figure only | Only a 20-practice NICE pilot from 2014 exists. |
| IND123 | Hypertension: ECG for target organ damage | NICE pilot (Oct 2013-Mar 2014, pilot) | 17.2% (Mar 2026) | 35.0% |  |  | Related figure only | Only a 20-practice NICE pilot from 2014 exists. |
| IND125 | Myocardial infarction: medication for MI in preceding 12 months | MINAP eligible secondary-prevention discharge drugs (2022/23, England) | 40.3% | 81.0% |  |  | Related figure only | The national figure is medication at hospital discharge; ours is current GP prescribing. |
| IND126 | Myocardial infarction: medication for MI more than 12 months ago | Retired QOF CHD006 (2013/14, NCL) | 44.9% (Mar 2026) | 72.9% |  |  | Related figure only | Retired from QOF in 2014, with a different definition. |
| IND127 | Atrial fibrillation: annual stroke risk assessment | QOF AF006 (2025/26, by practice) | 94.5% | 94.6% | -0.1 |  | Matches |  |
| IND128 | Atrial fibrillation: current treatment with anticoagulation | CVDPREVENT CVDP002AF (Mar 2026, by practice) | 90.9% | 89.6% | +1.3 |  | Matches |  |
| IND131 | Immunisation: flu vaccine for people with CHD | UKHSA flu uptake: chronic heart disease (2024/25, London) | 61.1% | 31.5% | +29.6 | +0.9 | Explained | UKHSA publishes under-65s only, for London; ours includes all ages, and older people have higher uptake. |
| IND132 | Angina and coronary heart disease: anti-platelet or anticoagulation | QOF CHD005 (2025/26, by practice) | 93.2% | 89.2% | +4.0 | -0.2 | Explained | NICE leaves out people who cannot take these medicines; QOF's figure keeps them. |
| IND161 | Cardiovascular disease prevention: cardiovascular risk assessment for people newly diagnosed with hypertension or T2DM | Network DES CVD-07 (Mar 2025, NCL) | 47.0% | 30.0% |  |  | Related figure only | The national figure covers a wider group, not just people newly diagnosed with hypertension or type 2 diabetes. |
| IND169 | Atrial fibrillation: review of anticoagulation | Network DES DOAC reviews (Aug 2026, NCL) | 23.8% | 18.7% |  |  | Related figure only | The national figure covers DOAC reviews only; NICE covers any anticoagulant. |
| IND192 | Heart failure: confirmation of diagnosis | QOF HF008 (2025/26, by practice) | 84.6% | 86.6% | -2.1 |  | Matches | QOF covers diagnoses since April 2023 only; NICE covers everyone with heart failure. |
| IND195 | Heart failure: annual review | QOF HF007 (2025/26, by practice) | 35.0% | 84.7% | -49.6 | -0.6 | Explained | NICE requires a separately recorded symptom severity (NYHA class); QOF does not. |
| IND196 | Alcohol use: risk assessment for people with hypertension | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 17.8% | 41.1% |  |  | Related figure only | The national figure covers new patients registering, not people with hypertension. |
| IND197 | Alcohol use: brief intervention for people with hypertension | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 31.6% | 0.9% |  |  | Related figure only | The national figure covers new patients registering, not people with hypertension. |
| IND229 | Cardiovascular disease prevention: primary prevention with lipid lowering therapies | CVDPREVENT CVDP006CHOL (Mar 2026, by practice) | 56.1% | 56.2% | -0.1 |  | Matches |  |
| IND230 | Cardiovascular disease prevention: secondary prevention with lipid lowering therapies | CVDPREVENT CVDP009CHOL (Mar 2026, by practice) | 85.9% | 86.1% | -0.2 |  | Matches |  |
| IND239 | Hypertension: blood pressure (79 years and under) | CVDPREVENT CVDP002HYP (Mar 2026, by practice) | 66.2% | 73.8% | -7.6 | +0.3 | Explained | NICE counts readings below 140/90; the published figure also counts readings of exactly 140/90. |
| IND240 | Hypertension: blood pressure (80 years and over) | CVDPREVENT CVDP003HYP (Mar 2026, by practice) | 81.0% | 84.2% | -3.3 | -0.3 | Explained | NICE counts readings below 150/90; the published figure also counts readings exactly at the target. |
| IND241 | Angina and coronary heart disease: blood pressure (79 years and under) | CVDPREVENT CVDP002CHD age 18-79 (Mar 2026, NCL) | 78.3% | 83.9% | -5.6 | +0.2 | Explained | NICE counts readings below 140/90; the published figure also counts readings of exactly 140/90. |
| IND242 | Angina and coronary heart disease: blood pressure (80 years and over) | CVDPREVENT CVDP002CHD age 80+ (Mar 2026, NCL) | 86.4% | 89.1% | -2.7 |  | Matches | NICE counts readings below 150/90; the published figure also counts readings exactly at the target. |
| IND245 | Peripheral arterial disease: blood pressure (79 years and under) | CVDPREVENT CVDP010HYP age 18-79 (Mar 2026, NCL) | 69.3% | 81.7% |  |  | Related figure only | The national figure combines heart disease, stroke and arterial disease; NICE covers arterial disease only. |
| IND246 | Peripheral arterial disease: blood pressure (80 years and over) | CVDPREVENT CVDP010HYP age 80+ (Mar 2026, NCL) | 83.7% | 87.8% |  |  | Related figure only | The national figure combines heart disease, stroke and arterial disease; NICE covers arterial disease only. |
| IND247 | Atrial fibrillation: DOACs and Vitamin K antagonists | CVDPREVENT CVDP005AF (Mar 2026, by practice) | 87.7% | 88.0% | -0.2 |  | Matches |  |
| IND269 | Cardiovascular disease prevention: risk assessment (general population) | Network DES CVD-07 (Mar 2025, by practice) | 45.8% | 30.0% | +15.8 | -1.4 | Explained | The national figure uses different ages and exclusions. |
| IND270 | Cardiovascular disease prevention: risk assessment (modifiable risk factors) | NICE testing report (Mar 2023, England) | 33.4% | 33.3% | +0.1 |  | Matches |  |
| IND278 | Cardiovascular disease prevention: cholesterol treatment target (secondary prevention) | CVDPREVENT CVDP012CHOL (Mar 2026, by practice) | 51.4% | 51.6% | -0.2 |  | Matches |  |
| IND287 | Cardiovascular disease prevention: lipid lowering therapy for people newly diagnosed with hypertension or T2DM | Network DES CVD-13 (Mar 2025, NCL) | 55.1% | 53.5% |  |  | Related figure only | The national figure covers a general higher-risk group, not newly diagnosed people. |
| IND317 | Heart failure: 4 pillars (HFrEF) | CVDPREVENT CVDP003HF (Mar 2026, by practice) | 45.5% | 46.1% | -0.6 |  | Matches |  |
| IND318 | Heart failure: ejection fraction category | NICE CPRD HF ejection-fraction recording (Mar 2024, England) | 54.1% (Oct 2026) | 42.0% |  |  | Related figure only | Our measure covers diagnoses from April 2026, so there is no matching period yet. |

### Renal

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND130 | Kidney conditions: CKD and renin–angiotensin system antagonists | CVDPREVENT CVDP005CKD (Mar 2026, by practice) | 77.7% | 72.9% | +4.8 | +0.0 | Explained | NICE leaves out people who cannot take these medicines; the published figure keeps them. |
| IND144 | Kidney conditions: CKD urine albumin:creatinine ratio | CVDPREVENT CVDP004CKD (Mar 2026, by practice) | 65.6% | 64.6% | +1.0 |  | Matches |  |
| IND231 | Kidney conditions: CKD and lipid lowering therapies | CVDPREVENT CVDP010CHOL (Mar 2026, by practice) | 73.1% | 73.3% | -0.1 |  | Matches |  |
| IND232 | Kidney conditions: eGFR for long-term NSAID use | Nothing published | 69.8% |  |  |  | No comparator | Nothing is published on kidney tests for people taking anti-inflammatory painkillers long term. |
| IND233 | Kidney conditions: CKD and eGFR | CVDPREVENT CVDP006CKD (Mar 2026, NCL) | 80.6% | 90.9% |  |  | Related figure only | The national figure covers everyone with CKD; NICE looks at the time of a new diagnosis. |
| IND234 | Kidney conditions: CKD – eGFR and ACR | CVDPREVENT CVDP004CKD (Mar 2026, NCL) | 64.5% | 64.6% |  |  | Related figure only | The national figure covers everyone with CKD; NICE looks at the time of a new diagnosis. |
| IND235 | Kidney conditions: CKD and blood pressure when ACR less than 70 | CVDPREVENT CVDP007CKD (Mar 2026, by practice) | 69.5% | 76.1% | -6.6 | -0.4 | Explained | NICE counts readings below the target, not at it, and leaves out people with frailty. |
| IND263 | Kidney conditions: CKD - ACEi and ARB | CVDPREVENT CVDP005CKD (Mar 2026, NCL) | 65.0% | 72.9% |  |  | Related figure only | The national figure covers a different kidney group. |
| IND264 | Kidney conditions: CKD and blood pressure when ACR 70 or more | Retired QOF CKD002 (Mar 2023, London) | 23.7% | 65.0% |  |  | Related figure only | Retired from QOF; it covered everyone with CKD, with a different target. |
| IND324 | Kidney conditions: CKD and SGLT2 inhibitors | CVDPREVENT CVDP008CKD (Mar 2026, NCL) | 44.4% | 48.6% |  |  | Explained, not tested | NICE covers a wider group than CVDPREVENT, which follows the narrower NICE technology appraisal criteria. |

### Diabetes, weight and metabolic

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND81 | Diabetes: annual foot exam and risk classification | NDA foot surveillance (Mar 2026, by practice) | 82.1% | 77.4% | +4.8 | +1.1 | Explained | NICE allows 15 months for the foot check; the audit counts 12. |
| IND88 | Diabetes: referral for structured education | QOF DM014 (2025/26, by practice) | 67.7% | 66.9% | +0.8 |  | Matches |  |
| IND89 | Diabetes: annual dietary review | Retired QOF DM013 (2013/14, NCL) | 13.7% (Mar 2026) | 83.9% |  |  | Related figure only | Retired from QOF in 2014, when practices were paid for it. |
| IND111 | Diabetes: annual albumin creatinine test | NDA urine albumin (Mar 2026, by practice) | 76.6% | 69.9% | +6.7 | +0.8 | Explained | NICE allows 15 months for the urine test; the audit counts 12. |
| IND116 | Contraception: advice for people with diabetes | NICE pilot (Oct 2012-Mar 2013, pilot) | 9.0% (Mar 2026) | 1.5% |  |  | Related figure only | Only a 12-practice NICE pilot from 2013 exists. |
| IND120 | Diabetes: annual general practice checks | NDA eight care processes (Mar 2026, by practice) | 58.6% | 59.1% | -0.5 |  | Matches |  |
| IND134 | Diabetes: ACEi or ARBs | QOF DM006 (2025/26, by practice) | 88.5% | 78.2% | +10.4 | -0.1 | Explained | NICE leaves out people who cannot take these medicines; QOF's figure keeps them. |
| IND135 | Diabetes: IFCC-HbA1c 64mmol/mol or less | QOF DM020 (2025/26, by practice) | 71.0% | 59.5% | +11.5 | +0.1 | Explained | NICE uses a 64 mmol/mol target for everyone; QOF uses 58 and leaves out moderate or severe frailty. |
| IND136 | Diabetes: IFCC-HbA1c 75mmol/mol or less | NDA HbA1c <=75 (Mar 2026, by practice) | 80.1% | 86.6% | -6.5 | -0.1 | Explained | The audit counts only people with a valid HbA1c result; NICE counts everyone on the register. |
| IND137 | Diabetes: annual retinal screening | NHS diabetic eye screening programme uptake (12 months to Sep 2024, NCL) | 48.0% | 86.0% |  |  | Explained, not tested | The programme reports attendance among people invited; many results do not reach GP records as codes. |
| IND160 | Diabetes: annual examination of foot sensation | QOF DM012 (2025/26, by practice) | 64.8% | 78.7% | -13.9 | +0.0 | Explained | NICE counts a foot sensation (monofilament) test; QOF counts a foot risk classification. |
| IND163 | Immunisation: flu vaccine for people with diabetes | UKHSA flu uptake: diabetes (2024/25, London) | 53.0% | 41.2% | +11.8 | -0.9 | Explained | UKHSA publishes under-65s only, for London; ours includes all ages, and older people have higher uptake. |
| IND165 | Diabetes: IFCC-HbA1c 58mmol/mol or less | NDA HbA1c <=58 (Mar 2026, by practice) | 62.7% | 65.8% | -3.0 |  | Matches |  |
| IND171 | Diabetes: NDH diabetes prevention programme | NDA NDH cumulative DPP offers (Mar 2026, by practice) | 36.3% | 63.6% | -27.3 | +2.1 | Explained | NICE counts referrals of newly identified people; the audit counts offers to everyone with the condition, ever. |
| IND172 | Diabetes: NDH annual HbA1c or FPG test | NDA NDH HbA1c/FPG (Mar 2026, by practice) | 83.7% | 83.3% | +0.3 |  | Matches |  |
| IND179 | Diabetes: HbA1c 58 mmol/mol | QOF DM020 (2025/26, by practice) | 63.0% | 59.5% | +3.5 | +0.1 | Explained | NICE leaves out people already on maximum treatment; QOF's figure keeps them. |
| IND180 | Diabetes: HbA1c 75 mmol/mol | QOF DM021 (2025/26, by practice) | 86.8% | 83.1% | +3.7 | +0.1 | Explained | NICE leaves out people already on maximum treatment; QOF's figure keeps them. |
| IND181 | Diabetes: CVD risk assessment | NICE CPRD testing report comorbidity group (Mar 2023, England) | 62.4% | 41.0% |  |  | Related figure only | Only a related group in NICE's own testing report exists. |
| IND220 | Weight management: referral to weight management programmes for obesity | GP annual paid weight referrals / obesity register (approx.) (2023/24-2024/25, NCL) | 22.8% | 15.0% |  |  | Related figure only | The only figure is the referral payments practices claimed, which leave out offers and declines. |
| IND221 | Weight management: referral to weight management programmes for obesity (co-existing hypertension or diabetes) | GP annual paid weight referrals / obesity register (approx.) (2023/24-2024/25, NCL) | 11.5% | 15.0% |  |  | Related figure only | The only figure is the referral payments practices claimed, for all obesity. |
| IND249 | Diabetes: blood pressure (without moderate or severe frailty) | QOF DM036 (2025/26, by practice) | 73.2% | 80.6% | -7.4 | -0.1 | Explained | NICE counts readings below 140/90; QOF also counts readings of exactly 140/90. |
| IND274 | Diabetes: lipid-lowering therapies for primary prevention of CVD (T2DM and 10% risk) | NDA T2/other primary-prevention statins (Apr 2025-Mar 2026, NCL) | 75.6% | 75.3% |  |  | Related figure only | The audit counts statins only and has no heart risk score filter. |
| IND275 | Diabetes: lipid-lowering therapies for primary prevention of CVD (40 years and over) | NDA primary-prevention statins (Mar 2026, by practice) | 76.5% | 75.2% | +1.3 |  | Matches |  |
| IND276 | Diabetes: lipid-lowering therapies for secondary prevention of CVD | NDA secondary-prevention statins (Mar 2026, by practice) | 91.7% | 91.0% | +0.8 |  | Matches |  |
| IND277 | Diabetes: T1DM and lipid-lowering therapies | NDA T1 primary-prevention statins (Mar 2026, NCL) | 71.5% | 69.1% | +2.4 |  | Matches |  |

### Endocrine

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND139 | Hypothyroidism: annual thyroid function test | Retired QOF THY002 (Mar 2024, London) | 88.1% | 88.2% | -0.1 |  | Matches |  |

### Respiratory

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND101 | COPD: offered pulmonary rehabilitation | QOF COPD008 (2022/23, by practice) | 90.1% | 56.8% | +33.3 | +0.4 | Explained | NICE counts an offer (or a decline) after a breathlessness score in the last 15 months; QOF counted referrals in a shorter window. |
| IND140 | COPD: FEV1 | Retired QOF COPD004 (FEV1 recording) (2018/19, NCL) | 7.2% (Mar 2026) | 79.4% |  |  | No comparator | No current published figure. The QOF indicator was retired in 2019. |
| IND141 | Immunisation: flu vaccine for people with COPD | UKHSA flu uptake: chronic respiratory disease (2024/25, London) | 59.8% | 33.9% | +25.9 | +0.9 | Explained | UKHSA publishes under-65s only, for London; ours includes all ages, and older people have higher uptake. |
| IND189 | Asthma: smoking status (under 19) | QOF AST008 (2025/26, by practice) | 47.1% | 70.0% | -23.0 | -0.1 | Explained | NICE includes everyone with asthma; QOF only those treated in the last year. |
| IND191 | COPD: annual review | QOF COPD010 (2025/26, by practice) | 77.4% | 80.3% | -2.9 |  | Matches | Matches overall, but Enfield practices sit 5 to 9 points below QOF. The cause is not known. |
| IND212 | COPD: oxygen saturation recording | Retired QOF COPD005 (oxygen saturation) (2018/19, NCL) | 66.4% (Mar 2026) | 96.0% |  |  | No comparator | No current published figure. The related QOF indicator was retired in 2019. |
| IND272 | Asthma: objective tests | QOF AST012 (2025/26, by practice) | 67.8% | 78.2% | -10.5 | -0.6 | Explained | QOF counts diagnoses since April 2025 from age 6, with its own list of tests. |
| IND273 | Asthma: annual review | QOF AST007 (2025/26, by practice) | 34.6% | 67.1% | -32.5 | -0.3 | Explained | NICE includes everyone with asthma; QOF only those treated in the last year. |
| IND315 | Asthma: annual review (higher risk patients) | NICE asthma testing report (2023/24, England) | 67.1% | 66.3% | +0.8 |  | Matches |  |
| IND316 | Asthma: MART (higher risk patients) | NICE asthma testing report (2023/24, England) | 1.5% | 2.3% | -0.8 |  | Matches |  |

### Mental health

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND80 | Dementia: target organ damage (new diagnoses) | Retired QOF DEM005 (2018/19, NCL) | 50.9% (Mar 2026) | 63.4% |  |  | Related figure only | The QOF indicator was retired in 2019, before our data start. |
| IND82 | Bipolar, schizophrenia and other psychoses: annual record of alcohol consumption | SMI health checks: alcohol (Jun 2026, by practice) | 85.6% | 78.0% | +7.6 | -0.2 | Explained | NICE allows 15 months; the national checks count 12. |
| IND83 | Bipolar, schizophrenia and other psychoses: annual BMI recording | SMI health checks: BMI (Jun 2026, by practice) | 84.1% | 78.3% | +5.8 | -1.1 | Explained | NICE allows 15 months; the national checks count 12. |
| IND84 | Bipolar, schizophrenia and other psychoses: annual blood pressure | SMI health checks: blood pressure (Jun 2026, by practice) | 84.4% | 78.2% | +6.2 | -0.6 | Explained | NICE allows 15 months; the national checks count 12. |
| IND85 | Bipolar, schizophrenia and other psychoses: cervical screening | Retired QOF MH008 (Mar 2023, London) | 64.4% | 66.3% | -1.9 |  | Matches |  |
| IND86 | Bipolar, schizophrenia and other psychoses: target organ damage | Retired QOF MH009 (2018/19, NCL) | 88.7% (Mar 2026) | 91.9% |  |  | Explained, not tested | Retired from QOF in 2019, before our data start. |
| IND87 | Bipolar, schizophrenia and other psychoses: lithium levels in therapeutic range | Network DES lithium monitoring (Mar 2025, NCL) | 53.9% | 45.0% |  |  | Explained, not tested | The national figure counts lithium tests, not levels in range. NICE asks for a level every 4 months; guidance now allows 6 for stable patients. |
| IND104 | Depression and anxiety: review within 10 to 35 days | QOF DEP004 (2025/26, NCL) | 18.8% | 22.0% |  |  | Explained, not tested | NICE asks for a review 10 to 35 days after diagnosis, QOF 10 to 56. We could not reproduce how QOF selects new episodes. |
| IND118 | Dementia: target organ damage (all patients) | Retired QOF DEM005 (2018/19, NCL) | 57.3% (Mar 2026) | 63.4% |  |  | Related figure only | The retired QOF indicator covered new diagnoses only and ended in 2019. |
| IND124 | Contraception: advice for people with bipolar, schizophrenia or other psychoses | NICE pilot (Oct 2013-Mar 2014, pilot) | 6.1% (Mar 2026) | 24.3% |  |  | Related figure only | Only a 20-practice NICE pilot from 2014 exists. |
| IND142 | Dementia: care planning | Primary Care Dementia Data (Mar 2026, by practice) | 73.4% | 76.0% | -2.6 |  | Matches | Our dementia register is about 4% larger than the national one, with the same number of care plans. The cause is not known. |
| IND143 | Bipolar, schizophrenia and other psychoses: care planning | QOF MH002 (2025/26, by practice) | 75.9% | 77.5% | -1.6 |  | Matches |  |
| IND150 | Cardiovascular disease prevention: cardiovascular risk assessment for people with bipolar, schizophrenia or other psychoses | NICE pilot (Oct 2014-Mar 2015, pilot) | 35.0% (Mar 2026) | 36.1% |  |  | Related figure only | Only a 26-practice NICE pilot from 2015 exists. |
| IND154 | Smoking: smoking status of people with bipolar, schizophrenia and other psychoses | SMI health checks: smoking (Mar 2026, by practice) | 90.8% | 83.9% | +6.9 | -0.4 | Explained | The national checks count any smoking record in 12 months; NICE also accepts never-smoked and long-standing ex-smoker records. |
| IND155 | Smoking: support and treatment for people with bipolar, schizophrenia and other psychoses | QOF SMOK005 (LTC smoking support) (2025/26, NCL) | 79.2% | 80.4% |  |  | Related figure only | QOF combines several conditions; there is no published figure for severe mental illness alone. |
| IND158 | Bipolar, schizophrenia and other psychoses: annual cholesterol | SMI health checks: cholesterol (Jun 2026, by practice) | 64.6% | 70.4% | -5.8 | -1.7 | Explained | NICE counts a cholesterol ratio test and leaves out people with heart disease; the national checks count any cholesterol test. |
| IND159 | Bipolar, schizophrenia and other psychoses: annual blood glucose or HbA1c | SMI health checks: glucose (Jun 2026, by practice) | 68.2% | 70.8% | -2.6 |  | Matches |  |
| IND198 | Alcohol use: risk assessment for people with depression or anxiety | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 10.9% | 41.1% |  |  | Related figure only | The national figure covers new patients registering, not people with depression or anxiety. |
| IND199 | Alcohol use: brief intervention for people with depression or anxiety | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 7.3% | 0.9% |  |  | Related figure only | The national figure covers new patients registering, not people with depression or anxiety. |
| IND200 | Alcohol use: brief intervention for people with SMI | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 24.2% | 0.9% |  |  | Related figure only | The national figure covers new patients registering, not people with severe mental illness. |
| IND213 | Bipolar, schizophrenia and other psychoses: cervical screening (25 to 49 years) | Cervical screening coverage age 25-49 (Jun 2024, NCL) | 61.1% | 58.0% |  |  | Related figure only | Published coverage is for all women, not women with severe mental illness. |
| IND214 | Bipolar, schizophrenia and other psychoses: cervical screening (50 to 64 years) | Cervical screening coverage age 50-64 (Jun 2024, NCL) | 63.2% | 71.3% |  |  | Related figure only | Published coverage is for all women, not women with severe mental illness. |
| IND248 | Bipolar, schizophrenia and other psychoses: 6 physical health checks | SMI health checks: all six (Mar 2026, by practice) | 66.7% | 67.0% | -0.3 |  | Matches |  |

### Neurology

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND117 | Contraception: advice for people with epilepsy | Retired QOF EP003 (Mar 2023, London) | 11.1% | 0.3% |  |  | Related figure only | Only a retired, sparsely reported London figure for a narrower age group exists. |
| IND133 | Stroke and ischaemic attack: anti-platelet or anticoagulation | QOF STIA007 (2025/26, by practice) | 93.0% | 88.2% | +4.8 | -0.3 | Explained | NICE leaves out people who cannot take these medicines; QOF's figure keeps them. |
| IND164 | Immunisation: flu vaccine for people with stroke or TIA | UKHSA flu uptake: chronic neurological disease (2024/25, London) | 57.9% | 32.9% | +25.0 | +1.7 | Explained | UKHSA publishes under-65s only, for London; ours includes all ages, and older people have higher uptake. |
| IND243 | Stroke and ischaemic attack: blood pressure (79 years and under) | CVDPREVENT CVDP002STRK age 18-79 (Mar 2026, NCL) | 74.3% | 81.0% | -6.7 | +0.1 | Explained | NICE counts readings below 140/90; the published figure also counts readings of exactly 140/90. |
| IND244 | Stroke and ischaemic attack: blood pressure (80 years and over) | CVDPREVENT CVDP002STRK age 80+ (Mar 2026, NCL) | 85.6% | 88.5% | -2.9 |  | Matches | NICE counts readings below 150/90; the published figure also counts readings exactly at the target. |

### Learning disability and autism

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND79 | Learning disabilities: annual TSH test | Retired QOF LD002B (Mar 2024, London) | 54.8% | 52.6% | +2.2 |  | Matches |  |
| IND265 | Learning disabilities: health checks and action plans | Network DES learning disability health checks (Mar 2025, by practice) | 72.0% | 83.5% | -11.5 | -1.6 | Explained | NICE includes all ages; annual health checks start at age 14. |
| IND266 | Learning disabilities: health checks, action plans and ethnicity | Learning disability health checks and ethnicity recording (Mar 2026, NCL) | 68.7% | 78.0% | -9.3 | +0.5 | Explained | NICE includes all ages; annual health checks start at age 14. |

### Musculoskeletal

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND91 | Osteoporosis: bone sparing agents (50-74 years) | Retired QOF OST002 (2018/19, NCL) | 36.9% (Mar 2026) | 63.6% |  |  | Explained, not tested | Retired from QOF in 2019, before our data start. NICE does not require a recorded osteoporosis diagnosis. |
| IND92 | Osteoporosis: bone sparing agents (75 years and over) | Retired QOF OST005 (2018/19, NCL) | 24.2% (Mar 2026) | 54.0% |  |  | Explained, not tested | Retired from QOF in 2019. QOF required an osteoporosis diagnosis and counted fractures from 2014 only. |
| IND108 | Rheumatoid arthritis: cardiovascular risk assessment | Retired QOF RA003 (Mar 2023, London) | 31.1% | 30.3% | +0.8 |  | Matches |  |
| IND110 | Rheumatoid arthritis: annual review | QOF RA002 (2022/23, by practice) | 86.2% | 85.3% | +0.9 |  | Matches |  |

### Frailty

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND208 | Multiple long-term conditions: asking about falls | GP Contract Services falls assessment (Mar 2026, by practice) | 1.4% | 5.2% | -3.8 | +0.1 | Explained | The national figure covers people with severe frailty; NICE covers people with several long-term conditions. |

### Multimorbidity

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND201 | Alcohol use: risk assessment for people with a long-term condition | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 34.0% | 41.1% |  |  | Related figure only | The national figure covers new patients registering, not people with long-term conditions. |
| IND202 | Alcohol use: brief intervention for people with a long-term condition | GP Contract Services alcohol (new patients) (Mar 2026, NCL) | 25.5% | 0.9% |  |  | Related figure only | The national figure covers new patients registering, not people with long-term conditions. |
| IND207 | Multiple long-term conditions: medication review | Network DES medication reviews (Mar 2026, by practice) | 14.8% | 21.2% | -6.4 | +0.0 | Explained | The national figure covers people with severe frailty; NICE covers people with several long-term conditions. |

### Cancer

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND113 | Cancer: 3-month review | QOF CAN004 (2025/26, by practice) | 38.9% | 71.6% | -32.7 | +0.1 | Explained | NICE looks for a review within 3 months of diagnosis; QOF allows 12. |
| IND222 | Cancer: review within 3 months | QOF CAN005 (2025/26, by practice) | 39.5% | 36.9% | +2.6 |  | Matches |  |
| IND223 | Cancer: review within 12 months | QOF CAN004 (2025/26, by practice) | 83.6% | 71.6% | +11.9 | +0.1 | Explained | NICE and QOF use different diagnosis periods. |
| IND267 | Cancer: faecal immunochemical testing | IIF CAN-04 (FIT before referral) (Mar 2026, by practice) | 78.2% | 80.7% | -2.5 |  | Matches |  |

### Lifestyle

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND97 | Smoking: smoking status for people with long-term conditions | QOF SMOK002 (2025/26, by practice) | 88.9% | 93.0% | -4.1 | +0.5 | Explained | QOF also includes people with severe mental illness and lets long-standing ex-smokers count without a yearly record. |
| IND98 | Smoking: support and treatment for people with long-term conditions or SMI | QOF SMOK005 (2025/26, by practice) | 80.1% | 80.4% | -0.3 |  | Matches |  |
| IND99 | Smoking: support and treatment (all patients) | QOF SMOK004 (2025/26, by practice) | 93.4% | 94.4% | -1.0 |  | Matches |  |
| IND156 | Smoking: smoking status of people with long-term conditions | QOF SMOK002 (2025/26, by practice) | 91.7% | 93.0% | -1.3 |  | Matches |  |
| IND157 | Smoking: support and treatment for people with long term conditions | QOF SMOK005 (2025/26, by practice) | 80.8% | 80.4% | +0.4 |  | Matches |  |
| IND319 | Weight management: advice for people living with overweight (18 to 39 years) | England survey: overweight adults recalling GP advice (Oct 2018, England) | 8.4% (Mar 2026) | 12.6% |  |  | Related figure only | Only a 2018 survey of what patients recall exists. |
| IND320 | Weight management: BMI recording (long term conditions) | NDA BMI recording (diabetes subgroup) (2025/26, by practice) | 85.7% | 85.7% | +0.0 |  | Matches | Compared for people with diabetes, about half of those covered. |

### Prevention and screening

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND152 | Immunisation: flu vaccine for people with long-term conditions | UKHSA flu total at risk under 65 (2025/26, NCL) | 54.2% | 31.7% | +22.5 | +1.4 | Explained | UKHSA publishes under-65s in risk groups; ours includes all ages on four registers. |
| IND215 | Immunisation: DTaP (8 months) | Childhood vaccination (COVER) DTaP 12 months (Mar 2026, by practice) | 82.4% | 84.9% | -2.5 |  | Matches | NICE checks at 8 months; the national figure at 12. |
| IND216 | Immunisation: MMR (18 months) | Childhood vaccination (COVER) MMR1 24 months (Mar 2026, by practice) | 73.4% | 76.8% | -3.4 | +0.9 | Explained | NICE checks at 18 months; the national figure at 24. |
| IND217 | Immunisation: DTaP/IPV and MMR (5 years) | QOF VI003 (2025/26, by practice) | 68.8% | 69.0% | -0.2 |  | Matches |  |
| IND218 | Immunisation: MMR (5 years) | Childhood vaccination (COVER) MMR1 5 years (Mar 2026, by practice) | 85.5% | 84.5% | +1.0 |  | Matches |  |
| IND219 | Immunisation: shingles | UKHSA shingles age-75 cohort (2023/24 (doses to Sep 2023), NCL) | 64.1% | 64.2% | -0.1 |  | Matches |  |
| IND224 | Immunisation: rotavirus (24 weeks) | Childhood vaccination (COVER) rotavirus 12 months (Mar 2026, by practice) | 83.4% | 83.0% | +0.5 |  | Matches |  |
| IND225 | Immunisation: meningitis B (8 months) | Childhood vaccination (COVER) MenB 12 months (Mar 2026, by practice) | 83.6% | 84.8% | -1.2 |  | Matches | NICE checks at 8 months; the national figure at 12. |
| IND226 | Immunisation: meningitis B (18 months) | Childhood vaccination (COVER) MenB booster 24 months (Mar 2026, by practice) | 70.4% | 75.5% | -5.1 | +1.2 | Explained | NICE checks the booster at 18 months; the national figure at 24. |

### Sexual and reproductive health

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND78 | Contraception: advice for people taking anti-seizure medication | Retired QOF EP003 (Mar 2023, London) | 0.9% | 0.3% |  |  | Related figure only | Only a retired, sparsely reported London figure exists. Both show advice is rarely recorded. |
| IND148 | Contraception: LARC for people on oral or patch contraceptives | Retired QOF CON002 (2018/19, NCL) | 22.3% (Mar 2026) | 39.9% |  |  | Related figure only | Retired from QOF in 2019, before our data start. |
| IND149 | Contraception: LARC for people using emergency contraception | Retired QOF CON003 (2018/19, NCL) | 12.9% (Mar 2026) | 93.2% |  |  | Related figure only | Retired from QOF in 2019, when practices were paid for it. |
| IND176 | Screening: cervical screening (25 to 49 years) | Cervical screening coverage 25-49 (Mar 2024, by practice) | 67.7% | 58.3% | +9.4 | +1.9 | Explained | NICE leaves out pregnant women and women who did not respond to invitations; the published figure keeps them. |
| IND177 | Screening: cervical screening (50 to 64 years) | QOF CS006 (2025/26, by practice) | 78.3% | 73.7% | +4.5 | +0.4 | Explained | NICE leaves out women who did not respond to invitations; QOF's figure keeps them. |
| IND178 | Pregnancy and neonates: postnatal mental health | CQC maternity survey: enough GP mental-health time (2025, England) | 13.9% | 75.8% |  |  | Related figure only | The national figure is a patient survey, not GP records. |
| IND210 | HIV: testing at registration | NICE pilot (Dec 2018-Mar 2019, pilot) | 1.6% (Mar 2026) | 0.0% |  |  | Related figure only | Only an 8-practice NICE pilot exists. |
| IND321 | Screening: cervical (25 to 64 years) | Cervical screening coverage 25-64 (Mar 2024, by practice) | 68.4% | 61.8% | +6.6 | +2.2 | Explained | NICE uses one 5.5-year interval; the published figure uses 3.5 years under 50, and NICE leaves out non-responders. |

### Maternity

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND173 | Diabetes: gestational diabetes annual HbA1c test | CPRD-HES annual glucose testing after GDM (GDM diagnoses 2000-18, England) | 53.6% (Mar 2026) | 23.9% |  |  | Related figure only | Only a published study of older cohorts exists. |

### Familial hypercholesterolaemia

| ID | Indicator | Compared with | Ours | Published | Difference | With their rules | Result | Why they differ |
|---|---|---|---:|---:|---:|---:|---|---|
| IND260 | Lipid disorders: FH assessment and diagnosis (historical readings) | Network DES CVD-04 (Mar 2025, by practice) | 49.7% | 42.4% | +7.3 | +2.4 | Explained | NICE also counts a recorded familial hypercholesterolaemia diagnosis. |
| IND261 | Lipid disorders: FH assessment and diagnosis (new readings) | Network DES CVD-04 (Mar 2025, NCL) | 12.8% | 42.0% |  |  | Related figure only | The national figure covers high readings at any age; NICE looks at recent readings. |

## Sources

- **QOF:** Quality and Outcomes Framework, 2021/22 to 2025/26, by practice. **Retired QOF**
  figures come from earlier QOF years or from "Indicators no longer in QOF".
- **CVDPREVENT:** the national cardiovascular disease prevention audit, by practice.
- **NDA:** the National Diabetes Audit, by practice.
- **SMI health checks:** physical health checks for people with severe mental illness, NHS England.
- **Learning disability health checks:** the Learning Disabilities Health Check Scheme and Health
  and Care of People with Learning Disabilities, NHS England.
- **Primary Care Dementia Data**, NHS England.
- **Childhood vaccination (COVER)**, and **UKHSA** flu and shingles uptake.
- **Cervical screening coverage**, NHS England.
- **GP Contract Services**, the **Network Contract DES** and the **Investment and Impact Fund
  (IIF)**, NHS England.
- **NICE pilots and testing reports:** NICE's own indicator development work.

Where a publisher removes excluded patients, we compare with its figure before those exclusions,
because our indicators do not use exception reporting.
