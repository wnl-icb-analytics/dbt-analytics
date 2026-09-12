# NICE indicator specification review

Reviewed 12 September 2026. This audit covers all 105 implemented NICE indicators,
their metadata, and the observation, medication and register inputs that determine
their results. It records confirmed corrections and retained limitations. It does
not certify full conformance where the specification or available evidence is
incomplete.

## Scope and sources

The official NICE specifications were downloaded before the parallel review
started. NICE defines the population, numerator, exclusions, thresholds and time
rules. Matching QOF v51 business rules resolve details that NICE leaves unclear;
they do not replace an explicit NICE rule with a different payment indicator or
newer clinical guideline.

Each measure represents one currently registered, living, non-test person in its
denominator, assessed on the build date. Family tables have one row per person and
indicator. Measures retain their documented rolling periods and scope before
personalised care adjustments (PCAs), except where an explicit reporting-period
correction is recorded below. These are NICE measures, not QOF payment extracts.

The [source inventory](nice-specification-sources.json) records each indicator's
implementation path, official page, downloaded PDF and SHA-256. Page references
below refer to those PDFs. Downloaded source files were retained separately; full
copyrighted specifications are not included in this repository.

The effective July bundle from the [official QOF v51 business rules publication](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/primary-care-business-rules/business-rules/quality-and-outcomes-framework-qof-business-rules-v51-2026/)
was used. Tracked insertions were retained and deletions removed when extracting
the documents. The exact document versions are:

| Version | Documents |
|---|---|
| v51.0 | Asthma; Atrial Fibrillation; Blood Pressure; Cervical Screening; CHD; Cholesterol; COPD; Dementia; Disease registers; Hypertension; Mental health; Non-diabetic Hyperglycaemia; Smoking; Stroke |
| v51.1 | Cardiovascular Disease; Diabetes; Vaccination and Immunisation |
| v51.2 | HF |
| v51.3 | Obesity |

The expanded-cluster workbook contained a portal notice rather than cluster
members. Selected PCD clusters and drug refsets were checked against public
reference data or the official terminology service. Current reference membership
is not a frozen historical expansion of every QOF v51 code set.

## Indicator findings

"Reviewed" means no additional defect was confirmed in the checked rules.
"Fixed" includes corrections to a shared input. A retained limitation can remain
after a fix; the shared limitations below also apply to relevant rows.

### Alcohol

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND196](https://www.nice.org.uk/indicators/ind196-alcohol-use-risk-assessment-for-people-with-hypertension/IND196-20260224.pdf) | Fixed | p2. New hypertension requires FAST or AUDIT-C within three months either side of diagnosis. Full AUDIT alone is excluded; later unrelated screens no longer hide qualifying evidence. |
| [IND197](https://www.nice.org.uk/indicators/ind197-alcohol-use-brief-intervention-for-people-with-hypertension/IND197-20260224.pdf) | Fixed | p2. FAST >=3 or AUDIT-C >=5 and intervention within three months. Retains qualifying earlier pairs. NICE does not impose a separate 12-month expiry on this positive screen. |
| [IND198](https://www.nice.org.uk/indicators/ind198-alcohol-use-risk-assessment-for-people-with-depression-or-anxiety/IND198-20260224.pdf) | Fixed | pp2-3. Age >=10 with new depression/anxiety; FAST or AUDIT-C within three months either side of diagnosis. Restores depression-only patients aged 10-17 omitted by the adult register. Full AUDIT alone no longer qualifies. |
| [IND199](https://www.nice.org.uk/indicators/ind199-alcohol-use-brief-intervention-for-people-with-depression-or-anxiety/IND199-20260224.pdf) | Fixed | p2. New depression/anxiety from age 10, positive FAST/AUDIT-C in 12 months and intervention within three months. Restores depression-only patients aged 10-17. Any qualifying pair counts; alcohol-related disorders remain excluded. |
| [IND200](https://www.nice.org.uk/indicators/ind200-alcohol-use-brief-intervention-for-people-with-smi/IND200-20260224.pdf) | Fixed | p2. Diagnosed severe mental illness (SMI), positive FAST/AUDIT-C in 12 months and intervention within three months. Lithium-only membership no longer qualifies; diagnosed remission retains its existing eligibility. Any qualifying pair counts; alcohol-related disorders remain excluded. |
| [IND201](https://www.nice.org.uk/indicators/ind201-alcohol-use-risk-assessment-for-people-with-a-long-term-condition/IND201-20260224.pdf) | Fixed | pp2-3. Listed long-term conditions require FAST or AUDIT-C in two years. Full AUDIT alone cannot qualify or displace an earlier specified tool. |
| [IND202](https://www.nice.org.uk/indicators/ind202-alcohol-use-brief-intervention-for-people-with-a-long-term-condition/IND202-20260224.pdf) | Fixed | p2. Listed long-term conditions, positive FAST/AUDIT-C in two years and intervention within three months. Any qualifying pair counts; alcohol-related disorders remain excluded. |

### Antithrombotic treatment

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND94](https://www.nice.org.uk/indicators/ind94-peripheral-arterial-disease-antiplatelets/IND94-20260720.pdf) | Fixed; timing retained | p2. PAD requires antiplatelet use in 15 months. Added recorded over-the-counter aspirin. The existing 15-month anticoagulant prescription exclusion remains because NICE supplies no separate current-treatment window. |
| [IND132](https://www.nice.org.uk/indicators/ind132-angina-and-coronary-heart-disease-anti-platelet-or-anticoagulation/IND132-20260223.pdf) | Fixed | p2. CHD requires antiplatelet or anticoagulant use in 12 months. Added recorded over-the-counter salicylate and anticoagulant prophylaxis evidence, following QOF CHD005. Prescription dates remain distinct. |
| [IND133](https://www.nice.org.uk/indicators/ind133-stroke-and-ischaemic-attack-anti-platelet-or-anticoagulation/IND133-20260223.pdf) | Fixed | pp2-3. Requires positive non-haemorrhagic stroke or TIA evidence, including people with both stroke types. Added recorded treatment evidence. NICE's four-class contraindication exclusion remains despite QOF's additional ticagrelor class. |

### Atrial fibrillation

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND127](https://www.nice.org.uk/indicators/ind127-atrial-fibrillation-annual-stroke-risk-assessment/IND127-20260223.pdf) | Fixed | p3. QOF AF006 resolves previous risk as the latest eligible score, not the historical maximum. CHADS2 must precede 1 April 2015; a CHA2DS2-VASc assessment in 12 months can achieve the measure. |
| [IND128](https://www.nice.org.uk/indicators/ind128-atrial-fibrillation-current-treatment-with-anticoagulation/IND128-20260223.pdf) | Fixed | p2. Last CHA2DS2-VASc >=2, or legacy CHADS2 >=2 if none, and anticoagulant prescription in six months. Added generic persisting contraindications and retained invalid latest scores as unassessable. |
| [IND169](https://www.nice.org.uk/indicators/ind169-atrial-fibrillation-review-of-anticoagulation/IND169-20260223.pdf) | Proxy retained | pp2-3. Age >18, anticoagulant prescription in six months and review in 12 months. AFMON_COD does not prove every specified anticoagulant-review component; QOF v51 has no matching rule. |
| [IND247](https://www.nice.org.uk/indicators/ind247-atrial-fibrillation-doa-cs-and-vitamin-k-antagonists/IND247-20260224.pdf) | Fixed | pp2-3. Antiphospholipid syndrome allows DOAC success, then VKA fallback; valvular AF requires VKA. QOF AF008 resolves 12-month decline/not-indicated expiry and TTR >=65% in six months for not-indicated fallback. |

### BMI recording

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND320](https://www.nice.org.uk/indicators/ind320-weight-management-bmi-recording-long-term-conditions/IND320-20260218.pdf) | Fixed; proxy retained | pp2-3. QOF Obesity v51.3 resolves dyslipidaemia through treatment and latest LDL, triglyceride and sex-specific HDL thresholds. Non-diabetic hyperglycaemia remains a proxy for NICE's validated-risk-score-plus-glycaemia population. |

### Blood pressure control

All thresholds below are strict and require both components below their limits.
The shared paired-reading limitation described below applies to all eight.

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND239](https://www.nice.org.uk/indicators/ind239-hypertension-blood-pressure-79-years-and-under/IND239-20260224.pdf) | Reviewed; input limit | p2. Hypertension, age <=79; latest BP in 12 months <140/90 clinic or <135/85 home/ambulatory. |
| [IND240](https://www.nice.org.uk/indicators/ind240-hypertension-blood-pressure-80-years-and-over/IND240-20260224.pdf) | Reviewed; input limit | p2. Hypertension, age >=80; latest BP in 12 months <150/90 clinic or <145/85 home/ambulatory. |
| [IND241](https://www.nice.org.uk/indicators/ind241-angina-and-coronary-heart-disease-blood-pressure-79-years-and-under/IND241-20260224.pdf) | Reviewed; input limit | p2. CHD, age <=79; latest BP in 12 months <140/90 clinic or <135/85 home/ambulatory. |
| [IND242](https://www.nice.org.uk/indicators/ind242-angina-and-coronary-heart-disease-blood-pressure-80-years-and-over/IND242-20260224.pdf) | Reviewed; input limit | p2. CHD, age >=80; latest BP in 12 months <150/90 clinic or <145/85 home/ambulatory. |
| [IND243](https://www.nice.org.uk/indicators/ind243-stroke-and-ischaemic-attack-blood-pressure-79-years-and-under/IND243-20260224.pdf) | Reviewed; input limit | p2. Stroke/TIA, age <=79; latest BP in 12 months <140/90 clinic or <135/85 home/ambulatory. |
| [IND244](https://www.nice.org.uk/indicators/ind244-stroke-and-ischaemic-attack-blood-pressure-80-years-and-over/IND244-20260224.pdf) | Reviewed; input limit | p2. Stroke/TIA, age >=80; latest BP in 12 months <150/90 clinic or <145/85 home/ambulatory. |
| [IND245](https://www.nice.org.uk/indicators/ind245-peripheral-arterial-disease-blood-pressure-79-years-and-under/IND245-20260224.pdf) | Reviewed; input limit | p2. PAD, age <=79; latest BP in 12 months <140/90 clinic or <135/85 home/ambulatory. |
| [IND246](https://www.nice.org.uk/indicators/ind246-peripheral-arterial-disease-blood-pressure-80-years-and-over/IND246-20260224.pdf) | Reviewed; input limit | p2. PAD, age >=80; latest BP in 12 months <150/90 clinic or <145/85 home/ambulatory. |

### Cervical screening

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND176](https://www.nice.org.uk/indicators/ind176-screening-cervical-screening-25-to-49-years/IND176-20260223.pdf) | Fixed | p2. Women 25-49, screening in 42 months. Uses NOCX_COD for cervix removal, pregnancy outcomes for current pregnancy and QOF CS005 nonresponse timing. Completed screening takes precedence over nonresponse. |
| [IND177](https://www.nice.org.uk/indicators/ind177-screening-cervical-screening-50-to-64-years/IND177-20260223.pdf) | Fixed | p2. Women 50-64, screening in 66 months. Corrected cervix removal and current pregnancy evidence; QOF CS006 resolves the 66-month nonresponse window and success precedence. |
| [IND321](https://www.nice.org.uk/indicators/ind321-screening-cervical-25-to-64-years/IND321-20260218.pdf) | Fixed | p2. Women 25-64, screening in 66 months. NOCX_COD replaces generic unsuitable records. Nonresponse is a PCA and pregnancy is not excluded; neither removes people before PCAs. |

### Childhood immunisation

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND215](https://www.nice.org.uk/indicators/ind215-immunisation-d-ta-p-8-months/IND215-20260224.pdf) | Fixed; exclusion proxy | p2. Three DTP-containing doses before eight months. QOF accepts 4-, 5- and 6-in-1 evidence. Distinct administration dates survive programme decline/dose sequencing. Generic contraindication remains broader than anaphylaxis. |
| [IND216](https://www.nice.org.uk/indicators/ind216-immunisation-mmr-18-months/IND216-20260224.pdf) | Fixed; exclusion proxy | p2. MMR at 12 months up to, but not including, 18 months. Evidence survives programme dose sequencing. Contraindication codes do not fully establish anaphylaxis or immunocompromise. |
| [IND217](https://www.nice.org.uk/indicators/ind217-immunisation-d-ta-p-ipv-and-mmr-5-years/IND217-20260224.pdf) | Fixed; exclusion proxy | p2. DTaP/IPV booster and two MMR doses between the first and fifth birthdays. QOF VI003 requires explicit booster evidence; generic 4-in-1 products are insufficient. Exclusion evidence remains incomplete. |
| [IND218](https://www.nice.org.uk/indicators/ind218-immunisation-mmr-5-years/IND218-20260224.pdf) | Fixed; exclusion proxy | p2. At least one MMR dose between the first and fifth birthdays; two doses also satisfy it. Evidence survives programme sequencing. Anaphylaxis and immunocompromise are incompletely distinguished. |
| [IND224](https://www.nice.org.uk/indicators/ind224-immunisation-rotavirus-24-weeks/IND224-20260224.pdf) | Fixed | p2. Two rotavirus doses on distinct dates before 24 weeks. Clinical administration/order evidence survives programme sequencing. The specified vaccine-contraindication exclusion remains. |
| [IND225](https://www.nice.org.uk/indicators/ind225-immunisation-meningitis-b-8-months/IND225-20260224.pdf) | Fixed | p2. Two MenB doses on distinct dates before eight months. Clinical administration evidence survives programme sequencing. The specified MenB-contraindication exclusion remains. |
| [IND226](https://www.nice.org.uk/indicators/ind226-immunisation-meningitis-b-18-months/IND226-20260224.pdf) | Fixed; timing retained | p2. Two primary MenB doses and a booster before 18 months. Evidence survives programme sequencing. Primary doses before 12 months and booster at 12-18 months remain a local interpretation without a matching QOF rule. |

### Chronic kidney disease

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND130](https://www.nice.org.uk/indicators/ind130-kidney-conditions-ckd-and-renin-angiotensin-system-antagonists/IND130-20260223.pdf) | Reviewed | pp2-3. CKD, hypertension and proteinuria; ACE inhibitor or ARB in six months. Both-class contraindication interpretation follows IND134 and QOF DM006, with persisting and 12-month expiring records distinguished. |
| [IND144](https://www.nice.org.uk/indicators/ind144-kidney-conditions-ckd-urine-albumin-creatinine-ratio/IND144-20260223.pdf) | Reviewed | p2. Coded CKD stages 3-5 and ACR or PCR test in 12 months. Tests count without numeric results; stages 1-2 and unrelated albumin measurements do not qualify. |
| [IND233](https://www.nice.org.uk/indicators/ind233-kidney-conditions-ckd-and-e-gfr/IND233-20260224.pdf) | Fixed; provenance limit | p2. Two eGFRs at least 90 days apart, second within 90 days before new diagnosis. Future tests no longer count. Current inputs cannot identify excluded acute secondary-care episodes and require numeric results. |
| [IND234](https://www.nice.org.uk/indicators/ind234-kidney-conditions-ckd-e-gfr-and-acr/IND234-20260224.pdf) | Fixed; input limit | p2. eGFR and ACR within 90 days either side of new diagnosis. Both inputs are now capped at the build date. The eGFR input requires a numeric result. |
| [IND235](https://www.nice.org.uk/indicators/ind235-kidney-conditions-ckd-and-blood-pressure-when-acr-less-than-70/IND235-20260224.pdf) | Fixed; BP input limit | p2. ACR must be <70 mg/mmol; missing ACR no longer qualifies. Frailty exclusions and strict <140/90 clinic or <135/85 home/ambulatory targets remain. Invalid latest renal results do not fall back. |
| [IND263](https://www.nice.org.uk/indicators/ind263-kidney-conditions-ckd-ac-ei-and-arb/IND263-20260224.pdf) | Fixed upstream | pp2-3. Last ACR >=70 mg/mmol, no diabetes and ACE inhibitor/ARB in six months. Future renal results are excluded; invalid latest ACR no longer selects an older result. |
| [IND264](https://www.nice.org.uk/indicators/ind264-kidney-conditions-ckd-and-blood-pressure-when-acr-70-or-more/IND264-20260224.pdf) | Fixed; BP input limit | pp2-3. Last ACR >=70 mg/mmol, no moderate/severe frailty; strict <130/80 clinic or <125/75 home/ambulatory. Future and invalid latest renal selection corrected. |
| [IND324](https://www.nice.org.uk/indicators/ind324-kidney-conditions-ckd-and-sglt-2-inhibitors/IND324-20260218.pdf) | Fixed; boundaries retained | p3. CKD SGLT2 eligibility uses diabetes type, renal thresholds and RAS treatment/contraindications. Future/invalid latest renal selection corrected. Published integer eGFR ranges leave decimal gaps; same-date RAS/SGLT2 ordering remains an interpretation. |

### Cardiovascular risk assessment

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND161](https://www.nice.org.uk/indicators/ind161-cardiovascular-disease-prevention-cardiovascular-risk-assessment-for-people-newly-diagnosed-with-hypertension-or-t2-dm/IND161-20260223.pdf) | Fixed; timing retained | pp2-3. Age 25-84, new hypertension/type 2 diabetes; assessment within three months either side. CVD exclusion now includes haemorrhagic stroke. Earlier-date anchoring when both diagnoses are new remains unresolved. |
| [IND181](https://www.nice.org.uk/indicators/ind181-diabetes-cvd-risk-assessment/IND181-20260224.pdf) | Fixed | p2. Type 2 diabetes, age 25-84, no moderate/severe frailty or current statin; assessment in three years. CVD exclusion now includes haemorrhagic stroke. Existing FH and CKD exclusions remain. |
| [IND269](https://www.nice.org.uk/indicators/ind269-cardiovascular-disease-prevention-risk-assessment-general-population/IND269-20260224.pdf) | Reviewed | p3. Age 45-84, QRISK/2/3 in five years. Retains explicit CVD, type 1 diabetes, FH, CKD, recent lipid treatment and risk >=20 exclusions. This indicator explicitly uses non-haemorrhagic CVD. |
| [IND270](https://www.nice.org.uk/indicators/ind270-cardiovascular-disease-prevention-risk-assessment-modifiable-risk-factors/IND270-20260224.pdf) | Definition retained | pp3-4. Age 43-84 with specified risk factors; QRISK in three years and IND269 exclusions. Latest total cholesterol >5 mmol/L remains a hypercholesterolaemia proxy whose threshold NICE/QOF do not specify. |

### Diabetes and hyperglycaemia

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND81](https://www.nice.org.uk/indicators/ind81-diabetes-annual-foot-exam-and-risk-classification/IND81-20260223.pdf) | Fixed | p3. Examination and risk classification in 15 months; amputations excluded. Positive same-date evidence now survives declined/unsuitable codes. Current QOF DM037 success-before-PCA ordering resolves precedence; the pre-PCA population remains. |
| [IND88](https://www.nice.org.uk/indicators/ind88-diabetes-referral-for-structured-education/IND88-20260223.pdf) | Fixed; period retained | pp1-2. Referral within nine months of register entry, represented by first diagnosis. Future diagnoses/referrals excluded. The documented rolling 12-month denominator remains; it differs from the NICE financial year and QOF payment cohort. |
| [IND111](https://www.nice.org.uk/indicators/ind111-diabetes-annual-albumin-creatinine-test/IND111-20260223.pdf) | Reviewed | p2. ACR test in 15 months, with or without a value. PCR, albumin concentration and findings alone do not count. |
| [IND120](https://www.nice.org.uk/indicators/ind120-diabetes-annual-general-practice-checks/IND120-20260224.pdf) | Fixed | pp2-3. February 2026 correction requires eGFR rather than serum creatinine. ACR/eGFR tests count without values. Completed foot examinations survive same-date or later negative records. The other five shared processes remain. |
| [IND134](https://www.nice.org.uk/indicators/ind134-diabetes-ac-ei-or-ar-bs/IND134-20260218.pdf) | Fixed | p3. Proteinuria or microalbuminuria is required; nephropathy alone no longer qualifies. QOF DM006 confirms PRT/MAL evidence. Both-class contraindications and six-month ACE inhibitor/ARB treatment remain. |
| [IND135](https://www.nice.org.uk/indicators/ind135-diabetes-ifcc-hb-a1c-64mmol-mol-or-less/IND135-20260218.pdf) | Fixed | pp2-3. Last HbA1c <=64 mmol/mol in 12 months. Fructosamine excludes only when no HbA1c test exists in the period. Maximum-treatment exclusion and invalid-last-result handling remain. |
| [IND136](https://www.nice.org.uk/indicators/ind136-diabetes-ifcc-hb-a1c-75mmol-mol-or-less/IND136-20260218.pdf) | Fixed | p3. Last HbA1c <=75 mmol/mol in 12 months. QOF DM020/DM021 resolves fructosamine substitution as absence of HbA1c; simultaneous tests now retain eligibility. |
| [IND137](https://www.nice.org.uk/indicators/ind137-diabetes-annual-retinal-screening/IND137-20260218.pdf) | Reviewed | pp2-3. Retinal screening completion in 12 months. Wider screening-interval changes do not replace this explicit NICE interval. |
| [IND160](https://www.nice.org.uk/indicators/ind160-diabetes-annual-examination-of-foot-sensation/IND160-20260223.pdf) | Fixed | p2. Requires monofilament foot sensation testing in 12 months, not generic examination, pulses, risk or referral. Positive test evidence survives same-date declined/unsuitable codes. |
| [IND165](https://www.nice.org.uk/indicators/ind165-diabetes-ifcc-hb-a1c-58mmol-mol-or-less/IND165-20260218.pdf) | Fixed | p2. Whole diabetes register, last HbA1c <=58 mmol/mol in 12 months. Fructosamine exclusion now requires no HbA1c in the period; maximum-treatment exclusion remains. |
| [IND171](https://www.nice.org.uk/indicators/ind171-diabetes-ndh-diabetes-prevention-programme/IND171-20260223.pdf) | Fixed | p2. New adult non-diabetic hyperglycaemia without unresolved diabetes; NHS Healthier You referral made or declined after diagnosis. Invitations do not count. Future diagnoses/referrals excluded. |
| [IND172](https://www.nice.org.uk/indicators/ind172-diabetes-ndh-annual-hb-a1c-or-fpg-test/IND172-20260223.pdf) | Reviewed | p2. Adult non-diabetic hyperglycaemia without unresolved diabetes; HbA1c or fasting plasma glucose in 12 months. QOF's broader combined gestational-diabetes population is not imported. |
| [IND173](https://www.nice.org.uk/indicators/ind173-diabetes-gestational-diabetes-annual-hb-a1c-test/IND173-20260223.pdf) | Authority gap retained | pp1-2. Original NM151 guidance confirms latest gestational diabetes episode >12 months ago. The separate old-diabetes exclusion, including later-resolved disease, lacks independently recovered pilot authority. |
| [IND179](https://www.nice.org.uk/indicators/ind179-diabetes-hb-a1c-58-mmol-mol/IND179-20260218.pdf) | Fixed | p2. Last HbA1c <=58 mmol/mol without moderate/severe frailty. Fructosamine excludes only without HbA1c in the period, following QOF DM020. Maximum-treatment exclusion remains. |
| [IND180](https://www.nice.org.uk/indicators/ind180-diabetes-hb-a1c-75-mmol-mol/IND180-20260218.pdf) | Fixed | p2. Last HbA1c <=75 mmol/mol with moderate/severe frailty. Fructosamine excludes only without HbA1c in the period, following QOF DM021. Maximum-treatment exclusion remains. |
| [IND249](https://www.nice.org.uk/indicators/ind249-diabetes-blood-pressure-without-moderate-or-severe-frailty/IND249-20260224.pdf) | Reviewed; BP input limit | p3. Diabetes, age 17-79, no moderate/severe frailty; last BP in 12 months strictly <140/90 clinic or <135/85 home/ambulatory. Shared paired-reading selection remains. |

### Influenza vaccination

The completed August-March season convention remains explicit for these measures.
NICE and the supplied QOF rules do not settle the off-season rollover.

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND131](https://www.nice.org.uk/indicators/ind131-immunisation-flu-vaccine-for-people-with-chd/IND131-20260223.pdf) | Reviewed; season retained | p2. CHD and in-season vaccination. Persisting contraindications and expiring contraindications in 12 months are excluded. Later off-season doses cannot hide qualifying in-season evidence. |
| [IND141](https://www.nice.org.uk/indicators/ind141-immunisation-flu-vaccine-for-people-with-copd/IND141-20260722.pdf) | Reviewed; season retained | p2. COPD and in-season vaccination. Contraindication timing follows IND131/163 where IND141 is silent. The QOF COPD register migration is separate in PR #994. |
| [IND152](https://www.nice.org.uk/indicators/ind152-immunisation-flu-vaccine-for-people-with-long-term-conditions/IND152-20260223.pdf) | Reviewed; season retained | p2. CHD, stroke/TIA, diabetes from age 17, or COPD and in-season vaccination. NICE lists no exclusions; no contraindication exclusion is added. |
| [IND163](https://www.nice.org.uk/indicators/ind163-immunisation-flu-vaccine-for-people-with-diabetes/IND163-20260223.pdf) | Reviewed; season retained | p2. Diabetes from age 17 and in-season vaccination, with explicit persisting/expiring contraindication handling. Shared diabetes-register date ties remain separate. |
| [IND164](https://www.nice.org.uk/indicators/ind164-immunisation-flu-vaccine-for-people-with-stroke-or-tia/IND164-20260223.pdf) | Fixed; season retained | p2. Stroke/TIA and in-season vaccination. Removed contraindication from denominator exclusions because NICE classifies it as a PCA. |

### Lipids

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND229](https://www.nice.org.uk/indicators/ind229-cardiovascular-disease-prevention-primary-prevention-with-lipid-lowering-therapies/IND229-20260224.pdf) | Fixed | pp2-3. Restored explicit ages 25-84 and all-stroke CVD exclusion. Last risk >=10 uses QOF CVDASS2 tools beyond QRISK; invalid latest scores cannot fall back. Treatment remains six months. |
| [IND230](https://www.nice.org.uk/indicators/ind230-cardiovascular-disease-prevention-secondary-prevention-with-lipid-lowering-therapies/IND230-20260224.pdf) | Reviewed | p3. CHD, stroke/TIA or PAD; haemorrhagic history excluded; statin or non-statin lipid prescription in six months. Shared population, treatment classes and grain checked. |
| [IND231](https://www.nice.org.uk/indicators/ind231-kidney-conditions-ckd-and-lipid-lowering-therapies/IND231-20260224.pdf) | Reviewed | pp2-3. Adult coded CKD stages 3-5, haemorrhagic history excluded; lipid prescription in six months. Shared CKD-register limitations remain. |
| [IND274](https://www.nice.org.uk/indicators/ind274-diabetes-lipid-lowering-therapies-for-primary-prevention-of-cvd-t2-dm-and-10-risk/IND274-20260224.pdf) | Fixed | p3. Restored ages 25-84; type 2 diabetes, risk >=10 in 12 months, no moderate/severe frailty or CVD. Added broader CVDASS2 numeric tools and all-stroke exclusion. |
| [IND275](https://www.nice.org.uk/indicators/ind275-diabetes-lipid-lowering-therapies-for-primary-prevention-of-cvd-40-years-and-over/IND275-20260224.pdf) | Fixed | p3. Diabetes age >=40, no CVD or moderate/severe frailty. QOF DM034 resolves low-risk chronology: a later score >=10 cancels the three-year low-score exclusion. Added all-stroke CVD exclusion. |
| [IND276](https://www.nice.org.uk/indicators/ind276-diabetes-lipid-lowering-therapies-for-secondary-prevention-of-cvd/IND276-20260224.pdf) | Reviewed | p3. Diabetes with secondary-prevention CVD; haemorrhagic history excluded; lipid prescription in six months. Register precedence and overlapping exclusions checked. |
| [IND277](https://www.nice.org.uk/indicators/ind277-diabetes-t1-dm-and-lipid-lowering-therapies/IND277-20260224.pdf) | Reviewed | p3. Type 1 diabetes, age strictly >40; haemorrhagic history excluded; lipid prescription in six months. The explicit >40 boundary remains. |
| [IND278](https://www.nice.org.uk/indicators/ind278-cardiovascular-disease-prevention-cholesterol-treatment-target-secondary-prevention/IND278-20260224.pdf) | Reviewed | pp2-3. CVD without FH/haemorrhagic history; latest LDL/non-HDL in 12 months, LDL wins same-day ties. Inclusive targets <=2.0/2.6 mmol/L respectively; invalid latest result is unassessable. |
| [IND287](https://www.nice.org.uk/indicators/ind287-cardiovascular-disease-prevention-lipid-lowering-therapy-for-people-newly-diagnosed-with-hypertension-or-t2-dm/IND287-20260224.pdf) | Fixed | pp2-3. Age 25-84, new hypertension/type 2 diabetes, risk >=10 in 12 months, no CVD/CKD/FH/type 1 diabetes. Added broader CVDASS2 tools and all-stroke exclusion. Treatment uses six months. |

### Long-term condition reviews

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND104](https://www.nice.org.uk/indicators/ind104-depression-and-anxiety-review-within-10-to-35-days/IND104-20260223.pdf) | Fixed | p2. Adults with a new depression episode since 1 April of the current financial year; review at 10-35 days inclusive. Replaced rolling 12 months with explicit financial-year-to-date eligibility. |
| [IND110](https://www.nice.org.uk/indicators/ind110-rheumatoid-arthritis-annual-review/IND110-20260223.pdf) | Reviewed; evidence limit | p2. RA register, age >=16 upstream, review in 15 months. Review codes do not independently establish the required face-to-face setting. |
| [IND139](https://www.nice.org.uk/indicators/ind139-hypothyroidism-annual-thyroid-function-test/IND139-20260223.pdf) | Reviewed | p2. Hypothyroidism register and thyroid-function test in 12 months. No extra age floor or exclusion added; the condition is outside current QOF disease-register rules. |
| [IND142](https://www.nice.org.uk/indicators/ind142-dementia-care-planning/IND142-20260223.pdf) | Fixed; evidence limit | p2. Dementia care plan/review in 12 months must now be on or after first diagnosis, following QOF DEM004. Review codes do not prove face-to-face setting. |
| [IND191](https://www.nice.org.uk/indicators/ind191-copd-annual-review/IND191-20260722.pdf) | Fixed | p2. COPD review now also requires exacerbation-count and MRC assessment records, each in 12 months. NICE and QOF COPD010 require all three but no same-day restriction. |
| [IND195](https://www.nice.org.uk/indicators/ind195-heart-failure-annual-review/IND195-20260224.pdf) | Fixed | p2. Heart failure review now requires NYHA assessment and medication review, each in 12 months. QOF HF v51.2 resolves medication-review evidence; no same-day rule added. |
| [IND223](https://www.nice.org.uk/indicators/ind223-cancer-review-within-12-months/IND223-20260224.pdf) | Reviewed; evidence limit | p2. Latest first/new cancer episode in 24 months; review on/after diagnosis and within 12 months. Review codes do not independently establish use of a structured template. |
| [IND265](https://www.nice.org.uk/indicators/ind265-learning-disabilities-health-checks-and-action-plans/IND265-20260224.pdf) | Reviewed | p2. All ages on learning-disability register; health check and completed action plan in 12 months, plan on/after check. No general-programme age floor imported. |
| [IND266](https://www.nice.org.uk/indicators/ind266-learning-disabilities-health-checks-action-plans-and-ethnicity/IND266-20260224.pdf) | Reviewed | pp2-3. IND265 plus recorded ethnicity. Unknown/not-stated values remain excluded under the existing National Clinical Director interpretation. Same-date plans are allowed. |
| [IND273](https://www.nice.org.uk/indicators/ind273-asthma-annual-review/IND273-20260224.pdf) | Fixed | p2. Asthma age >=5; review and written plan on the same date, exacerbation count in the preceding month through review date. QOF AST015 resolves timing. Later incomplete reviews cannot hide qualifying evidence. |

### Multimorbidity

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND207](https://www.nice.org.uk/indicators/ind207-multiple-long-term-conditions-medication-review/IND207-20260224.pdf) | Fixed; coverage limit | pp2-4. Corrected label to four NICE condition categories; lithium alone no longer creates a mental-health category. Several condition inputs remain absent, and review codes do not prove tool consideration/shared discussion. |
| [IND208](https://www.nice.org.uk/indicators/ind208-multiple-long-term-conditions-asking-about-falls/IND208-20260224.pdf) | Evidence limit retained | p2. Age >=65 with moderate/severe frailty; discussion/falls assessment in 12 months. Broad codes do not prove the required number and type of falls; QOF supplies no matching rule. |

### Shingles vaccination

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND219](https://www.nice.org.uk/indicators/ind219-immunisation-shingles/IND219-20260224.pdf) | Fixed; exclusions/age retained | p2. Vaccination at ages 70-75 for people reaching 75 in 12 months. Earlier qualifying SHVAC evidence survives later doses. Generic contraindication no longer substitutes for anaphylaxis; immunosuppression evidence and inclusive 75th-birthday boundary remain limitations. |

### Severe mental illness and lithium monitoring

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND82](https://www.nice.org.uk/indicators/ind82-bipolar-schizophrenia-and-other-psychoses-annual-record-of-alcohol-consumption/IND82-20260223.pdf) | Reviewed | p2. Active diagnosed SMI and alcohol consumption recorded in 15 months. QOF MH007 supports the diagnosis-only population and remission exclusion without replacing NICE's interval. |
| [IND83](https://www.nice.org.uk/indicators/ind83-bipolar-schizophrenia-and-other-psychoses-annual-bmi-recording/IND83-20260223.pdf) | Reviewed | p2. Active diagnosed SMI and numeric BMI recorded in 15 months. QOF MH006 supports remission exclusion; declined measurement remains a PCA. |
| [IND84](https://www.nice.org.uk/indicators/ind84-bipolar-schizophrenia-and-other-psychoses-annual-blood-pressure/IND84-20260223.pdf) | Reviewed; BP input limit | p2. Active diagnosed SMI and blood pressure recorded in 15 months. QOF MH003 supports remission exclusion; shared valid-pair evidence and pre-PCA scope remain. |
| [IND85](https://www.nice.org.uk/indicators/ind85-bipolar-schizophrenia-and-other-psychoses-cervical-screening/IND85-20260223.pdf) | Population retained | p3. Women 25-64, screening in 60 months, no clinical exclusions. Existing active-SMI scope is retained by analogy with QOF; NICE does not independently settle remission. |
| [IND87](https://www.nice.org.uk/indicators/ind87-bipolar-schizophrenia-and-other-psychoses-lithium-levels-in-therapeutic-range/IND87-20260223.pdf) | Definition retained | pp2-3. Current lithium treatment and monitoring in four months. Existing latest-result and 0.4-1.0 mmol/L rules lack explicit NICE authority; retired MH010 is absent from QOF v51. |
| [IND143](https://www.nice.org.uk/indicators/ind143-bipolar-schizophrenia-and-other-psychoses-care-planning/IND143-20260223.pdf) | Reviewed | p2. Care plan in 12 months on/after latest diagnosis following remission, otherwise first diagnosis. Existing sequence matches QOF MH002 and NICE relapse handling. |
| [IND150](https://www.nice.org.uk/indicators/ind150-cardiovascular-disease-prevention-cardiovascular-risk-assessment-for-people-with-bipolar-schizophrenia-or-other-psychoses/IND150-20260223.pdf) | Reviewed | p3. Active diagnosed SMI, ages 25-84; formal CVD assessment in 12 months. Excludes CVD, CKD, FH and type 1 diabetes; NICE permits tools beyond QRISK3. |
| [IND154](https://www.nice.org.uk/indicators/ind154-smoking-smoking-status-of-people-with-bipolar-schizophrenia-and-other-psychoses/IND154-20260223.pdf) | Population retained | pp2-3. Active diagnosed SMI and smoking status in 12 months. Explicit financial-year age and post-birthday/post-diagnosis never-smoker exemption retained; optional ex-smoker exemption remains unapplied. |
| [IND155](https://www.nice.org.uk/indicators/ind155-smoking-support-and-treatment-for-people-with-bipolar-schizophrenia-and-other-psychoses/IND155-20260223.pdf) | Fixed | p2. Current smokers with active diagnosed SMI; support/treatment in 12 months. QOF SMOK004 resolves referral and pharmacotherapy evidence. Generic advice or declined care alone no longer qualifies. |
| [IND158](https://www.nice.org.uk/indicators/ind158-bipolar-schizophrenia-and-other-psychoses-annual-cholesterol/IND158-20260223.pdf) | Reviewed | p2. Active diagnosed SMI, age >=18; cholesterol:HDL ratio in 12 months. CVD diagnosed >12 months ago is excluded. QOF's broader lipid-profile requirement does not replace the explicit ratio. |
| [IND159](https://www.nice.org.uk/indicators/ind159-bipolar-schizophrenia-and-other-psychoses-annual-blood-glucose-or-hb-a1c/IND159-20260223.pdf) | Reviewed | p2. Active diagnosed SMI, age >=18; blood glucose/HbA1c in 12 months. Diabetes diagnosed >12 months ago is excluded. Current measurement inputs require values. |
| [IND213](https://www.nice.org.uk/indicators/ind213-bipolar-schizophrenia-and-other-psychoses-cervical-screening-25-to-49-years/IND213-20260224.pdf) | Population retained | p3. Women 25-49 with active SMI; screening in 42 months and no clinical exclusions. NICE interval retained; remission interpretation remains as for IND85. |
| [IND214](https://www.nice.org.uk/indicators/ind214-bipolar-schizophrenia-and-other-psychoses-cervical-screening-50-to-64-years/IND214-20260224.pdf) | Population retained | p3. Women 50-64 with active SMI; screening in 66 months and no clinical exclusions. Remission interpretation remains as for IND85. |
| [IND248](https://www.nice.org.uk/indicators/ind248-bipolar-schizophrenia-and-other-psychoses-6-physical-health-checks/IND248-20260224.pdf) | Fixed | pp2-3. All six physical-health checks in 12 months. Now retains diagnosed SMI in remission because remission is a PCA; lithium-only register members do not establish diagnosed SMI. |

### Smoking

| Source | Verdict | Material rule, correction or retained limit |
|---|---|---|
| [IND97](https://www.nice.org.uk/indicators/ind97-smoking-smoking-status-for-people-with-long-term-conditions/IND97-20260223.pdf) | Fixed | p3. Listed long-term conditions or diagnosed SMI; smoking status in 12 months. Lithium-only SMI register membership no longer qualifies. NICE/QOF never-smoker timing remains; optional ex-smoker exemption is unapplied. |
| [IND156](https://www.nice.org.uk/indicators/ind156-smoking-smoking-status-of-people-with-long-term-conditions/IND156-20260223.pdf) | Reviewed | pp3-4. Listed long-term conditions and smoking status in 12 months. Explicit financial-year never-smoker rule retained; optional three-year ex-smoker exemption remains unapplied. Shared register limitations remain. |
| [IND157](https://www.nice.org.uk/indicators/ind157-smoking-support-and-treatment-for-people-with-long-term-conditions/IND157-20260223.pdf) | Fixed | p3. Listed long-term condition current smokers; support/treatment in 12 months. QOF SMOK004 supplies referral/pharmacotherapy evidence without importing its different age or 24-month rules. |

## Shared limitations and work outside this PR

- Blood pressure inputs retain the lowest valid paired reading per day and omit
  incomplete or implausible readings. An older valid pair can therefore survive a
  newer unusable observation. NICE's last-reading wording and QOF's component-wise
  rules do not settle the project's paired-reading definition. This affects BP
  targets and recording measures; the shared input was not redesigned here.
- The original [NICE NM151 guidance](https://www.nice.org.uk/Media/Default/Standards-and-indicators/QOF%20Indicator%20Key%20documents/NM151%20guidance%20document.pdf)
  resolves IND173's contradictory gestational-diabetes episode timing. It does not
  establish the separate retained exclusion for an old, later-resolved diabetes
  diagnosis.
- IND207 lacks complete condition inputs for chronic pain, digestive disorders,
  eating disorders, substance misuse, inflammatory polyarthropathies, systemic
  connective-tissue disorders and bronchiectasis. Its denominator is incomplete.
  Code-only review records also leave encounter setting, review content or falls
  details unproven in the indicators identified above.
- Medication and vaccination completeness depends on captured clinical records,
  orders and current reference mappings. The childhood and shingles anaphylaxis
  and immunocompromise gaps remain explicit; no unsupported clinical exclusions
  were invented.
- Shared asthma/COPD register handling of future-dated diagnoses and resolutions
  remains outside this review's register changes. The separate QOF work already
  handles the COPD diagnosis/administrative-code split and asthma age update.

Diabetes and CKD same-date diagnosis/resolution or downstage defects are tracked
in [issue #1183](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1183).
[QOF v51 PR #994](https://github.com/wnl-icb-analytics/dbt-analytics/pull/994) was
checked at `82c6ad8bb3a0a9069f1b496aede083d102691355` and still contains both
comparisons. QOF only removes a diagnosis after a strictly later resolution or
downstage. These shared-register fixes are excluded from this PR at the user's
request because they affect other programmes.

## Metadata and analyst descriptions

All 105 indicator metadata blocks were reviewed against the catalogue extractor
and its `def_indicator` and `def_indicator_usage` consumers. IDs and emitted SQL
IDs, type, category, clinical domain, names, descriptions, source column, usage
context, sort key and clinical-source link were checked. `is_qof: false` remains
appropriate for these NICE measures before PCAs; no payment mapping was invented.

Long catalogue definitions now state population, achievement, periods and material
exclusions or limits. Model descriptions use readable paragraphs rather than a
single dense block. Family and snapshot descriptions explain grain and purpose.
Duplicate opt-out text was removed from descriptions because the inherited
`config.meta.custom_message` and `generate_table_comment` hook already publish the
warning. The warning and project configuration remain unchanged.

## Validation

All checks below passed on 12 September 2026 using the tracked `dev` target and
existing DEV layers. No task-specific database, schema or target was created.

| Warehouse selection | Models | Tests | Snapshots | Build time |
|---|---:|---:|---:|---:|
| All NICE measures, changed shared inputs and their descendants, plus indicator definition models and consumers | 185 | 551 | 21 | 2m 46s |
| Final eGFR, SMI alcohol and cervical corrections, their descendants and catalogue refresh | 19 | 90 | 3 | 1m 10s |
| Depression population correction, affected alcohol descendants, final comments and catalogue refresh | 17 | 77 | 1 | 1m 54s |

Each selection compiled successfully before building. `dbt ls` checked the full
downstream selection. The final profile change adds depression evidence for ages
10-17; SQL lineage confirms that only IND198 and IND199 consume the changed date.
Their family table, snapshot input, snapshot and combined NICE status were rebuilt.

The [local regression checks](../scripts/qa/nice/README.md) execute repository SQL
against synthetic data. All six scripts passed: 154 cardiovascular assertions,
84 alcohol-pair assertions, 33 alcohol-population cases, 72 review/smoking cases,
10 diabetes/CKD groups and six prevention groups. They cover thresholds, age and
date boundaries, invalid or absent results, conflicting records and later events
that previously displaced valid evidence.

The metadata check passed for all 105 distinct NICE measures, including their
source flags, required properties and lineage to the combined status table.
Warehouse metadata queries confirmed 105 catalogue definitions, NICE usage rows
and published models. All 105 Snowflake comments contain the exact final model
description and retain the inherited data-use warning.

Aggregate result checks found all 105 indicators in both production and DEV, with
no numerator greater than its denominator. Production reports 11 September and
DEV reports 12 September, so that comparison does not isolate the effect of these
fixes. No patient-level results were inspected or exported.

Model-description coverage, model-test coverage, hardcoded-reference checks,
raw/staging boundary checks and `git diff --check` passed. The source inventory
and audit each contain exactly 105 distinct NICE IDs. The branch was checked
against the latest `origin/main` before publication.
