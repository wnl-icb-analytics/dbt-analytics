# NICE general practice indicators

This page records coverage of all 192 NICE general practice indicators for the NICE close-out, including the 24 wave 1 measures.
Network and system indicators are out of scope.

`Built` includes 105 measures on main and 24 assigned to wave 1; wave 1 names below are integration locations and depend on those packages passing validation.
`Register` is a catalogue entry without a numerator; it does not imply an exact NICE population.
Register filters and rule differences are in the model YAML and `def_indicator.description_long`.
Register counts use currently registered, living, non-test people and the appropriate reference date.
`Planned` identifies later work that needs new or extended code lists, inputs or agreed rules.
The evidence figures in `Not built` rows describe coded activity in the close-out research, not whether care was delivered.

| ID | Title | Status | Where it lives or reason |
|---|---|---|---|
| IND78 | Contraception: advice for people taking anti-seizure medication | Planned | Contraception: advice code list and sterilisation or hysterectomy evidence for anti-seizure medication users. |
| IND79 | Learning disabilities: annual TSH test | Built | `fct_person_learning_disability_thyroid_test_ind79` (wave 1) |
| IND80 | Dementia: target organ damage (new diagnoses) | Built | `fct_person_dementia_baseline_tests_ind80` (wave 1) |
| IND81 | Diabetes: annual foot exam and risk classification | Built | `fct_person_diabetes_foot_risk_ind81` |
| IND82 | Bipolar, schizophrenia and other psychoses: annual record of alcohol consumption | Built | `fct_person_smi_alcohol_ind82` |
| IND83 | Bipolar, schizophrenia and other psychoses: annual BMI recording | Built | `fct_person_smi_bmi_ind83` |
| IND84 | Bipolar, schizophrenia and other psychoses: annual blood pressure | Built | `fct_person_smi_blood_pressure_ind84` |
| IND85 | Bipolar, schizophrenia and other psychoses: cervical screening | Built | `fct_person_smi_cervical_screening_ind85` |
| IND86 | Bipolar, schizophrenia and other psychoses: target organ damage | Built | `fct_person_lithium_renal_thyroid_monitoring_ind86` (wave 1; proposed model name) |
| IND87 | Bipolar, schizophrenia and other psychoses: lithium levels in therapeutic range | Built | `fct_person_lithium_monitoring_ind87` |
| IND88 | Diabetes: referral for structured education | Built | `fct_person_diabetes_structured_education_ind88` |
| IND89 | Diabetes: annual dietary review | Planned | Diabetes: dietary review code list and agreed review definition. |
| IND90 | Osteoporosis: register | Register | `fct_person_osteoporosis_register` |
| IND91 | Osteoporosis: bone sparing agents (50-74 years) | Built | `fct_person_bone_sparing_therapy_osteoporosis_ind91` (wave 1) |
| IND92 | Osteoporosis: bone sparing agents (75 years and over) | Built | `fct_person_bone_sparing_therapy_fragility_fracture_ind92` (wave 1) |
| IND93 | Peripheral arterial disease: register | Register | `fct_person_pad_register` |
| IND94 | Peripheral arterial disease: antiplatelets | Built | `fct_person_antithrombotic_therapy_ind94` |
| IND95 | Hypertension: assessment of physical activity | Not built | GPPAQ recorded for about 5.5% of eligible people with hypertension in 15 months. |
| IND96 | Hypertension: brief intervention to increase physical activity | Not built | GPPAQ reaches about 5.5% of the hypertension cohort; among recorded less-active people, intervention coding gives about 18% or 65% depending on code scope. |
| IND97 | Smoking: smoking status for people with long-term conditions | Built | `fct_person_smoking_ind97` |
| IND98 | Smoking: support and treatment for people with long-term conditions or SMI | Built | `fct_person_smoking_ind98` (wave 1) |
| IND99 | Smoking: support and treatment (all patients) | Built | `fct_person_smoking_ind99` (wave 1) |
| IND100 | Diabetes: register including type | Register | `fct_person_diabetes_register` |
| IND101 | COPD: offered pulmonary rehabilitation | Planned | COPD: pulmonary rehabilitation offer codes and agreed offer definition. |
| IND102 | Heart failure: referral for cardiac rehabilitation | Not built | Cardiac rehabilitation offers coded for about 1-2% of new HF; clinical exclusions are uncoded. |
| IND103 | Depression and anxiety: biopsychosocial assessment at diagnosis | Not built | Biopsychosocial assessment coded for under 1% of new depression diagnoses. |
| IND104 | Depression and anxiety: review within 10 to 35 days | Built | `fct_person_depression_review_ind104` |
| IND105 | Diabetes: asking about erectile dysfunction | Not built | Erectile dysfunction enquiry coded for about 2.5% of men with diabetes in 15 months; coding has halved twice since 2019. |
| IND106 | Diabetes: advice for erectile dysfunction | Not built | Erectile dysfunction advice or assessment coded for under 1% of men with diabetes and erectile dysfunction. |
| IND107 | Rheumatoid arthritis: register | Register | `fct_person_rheumatoid_arthritis_register` |
| IND108 | Rheumatoid arthritis: cardiovascular risk assessment | Built | `fct_person_cvd_risk_assessment_rheumatoid_arthritis_ind108` (wave 1; proposed model name) |
| IND109 | Rheumatoid arthritis: fracture risk assessment | Not built | Fracture risk assessment recorded for about 2% of eligible people with rheumatoid arthritis. |
| IND110 | Rheumatoid arthritis: annual review | Built | `fct_person_rheumatoid_arthritis_review_ind110` |
| IND111 | Diabetes: annual albumin creatinine test | Built | `fct_person_diabetes_acr_ind111` |
| IND112 | Cardiovascular disease prevention: blood pressure measurement every 5 years | Built | `fct_person_blood_pressure_cvd_prevention_ind112` (wave 1) |
| IND113 | Cancer: 3-month review | Built | `fct_person_cancer_care_review_ind113` (wave 1; proposed model name) |
| IND114 | Dementia: named carer | Not built | OLIDS has no carer contact details; coded proxies cover about 11% with narrow codes or 53% with broad codes. |
| IND115 | Hypertension: confirming diagnosis with HBPM or ABPM | Built | `fct_person_home_ambulatory_bp_hypertension_ind115` (wave 1) |
| IND116 | Contraception: advice for people with diabetes | Planned | Contraception: advice code list for people with diabetes. |
| IND117 | Contraception: advice for people with epilepsy | Built | `fct_person_epilepsy_contraception_advice_ind117` (wave 1) |
| IND118 | Dementia: target organ damage (all patients) | Built | `fct_person_dementia_baseline_tests_ind118` (wave 1) |
| IND119 | Learning disabilities: register | Register | `fct_person_learning_disability_register` |
| IND120 | Diabetes: annual general practice checks | Built | `fct_person_diabetes_care_processes_ind120` |
| IND121 | Hypertension: urinary albumin for target organ damage | Built | `fct_person_urine_acr_hypertension_ind121` (wave 1) |
| IND122 | Hypertension: haematuria for target organ damage | Planned | Hypertension: urine blood test code list and dated evidence. |
| IND123 | Hypertension: ECG for target organ damage | Planned | Hypertension: resting or 12-lead ECG code list and dated evidence. |
| IND124 | Contraception: advice for people with bipolar, schizophrenia or other psychoses | Planned | Contraception: advice code list and sterilisation or hysterectomy evidence for people with SMI. |
| IND125 | Myocardial infarction: medication for MI in preceding 12 months | Built | `fct_person_myocardial_infarction_therapy_ind125` (wave 1) |
| IND126 | Myocardial infarction: medication for MI more than 12 months ago | Built | `fct_person_myocardial_infarction_therapy_ind126` (wave 1) |
| IND127 | Atrial fibrillation: annual stroke risk assessment | Built | `fct_person_atrial_fibrillation_ind127` |
| IND128 | Atrial fibrillation: current treatment with anticoagulation | Built | `fct_person_atrial_fibrillation_ind128` |
| IND129 | Kidney conditions: CKD register (3a to 5) | Register | `fct_person_ckd_register` |
| IND130 | Kidney conditions: CKD and renin–angiotensin system antagonists | Built | `fct_person_ckd_ras_therapy_ind130` |
| IND131 | Immunisation: flu vaccine for people with CHD | Built | `fct_person_flu_vaccination_ind131` |
| IND132 | Angina and coronary heart disease: anti-platelet or anticoagulation | Built | `fct_person_antithrombotic_therapy_ind132` |
| IND133 | Stroke and ischaemic attack: anti-platelet or anticoagulation | Built | `fct_person_antithrombotic_therapy_ind133` |
| IND134 | Diabetes: ACEi or ARBs | Built | `fct_person_diabetes_ras_therapy_ind134` |
| IND135 | Diabetes: IFCC-HbA1c 64mmol/mol or less | Built | `fct_person_diabetes_hba1c_ind135` |
| IND136 | Diabetes: IFCC-HbA1c 75mmol/mol or less | Built | `fct_person_diabetes_hba1c_ind136` |
| IND137 | Diabetes: annual retinal screening | Built | `fct_person_diabetes_retinal_screening_ind137` |
| IND138 | Hypothyroidism: register | Planned | Hypothyroidism: levothyroxine orders and subclinical diagnosis code list. |
| IND139 | Hypothyroidism: annual thyroid function test | Built | `fct_person_hypothyroidism_tft_ind139` |
| IND140 | COPD: FEV1 | Planned | COPD: FEV1 measurement code list and dated results. |
| IND141 | Immunisation: flu vaccine for people with COPD | Built | `fct_person_flu_vaccination_ind141` |
| IND142 | Dementia: care planning | Built | `fct_person_dementia_care_plan_ind142` |
| IND143 | Bipolar, schizophrenia and other psychoses: care planning | Built | `fct_person_smi_care_plan_ind143` |
| IND144 | Kidney conditions: CKD urine albumin:creatinine ratio | Built | `fct_person_ckd_albumin_testing_ind144` |
| IND145 | Epilepsy: seizure free in preceding 12 months | Not built | Seizure-free status recorded for about 3.5% of the epilepsy register in 12 months. |
| IND146 | Hypertension: lifestyle advice | Not built | All three lifestyle advice components coded for about 1.6% in 12 months. |
| IND148 | Contraception: LARC for people on oral or patch contraceptives | Planned | Contraception: practice-issued contraceptive medication evidence and LARC advice code list. |
| IND149 | Contraception: LARC for people using emergency contraception | Planned | Contraception: practice-issued emergency contraception evidence and LARC advice code list. |
| IND150 | Cardiovascular disease prevention: cardiovascular risk assessment for people with bipolar, schizophrenia or other psychoses | Built | `fct_person_smi_cvd_risk_assessment_ind150` |
| IND152 | Immunisation: flu vaccine for people with long-term conditions | Built | `fct_person_flu_vaccination_ind152` |
| IND154 | Smoking: smoking status of people with bipolar, schizophrenia and other psychoses | Built | `fct_person_smi_smoking_ind154` |
| IND155 | Smoking: support and treatment for people with bipolar, schizophrenia and other psychoses | Built | `fct_person_smi_smoking_support_ind155` |
| IND156 | Smoking: smoking status of people with long-term conditions | Built | `fct_person_smoking_ind156` |
| IND157 | Smoking: support and treatment for people with long term conditions | Built | `fct_person_smoking_ind157` |
| IND158 | Bipolar, schizophrenia and other psychoses: annual cholesterol | Built | `fct_person_smi_cholesterol_ind158` |
| IND159 | Bipolar, schizophrenia and other psychoses: annual blood glucose or HbA1c | Built | `fct_person_smi_glucose_ind159` |
| IND160 | Diabetes: annual examination of foot sensation | Built | `fct_person_diabetes_foot_examination_ind160` |
| IND161 | Cardiovascular disease prevention: cardiovascular risk assessment for people newly diagnosed with hypertension or T2DM | Built | `fct_person_cvd_risk_assessment_ind161` |
| IND163 | Immunisation: flu vaccine for people with diabetes | Built | `fct_person_flu_vaccination_ind163` |
| IND164 | Immunisation: flu vaccine for people with stroke or TIA | Built | `fct_person_flu_vaccination_ind164` |
| IND165 | Diabetes: IFCC-HbA1c 58mmol/mol or less | Built | `fct_person_diabetes_hba1c_ind165` |
| IND169 | Atrial fibrillation: review of anticoagulation | Built | `fct_person_atrial_fibrillation_ind169` |
| IND170 | Diabetes: NDH register | Register | `fct_person_ndh_register`. Code route only; NICE laboratory route is not built. |
| IND171 | Diabetes: NDH diabetes prevention programme | Built | `fct_person_ndh_prevention_programme_ind171` |
| IND172 | Diabetes: NDH annual HbA1c or FPG test | Built | `fct_person_ndh_glycaemic_test_ind172` |
| IND173 | Diabetes: gestational diabetes annual HbA1c test | Built | `fct_person_gestational_diabetes_hba1c_ind173` |
| IND174 | Kidney conditions: AKI register | Planned | Acute kidney injury: diagnosis code list and register with monthly history. |
| IND175 | Autism: register | Register | `fct_person_autism_register` |
| IND176 | Screening: cervical screening (25 to 49 years) | Built | `fct_person_cervical_screening_ind176` |
| IND177 | Screening: cervical screening (50 to 64 years) | Built | `fct_person_cervical_screening_ind177` |
| IND178 | Pregnancy and neonates: postnatal mental health | Planned | Maternity: delivery code list and agreed postnatal mental health enquiry codes. |
| IND179 | Diabetes: HbA1c 58 mmol/mol | Built | `fct_person_diabetes_hba1c_ind179` |
| IND180 | Diabetes: HbA1c 75 mmol/mol | Built | `fct_person_diabetes_hba1c_ind180` |
| IND181 | Diabetes: CVD risk assessment | Built | `fct_person_cvd_risk_assessment_ind181` |
| IND185 | Atrial fibrillation: register | Register | `fct_person_atrial_fibrillation_register`. Resolved AF is excluded by the model but included by NICE. |
| IND186 | Asthma: register | Register | `fct_person_asthma_register`. QOF drug rule applies; NICE counts use `nice_asthma_diagnosis_population` as IND273 does. |
| IND189 | Asthma: smoking status (under 19) | Built | `fct_person_asthma_smoking_status_ind189` (wave 1) |
| IND190 | COPD: register | Register | `fct_person_copd_register`. QOF spirometry windows and no-spirometry route differ from NICE. |
| IND191 | COPD: annual review | Built | `fct_person_copd_review_ind191` |
| IND192 | Heart failure: confirmation of diagnosis | Built | `fct_person_heart_failure_confirmation_ind192` (wave 1) |
| IND195 | Heart failure: annual review | Built | `fct_person_heart_failure_review_ind195` |
| IND196 | Alcohol use: risk assessment for people with hypertension | Built | `fct_person_alcohol_ind196` |
| IND197 | Alcohol use: brief intervention for people with hypertension | Built | `fct_person_alcohol_ind197` |
| IND198 | Alcohol use: risk assessment for people with depression or anxiety | Built | `fct_person_alcohol_ind198` |
| IND199 | Alcohol use: brief intervention for people with depression or anxiety | Built | `fct_person_alcohol_ind199` |
| IND200 | Alcohol use: brief intervention for people with SMI | Built | `fct_person_alcohol_ind200` |
| IND201 | Alcohol use: risk assessment for people with a long-term condition | Built | `fct_person_alcohol_ind201` |
| IND202 | Alcohol use: brief intervention for people with a long-term condition | Built | `fct_person_alcohol_ind202` |
| IND203 | Lipids disorders: FH assessment (29 years and under) | Planned | Familial hypercholesterolaemia: shared assessment evidence; age and reading window need agreement. |
| IND204 | Lipids disorders: FH assessment (30 years and over) | Planned | Familial hypercholesterolaemia: shared assessment evidence; age and reading window need agreement. |
| IND205 | Multiple long-term conditions: multimorbidity register | Register | `int_nice_multimorbidity_categories`. Count at least four distinct categories per active person aged 18+; tailored-approach route is uncoded. |
| IND206 | Multiple long-term conditions: frailty register | Register | `fct_person_frailty_register`. Filter `age >= 65` and latest severity 'Moderate' or 'Severe'. |
| IND207 | Multiple long-term conditions: medication review | Built | `fct_person_multimorbidity_ind207` |
| IND208 | Multiple long-term conditions: asking about falls | Built | `fct_person_multimorbidity_ind208` |
| IND210 | HIV: testing at registration | Planned | HIV: test code list, registration date at month-end and geographical scope. |
| IND211 | HIV: routine blood tests | Not built | NICE counts blood-test episodes; latest-day person adaptation gives about 1.8% same-day HIV testing and requires a large laboratory scan. |
| IND212 | COPD: oxygen saturation recording | Planned | COPD: FEV1 percentage predicted and agreed very-severe classification. |
| IND213 | Bipolar, schizophrenia and other psychoses: cervical screening (25 to 49 years) | Built | `fct_person_smi_cervical_screening_ind213` |
| IND214 | Bipolar, schizophrenia and other psychoses: cervical screening (50 to 64 years) | Built | `fct_person_smi_cervical_screening_ind214` |
| IND215 | Immunisation: DTaP (8 months) | Built | `fct_person_childhood_immunisation_ind215` |
| IND216 | Immunisation: MMR (18 months) | Built | `fct_person_childhood_immunisation_ind216` |
| IND217 | Immunisation: DTaP/IPV and MMR (5 years) | Built | `fct_person_childhood_immunisation_ind217` |
| IND218 | Immunisation: MMR (5 years) | Built | `fct_person_childhood_immunisation_ind218` |
| IND219 | Immunisation: shingles | Built | `fct_person_shingles_vaccination_ind219` |
| IND220 | Weight management: referral to weight management programmes for obesity | Planned | Weight management: referral evidence and agreed attendance or referral exclusions. |
| IND221 | Weight management: referral to weight management programmes for obesity (co-existing hypertension or diabetes) | Planned | Weight management: shared referral evidence and exclusions for people with hypertension or diabetes. |
| IND222 | Cancer: review within 3 months | Planned | Cancer: CANPCSUPP_COD support discussion refset and dated evidence. |
| IND223 | Cancer: review within 12 months | Built | `fct_person_cancer_care_review_ind223` |
| IND224 | Immunisation: rotavirus (24 weeks) | Built | `fct_person_childhood_immunisation_ind224` |
| IND225 | Immunisation: meningitis B (8 months) | Built | `fct_person_childhood_immunisation_ind225` |
| IND226 | Immunisation: meningitis B (18 months) | Built | `fct_person_childhood_immunisation_ind226` |
| IND227 | Epilepsy: annual review | Not built | Annual epilepsy reviews coded for about 5-9% of the register. |
| IND228 | Cardiovascular disease prevention: primary prevention with lifestyle changes | Not built | All four lifestyle advice components coded for about 1.8% within three months of a qualifying risk score. |
| IND229 | Cardiovascular disease prevention: primary prevention with lipid lowering therapies | Built | `fct_person_lipid_lowering_therapy_ind229` |
| IND230 | Cardiovascular disease prevention: secondary prevention with lipid lowering therapies | Built | `fct_person_lipid_lowering_therapy_ind230` |
| IND231 | Kidney conditions: CKD and lipid lowering therapies | Built | `fct_person_lipid_lowering_therapy_ind231` |
| IND232 | Kidney conditions: eGFR for long-term NSAID use | Built | `fct_person_nsaid_egfr_ind232` (wave 1; proposed model name) |
| IND233 | Kidney conditions: CKD and eGFR | Built | `fct_person_ckd_new_diagnosis_egfr_ind233` |
| IND234 | Kidney conditions: CKD – eGFR and ACR | Built | `fct_person_ckd_new_diagnosis_tests_ind234` |
| IND235 | Kidney conditions: CKD and blood pressure when ACR less than 70 | Built | `fct_person_ckd_bp_ind235` |
| IND237 | Weight management: overweight register | Planned | Weight management: NICE BMI register with a 12-month window and ethnicity rule. |
| IND238 | Weight management: obesity register | Planned | Weight management: NICE BMI register with a 12-month window and ethnicity rule. |
| IND239 | Hypertension: blood pressure (79 years and under) | Built | `fct_person_bp_control_hypertension_ind239` |
| IND240 | Hypertension: blood pressure (80 years and over) | Built | `fct_person_bp_control_hypertension_ind240` |
| IND241 | Angina and coronary heart disease: blood pressure (79 years and under) | Built | `fct_person_bp_control_chd_ind241` |
| IND242 | Angina and coronary heart disease: blood pressure (80 years and over) | Built | `fct_person_bp_control_chd_ind242` |
| IND243 | Stroke and ischaemic attack: blood pressure (79 years and under) | Built | `fct_person_bp_control_stroke_tia_ind243` |
| IND244 | Stroke and ischaemic attack: blood pressure (80 years and over) | Built | `fct_person_bp_control_stroke_tia_ind244` |
| IND245 | Peripheral arterial disease: blood pressure (79 years and under) | Built | `fct_person_bp_control_pad_ind245` |
| IND246 | Peripheral arterial disease: blood pressure (80 years and over) | Built | `fct_person_bp_control_pad_ind246` |
| IND247 | Atrial fibrillation: DOACs and Vitamin K antagonists | Built | `fct_person_atrial_fibrillation_ind247` |
| IND248 | Bipolar, schizophrenia and other psychoses: 6 physical health checks | Built | `fct_person_smi_physical_health_ind248` |
| IND249 | Diabetes: blood pressure (without moderate or severe frailty) | Built | `fct_person_diabetes_bp_ind249` |
| IND250 | Cancer: register | Register | `fct_person_cancer_register`. Model applies a 1 April 2003 date floor absent from NICE. |
| IND251 | Coronary heart disease: register | Register | `fct_person_chd_register` |
| IND252 | Dementia: register | Register | `fct_person_dementia_register` |
| IND253 | Epilepsy: register | Register | `fct_person_epilepsy_register` |
| IND254 | Heart failure: register | Register | `fct_person_heart_failure_register`. Filter `age >= 18`. |
| IND255 | Hypertension: register | Register | `fct_person_hypertension_register` |
| IND256 | Bipolar, schizophrenia and other psychoses: register (lithium therapy) | Register | `fct_person_smi_register`. Union with `int_nice_ltc_population.is_on_lithium`; six-month treatment rule applies only to the lithium arm. |
| IND257 | Bipolar, schizophrenia and other psychoses: register | Register | `fct_person_smi_register` |
| IND258 | GP services: palliative care register | Register | `fct_person_palliative_care_register`. Model applies a 1 April 2008 date floor absent from NICE. |
| IND259 | Stroke and ischaemic attack: register | Register | `fct_person_stroke_tia_register` |
| IND260 | Lipid disorders: FH assessment and diagnosis (historical readings) | Planned | Familial hypercholesterolaemia: assessment, referral and secondary hyperlipidaemia code lists. |
| IND261 | Lipid disorders: FH assessment and diagnosis (new readings) | Planned | Familial hypercholesterolaemia: shared assessment and referral evidence for new readings. |
| IND263 | Kidney conditions: CKD - ACEi and ARB | Built | `fct_person_ckd_ras_therapy_ind263` |
| IND264 | Kidney conditions: CKD and blood pressure when ACR 70 or more | Built | `fct_person_ckd_bp_ind264` |
| IND265 | Learning disabilities: health checks and action plans | Built | `fct_person_learning_disability_health_check_ind265` |
| IND266 | Learning disabilities: health checks, action plans and ethnicity | Built | `fct_person_learning_disability_health_check_ind266` |
| IND267 | Cancer: faecal immunochemical testing | Built | `fct_person_colorectal_cancer_fit_ind267` (wave 1) |
| IND269 | Cardiovascular disease prevention: risk assessment (general population) | Built | `fct_person_cvd_risk_assessment_ind269` |
| IND270 | Cardiovascular disease prevention: risk assessment (modifiable risk factors) | Built | `fct_person_cvd_risk_assessment_ind270` |
| IND272 | Asthma: objective tests | Built | `fct_person_asthma_objective_tests_ind272` (wave 1) |
| IND273 | Asthma: annual review | Built | `fct_person_asthma_review_ind273` |
| IND274 | Diabetes: lipid-lowering therapies for primary prevention of CVD (T2DM and 10% risk) | Built | `fct_person_lipid_lowering_therapy_ind274` |
| IND275 | Diabetes: lipid-lowering therapies for primary prevention of CVD (40 years and over) | Built | `fct_person_lipid_lowering_therapy_ind275` |
| IND276 | Diabetes: lipid-lowering therapies for secondary prevention of CVD | Built | `fct_person_lipid_lowering_therapy_ind276` |
| IND277 | Diabetes: T1DM and lipid-lowering therapies | Built | `fct_person_lipid_lowering_therapy_ind277` |
| IND278 | Cardiovascular disease prevention: cholesterol treatment target (secondary prevention) | Built | `fct_person_cholesterol_control_ind278` |
| IND287 | Cardiovascular disease prevention: lipid lowering therapy for people newly diagnosed with hypertension or T2DM | Built | `fct_person_lipid_lowering_therapy_ind287` |
| IND315 | Asthma: annual review (higher risk patients) | Built | `fct_person_asthma_higher_risk_review_ind315` (wave 1) |
| IND316 | Asthma: MART (higher risk patients) | Planned | Asthma: ICS/formoterol and MART code lists, plus higher-risk evidence. |
| IND317 | Heart failure: 4 pillars (HFrEF) | Built | `fct_person_heart_failure_four_pillars_ind317` (wave 1) |
| IND318 | Heart failure: ejection fraction category | Planned | Heart failure: ejection fraction category code list and dated evidence. |
| IND319 | Weight management: advice for people living with overweight (18 to 39 years) | Built | `fct_person_weight_management_advice_overweight_ind319` (wave 1) |
| IND320 | Weight management: BMI recording (long term conditions) | Built | `fct_person_bmi_recording_ind320` |
| IND321 | Screening: cervical (25 to 64 years) | Built | `fct_person_cervical_screening_ind321` |
| IND323 | Infections: scoring tools for sore throat | Not built | NICE counts diagnosis episodes; only about 4% of sore-throat diagnosis-days have a same-day score, and Pharmacy First records cannot be separated reliably. |
| IND324 | Kidney conditions: CKD and SGLT2 inhibitors | Built | `fct_person_ckd_sglt2_therapy_ind324` |
