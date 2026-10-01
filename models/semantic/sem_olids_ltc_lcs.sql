{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS LTC LCS Semantic View
    ===========================

    Case finding contains candidates with at least one indicator who are not
    necessarily diagnosed. Risk summary contains the risk-stratified LTC LCS
    register population and Model of Care activity. Do not mix their elements
    in one query: they are different populations.

    Risk summary includes inactive and deceased people; filter is_active.
    The same review event can surface as remote desktop or MDT review by
    pathway, so only pathway-neutral stage flags are exposed.
#}

TABLES(
    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics, geography, ethnicity, deprivation (current snapshot)',

    cf AS {{ ref('fct_person_ltc_lcs_case_finding') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Case-finding candidates with one or more programme indicators; excludes deceased and de-registered people',

    rs AS {{ ref('fct_person_ltc_lcs_risk_summary') }}
        PRIMARY KEY (person_id)
        COMMENT = 'People on one or more LTC LCS registers with risk and Model of Care status; includes deceased and inactive people'
)

RELATIONSHIPS(
    cf (person_id) REFERENCES demographics,
    rs (person_id) REFERENCES demographics
)

FACTS(
    cf.case_finding_count AS case_finding_count COMMENT = 'Number of case-finding indicators for the candidate',
    rs.number_of_ltc_lcs_conditions AS number_of_ltc_lcs_conditions COMMENT = 'Number of LTC LCS conditions on the register'
)

DIMENSIONS(
    -- Person linkage key
    demographics.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Case-finding indicators: programme-specific numbering
    cf.in_af_61 AS in_af_61 COMMENT = 'AF case-finding indicator 61',
    cf.in_af_62 AS in_af_62 COMMENT = 'AF case-finding indicator 62',
    cf.in_hf_61 AS in_hf_61 COMMENT = 'Heart failure case-finding indicator 61',
    cf.in_ckd_61 AS in_ckd_61 COMMENT = 'CKD case-finding indicator 61',
    cf.in_ckd_62 AS in_ckd_62 COMMENT = 'CKD case-finding indicator 62',
    cf.in_ckd_63 AS in_ckd_63 COMMENT = 'CKD case-finding indicator 63',
    cf.in_ckd_64 AS in_ckd_64 COMMENT = 'CKD case-finding indicator 64',
    cf.in_cvd_61 AS in_cvd_61 COMMENT = 'CVD case-finding indicator 61',
    cf.in_cvd_62 AS in_cvd_62 COMMENT = 'CVD case-finding indicator 62',
    cf.in_cvd_63 AS in_cvd_63 COMMENT = 'CVD case-finding indicator 63',
    cf.in_cvd_64 AS in_cvd_64 COMMENT = 'CVD case-finding indicator 64',
    cf.in_cvd_65 AS in_cvd_65 COMMENT = 'CVD case-finding indicator 65',
    cf.in_cvd_66 AS in_cvd_66 COMMENT = 'CVD case-finding indicator 66',
    cf.in_dm_61 AS in_dm_61 COMMENT = 'Diabetes case-finding indicator 61',
    cf.in_dm_62 AS in_dm_62 COMMENT = 'Diabetes case-finding indicator 62',
    cf.in_dm_63 AS in_dm_63 COMMENT = 'Diabetes case-finding indicator 63',
    cf.in_dm_64 AS in_dm_64 COMMENT = 'Diabetes case-finding indicator 64',
    cf.in_dm_65 AS in_dm_65 COMMENT = 'Diabetes case-finding indicator 65',
    cf.in_dm_66 AS in_dm_66 COMMENT = 'Diabetes case-finding indicator 66',
    cf.in_htn_61 AS in_htn_61 COMMENT = 'Hypertension case-finding indicator 61',
    cf.in_htn_62 AS in_htn_62 COMMENT = 'Hypertension case-finding indicator 62',
    cf.in_htn_63 AS in_htn_63 COMMENT = 'Hypertension case-finding indicator 63',
    cf.in_htn_65 AS in_htn_65 COMMENT = 'Hypertension case-finding indicator 65',
    cf.in_htn_66 AS in_htn_66 COMMENT = 'Hypertension case-finding indicator 66',
    cf.in_cyp_ast_61 AS in_cyp_ast_61 COMMENT = 'Children and young people asthma case-finding indicator 61',
    cf.in_any_case_finding AS in_any_case_finding COMMENT = 'At least one case-finding indicator; always TRUE in this table',

    -- LTC LCS risk and Model of Care
    rs.af_risk_group AS af_risk_group COMMENT = 'AF LTC LCS risk group: HRC, HR, MR, or LR when stratified.',
    rs.asthma_adult_risk_group AS asthma_adult_risk_group COMMENT = 'Adult asthma risk group',
    rs.asthma_cyp_risk_group AS asthma_cyp_risk_group COMMENT = 'Children and young people asthma risk group',
    rs.chd_risk_group AS chd_risk_group COMMENT = 'CHD risk group',
    rs.ckd_risk_group AS ckd_risk_group COMMENT = 'CKD risk group',
    rs.copd_risk_group AS copd_risk_group COMMENT = 'COPD risk group',
    rs.diabetes_risk_group AS diabetes_risk_group COMMENT = 'Diabetes risk group',
    rs.hf_risk_group AS hf_risk_group COMMENT = 'Heart failure risk group',
    rs.hypertension_risk_group AS hypertension_risk_group COMMENT = 'Hypertension risk group',
    rs.nafld_risk_group AS nafld_risk_group COMMENT = 'NAFLD risk group',
    rs.pad_risk_group AS pad_risk_group COMMENT = 'PAD risk group',
    rs.stroke_tia_risk_group AS stroke_tia_risk_group COMMENT = 'Stroke/TIA risk group',
    rs.overall_risk_group AS overall_risk_group COMMENT = 'Highest LTC LCS risk across conditions: HRC (high risk complex), HR, MR, or LR. Unstratified Model of Care members default to LR.',
    rs.overall_risk_rank AS overall_risk_rank COMMENT = 'Risk rank: 1=HRC, 2=HR, 3/4=MR subgroups, 5=LR.',
    rs.moc_pathway AS moc_pathway COMMENT = 'Model of Care pathway: HRCS, HRS, MRS, or LRS',
    rs.moc_risk_category AS moc_risk_category COMMENT = 'Model of Care risk category',
    rs.moc_pathway_status AS moc_pathway_status COMMENT = 'Model of Care pathway status: Declined, Cycle complete, In progress, or Not started',
    rs.moc_stage_completed_label AS moc_stage_completed_label COMMENT = 'Furthest recorded Model of Care stage: Not started, Check & Test, Remote Desktop Review, MDT Review, Care Plan Sharing, Discussion, or Follow-up.',
    rs.moc_next_action AS moc_next_action COMMENT = 'Next Model of Care action',
    rs.moc_cycle_complete AS moc_cycle_complete COMMENT = 'Model of Care cycle complete flag',
    rs.moc_any_activity_12m AS moc_any_activity_12m COMMENT = 'Any Model of Care activity in the last 12 months',
    rs.moc_declined AS moc_declined COMMENT = 'Model of Care declined flag',
    rs.moc_re_engaged_after_decline AS moc_re_engaged_after_decline COMMENT = 'Re-engaged after a previous decline',
    rs.moc_has_missing_priors AS moc_has_missing_priors COMMENT = 'Prior Model of Care stages are missing',
    rs.moc_check_test_completed_12m AS moc_check_test_completed_12m COMMENT = 'Check and test completed in the last 12 months',
    rs.moc_stage_2_completed AS moc_stage_2_completed COMMENT = 'Stage 2 complete: Remote Desktop Review for MR/LR; both MDT Review and Care Plan Sharing for HRC/HR.',
    rs.moc_discussion_completed_12m AS moc_discussion_completed_12m COMMENT = 'Discussion completed in the last 12 months',
    rs.moc_followup_completed_12m AS moc_followup_completed_12m COMMENT = 'Follow-up completed in the last 12 months',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.ethnicity_subcategory AS ethnicity_subcategory COMMENT = 'Ethnicity subcategory (detailed groupings)',
    demographics.main_language AS main_language COMMENT = 'Main spoken language (Not Recorded if unknown)',
    demographics.interpreter_needed AS interpreter_needed COMMENT = 'Whether interpreter is required',
    demographics.is_active AS is_active COMMENT = 'Currently registered. Risk summary includes inactive/deceased people — filter TRUE.',
    demographics.is_deceased AS is_deceased COMMENT = 'Deceased status',

    -- Organisation (registered practice)
    demographics.registered_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the patient''s registered GP practice',
    demographics.registered_practice_name AS practice_name COMMENT = 'Name of the patient''s registered GP practice',
    demographics.registered_pcn_code AS pcn_code COMMENT = 'PCN code of the registered practice',
    demographics.registered_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the registered practice',
    demographics.registered_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Registered PCN name with borough prefix',
    demographics.borough_registered AS borough_registered COMMENT = 'Registration borough',
    demographics.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB ODS code of the registered practice: 93C = NHS North Central London; W2U3Z = NHS North West London. NULL outside the WNL footprint.',
    demographics.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name. NULL outside the WNL footprint.',
    demographics.neighbourhood_registered AS neighbourhood_registered COMMENT = 'Registration neighbourhood',

    -- Geography and deprivation (residence)
    demographics.borough_resident AS borough_resident COMMENT = 'Residence borough',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Residence neighbourhood',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least)',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)'
)

METRICS(
    cf.case_finding_patients AS COUNT(DISTINCT cf.person_id) COMMENT = 'Case-finding candidates',
    cf.avg_indicators_per_person AS AVG(cf.case_finding_count) COMMENT = 'Average case-finding indicators per candidate',
    cf.af_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_af_61 OR cf.in_af_62 THEN cf.person_id END) COMMENT = 'Candidates with an AF indicator',
    cf.hf_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_hf_61 THEN cf.person_id END) COMMENT = 'Candidates with a heart failure indicator',
    cf.ckd_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_ckd_61 OR cf.in_ckd_62 OR cf.in_ckd_63 OR cf.in_ckd_64 THEN cf.person_id END) COMMENT = 'Candidates with a CKD indicator',
    cf.cvd_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_cvd_61 OR cf.in_cvd_62 OR cf.in_cvd_63 OR cf.in_cvd_64 OR cf.in_cvd_65 OR cf.in_cvd_66 THEN cf.person_id END) COMMENT = 'Candidates with a CVD indicator',
    cf.dm_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_dm_61 OR cf.in_dm_62 OR cf.in_dm_63 OR cf.in_dm_64 OR cf.in_dm_65 OR cf.in_dm_66 THEN cf.person_id END) COMMENT = 'Candidates with a diabetes indicator',
    cf.htn_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_htn_61 OR cf.in_htn_62 OR cf.in_htn_63 OR cf.in_htn_65 OR cf.in_htn_66 THEN cf.person_id END) COMMENT = 'Candidates with a hypertension indicator',
    cf.cyp_asthma_case_finding_count AS COUNT(DISTINCT CASE WHEN cf.in_cyp_ast_61 THEN cf.person_id END) COMMENT = 'Candidates with a children and young people asthma indicator',
    rs.stratified_patients AS COUNT(DISTINCT rs.person_id) COMMENT = 'Risk-stratified LTC LCS register population',
    rs.hrc_patients AS COUNT(DISTINCT CASE WHEN rs.overall_risk_group = 'HRC' THEN rs.person_id END) COMMENT = 'HRC patients',
    rs.hr_patients AS COUNT(DISTINCT CASE WHEN rs.overall_risk_group = 'HR' THEN rs.person_id END) COMMENT = 'HR patients',
    rs.mr_patients AS COUNT(DISTINCT CASE WHEN rs.overall_risk_group = 'MR' THEN rs.person_id END) COMMENT = 'MR patients',
    rs.lr_patients AS COUNT(DISTINCT CASE WHEN rs.overall_risk_group = 'LR' THEN rs.person_id END) COMMENT = 'LR patients',
    rs.cycle_complete_patients AS COUNT(DISTINCT CASE WHEN rs.moc_cycle_complete THEN rs.person_id END) COMMENT = 'Patients with a complete Model of Care cycle',
    rs.any_activity_12m_patients AS COUNT(DISTINCT CASE WHEN rs.moc_any_activity_12m THEN rs.person_id END) COMMENT = 'Patients with Model of Care activity in the last 12 months',
    rs.avg_ltc_lcs_conditions AS AVG(rs.number_of_ltc_lcs_conditions) COMMENT = 'Average number of LTC LCS conditions'
)

COMMENT = 'OLIDS LTC LCS Semantic View - case-finding candidates and the risk-stratified LTC LCS register population with Model of Care progress. Grain: one row per person in each element; cf (case finding) and rs (risk summary) are person-grain but DIFFERENT populations - never mix their elements in one query. Pathway-neutral stage flags avoid double counting the same review event. No date dimensions are exposed; recency is available only via the *_12m boolean flags.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. Case finding and risk summary are different populations; never mix cf and rs elements. Example: SELECT borough_resident, AGG(stratified_patients) FROM SEM_OLIDS_LTC_LCS WHERE is_active = TRUE GROUP BY borough_resident. Example linkage: reduce case-finding patients here and DNA patients in sem_olids_appointments before joining. Risk summary includes inactive and deceased people. Use the pathway-neutral Model of Care flags; hypertension has no indicator 64.'
AI_QUESTION_CATEGORIZATION 'Use this view for: LTC LCS case-finding indicators and candidates, risk stratification (HRC/HR/MR/LR), Model of Care pathway progress including check and test, care plans, discussions and follow-ups, and cycle completion.'
