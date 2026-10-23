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

    Risk summary contains adult MoC (including Check & Test-only LR) and CYP-only asthma members, including inactive and deceased people; filter is_active.
    Adult stage 2 uses sharing, or MDT/sharing for HRCS and HRS.
    CYP-only HR desktop review follows the EMIS HR description: due three months after the latest annual or desktop review, once the annual review is done.
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
        COMMENT = 'Adult MoC (including Check & Test-only LR) and CYP-only asthma members with risk and observation-based Model of Care status'
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
    rs.af_risk_group AS af_risk_group COMMENT = 'AF LTC LCS risk group: HR or MR when stratified.',
    rs.asthma_adult_risk_group AS asthma_adult_risk_group COMMENT = 'Adult asthma risk group',
    rs.asthma_cyp_risk_group AS asthma_cyp_risk_group COMMENT = 'Children and young people asthma risk group',
    rs.chd_risk_group AS chd_risk_group COMMENT = 'CHD risk group',
    rs.ckd_risk_group AS ckd_risk_group COMMENT = 'CKD risk group',
    rs.copd_risk_group AS copd_risk_group COMMENT = 'COPD risk group',
    rs.diabetes_risk_group AS diabetes_risk_group COMMENT = 'Diabetes risk group: HRC, HR, MR, MRa, MRb or LR; Type 1 HRC is separate',
    rs.hf_risk_group AS hf_risk_group COMMENT = 'Heart failure risk group',
    rs.hypertension_risk_group AS hypertension_risk_group COMMENT = 'Hypertension risk group: HRC, HR, MRa, MRb or LR',
    rs.nafld_risk_group AS nafld_risk_group COMMENT = 'NAFLD risk group',
    rs.pad_risk_group AS pad_risk_group COMMENT = 'PAD risk group',
    rs.stroke_tia_risk_group AS stroke_tia_risk_group COMMENT = 'Stroke/TIA risk group',
    rs.overall_risk_group AS overall_risk_group COMMENT = 'Highest LTC LCS risk across conditions: HRC (high risk complex), HR, MR, or LR. Unstratified Model of Care members default to LR.',
    rs.overall_risk_rank AS overall_risk_rank COMMENT = 'Risk rank: 1=HRC, 2=HR, 3=MR, 4=MRa/MRb, 5=LR.',
    rs.moc_pathway AS moc_pathway COMMENT = 'Model of Care pathway: HRCS, HRS, MRS or LRS',
    rs.moc_risk_category AS moc_risk_category COMMENT = 'Model of Care risk category',
    rs.moc_pathway_status AS moc_pathway_status COMMENT = 'Observation-based work-list pending, Declined, Cycle complete, or No action identified',
    rs.moc_stage_completed_label AS moc_stage_completed_label COMMENT = 'Highest completed adult stage including accepted declines, or CYP Annual Review/Desktop Review.',
    rs.moc_next_action AS moc_next_action COMMENT = 'Next observation-based EMIS work-list action: Check & Test (C&T), MDT Review (MDT), Care Plan Sharing (CPS), Discussion (DISC), Follow-up (F/U), Annual Review (AR) or Desktop Review (DTR). Null when no list selects the person. Future care-plan diaries are unavailable',
    rs.moc_cycle_complete AS moc_cycle_complete COMMENT = 'Project measure: adults need an agreed plan and review within 12 months, latest MoC event a review, and no observation-based work list; CYP-only asthma children need the annual review, plus the desktop review if HR; Check & Test-only people need the C&T.',
    rs.moc_any_activity_12m AS moc_any_activity_12m COMMENT = 'Any Model of Care activity in the last 12 months',
    rs.moc_declined AS moc_declined COMMENT = 'Model of Care declined flag',
    rs.is_in_moc_base AS is_in_moc_base COMMENT = 'In the adult MoC base, including Check & Test-only LR members. False for CYP-only asthma rows.',
    rs.is_moc_check_test_only AS is_moc_check_test_only COMMENT = 'LR hypertension-only or LR adult-asthma-only; in the MoC base, excluded from the EMIS MoC base search, needs only the annual Check & Test.',
    rs.is_type_1_diabetes_hrc AS is_type_1_diabetes_hrc COMMENT = 'Qualifies for the separate EMIS Type 1 HRC rule; not added to diabetes_risk_group.',
    rs.moc_mdt_review_completed AS moc_mdt_review_completed COMMENT = 'Actual annual MDT review event in the inclusive trailing 12 months; separate from stage 2 completion.',
    rs.moc_mdt_review_date AS moc_mdt_review_date COMMENT = 'Latest actual annual MDT review date within the inclusive trailing 12 months; null if absent.',
    rs.moc_stage_1_completed AS moc_stage_1_completed COMMENT = 'Adult pathway C&T completion, using its strict or inclusive annual window. Includes Check & Test-only people; false for CYP-only people.',
    rs.moc_stage_3_completed AS moc_stage_3_completed COMMENT = 'Adult stage 3: agreed plan or decline in the inclusive trailing 12 months. No stage 1 or 2 prerequisite.',
    rs.moc_stage_4_completed AS moc_stage_4_completed COMMENT = 'Adult stage 4: decline, or both agreed plan and review in the inclusive trailing 12 months. No stage 1 or 2 prerequisite.',
    rs.moc_requires_check_test AS moc_requires_check_test COMMENT = 'Observation-based EMIS stage 1 list. Old C&T can require a new C&T despite a more recent decline.',
    rs.moc_requires_stage_2 AS moc_requires_stage_2 COMMENT = 'Latest E in the inclusive trailing 12 months is C, strictly after the annual anniversary. Future diaries are unavailable.',
    rs.moc_requires_mdt_review AS moc_requires_mdt_review COMMENT = 'HRCS/HRS stage 2 list with no MDT strictly after the 12-month anniversary.',
    rs.moc_requires_careplan_sharing AS moc_requires_careplan_sharing COMMENT = 'MRS/LRS stage 2 list, or HRCS/HRS stage 2 list with MDT strictly after the anniversary and no sharing on or after it.',
    rs.moc_requires_discussion AS moc_requires_discussion COMMENT = 'Latest E in the inclusive trailing 12 months is S. Future diaries are unavailable.',
    rs.moc_requires_followup AS moc_requires_followup COMMENT = 'Latest E is not D/R/C/S and latest E plus offered in the inclusive trailing 12 months is A. Future diaries are unavailable.',
    rs.moc_ct_overdue AS moc_ct_overdue COMMENT = 'Latest C strictly within 12 months, latest B is C, and C is more than 42 days old. B is C/A/R/D/O across all history.',
    rs.moc_discussion_overdue AS moc_discussion_overdue COMMENT = 'No C on or before the 12-month anniversary, latest B is A, and A is more than 3 months old. B is C/A/R/D/O across all history.',
    rs.cyp_annual_review_completed AS cyp_annual_review_completed COMMENT = 'CYP-only annual asthma review strictly after the one-year anniversary. False for adult pathways.',
    rs.cyp_requires_annual_review AS cyp_requires_annual_review COMMENT = 'CYP-only person without an annual asthma review strictly after the one-year anniversary.',
    rs.cyp_requires_desktop_review AS cyp_requires_desktop_review COMMENT = 'CYP-only HR: annual review done, no completed desktop review, and the latest annual or desktop review was 3 or more months ago. Project rule from the EMIS HR description; the exported search selects no one.',
    rs.cyp_desktop_review_completed AS cyp_desktop_review_completed COMMENT = 'CYP-only HR: latest desktop review or latest annual review, after the annual anniversary with another AR/DTR in [review minus 3 months, review minus 1 day). Requires desktop review follows the HR description; see cyp_requires_desktop_review.',
    rs.cyp_annual_review_date AS cyp_annual_review_date COMMENT = 'Latest non-future CYP-only asthma annual review date; may be outside the completed window. Null for adult pathways or no event.',
    rs.cyp_desktop_review_date AS cyp_desktop_review_date COMMENT = 'Latest qualifying CYP-only HR desktop review or linked annual review date; null without completion or for other pathways.',
    rs.moc_has_missing_priors AS moc_has_missing_priors COMMENT = 'Prior Model of Care stages are missing',
    rs.moc_check_test_completed_12m AS moc_check_test_completed_12m COMMENT = 'Check and test completed in the last 12 months',
    rs.moc_stage_2_completed AS moc_stage_2_completed COMMENT = 'C&T plus sharing for MR/LR; C&T plus MDT or sharing for HRC/HR, using pathway windows.',
    rs.moc_discussion_completed_12m AS moc_discussion_completed_12m COMMENT = 'Discussion completed in the last 12 months',
    rs.moc_followup_completed_12m AS moc_followup_completed_12m COMMENT = 'Follow-up completed in the last 12 months',

    -- Core Demographics
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = '5-year age bands',
    demographics.age_band_10y AS age_band_10y COMMENT = '10-year age bands',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'NHS Digital standard age bands',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category: Asian, Black, Mixed, Other, White, or Unknown',
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
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile text label: ''Most Deprived'', ''Second Most Deprived'', ''Third Most Deprived'', ''Second Least Deprived'', ''Least Deprived'', or ''Unknown'''
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
    rs.stratified_patients AS COUNT(DISTINCT CASE WHEN rs.is_in_moc_base THEN rs.person_id END) COMMENT = 'Adult MoC base population, including Check & Test-only LR members; excludes CYP-only asthma',
    rs.hrc_patients AS COUNT(DISTINCT CASE WHEN rs.is_in_moc_base AND rs.overall_risk_group = 'HRC' THEN rs.person_id END) COMMENT = 'HRC patients',
    rs.hr_patients AS COUNT(DISTINCT CASE WHEN rs.is_in_moc_base AND rs.overall_risk_group = 'HR' THEN rs.person_id END) COMMENT = 'HR patients',
    rs.mr_patients AS COUNT(DISTINCT CASE WHEN rs.is_in_moc_base AND rs.overall_risk_group = 'MR' THEN rs.person_id END) COMMENT = 'MR patients',
    rs.lr_patients AS COUNT(DISTINCT CASE WHEN rs.is_in_moc_base AND rs.overall_risk_group = 'LR' THEN rs.person_id END) COMMENT = 'LR patients',
    rs.cycle_complete_patients AS COUNT(DISTINCT CASE WHEN rs.moc_cycle_complete THEN rs.person_id END) COMMENT = 'Patients with a complete Model of Care cycle',
    rs.any_activity_12m_patients AS COUNT(DISTINCT CASE WHEN rs.moc_any_activity_12m THEN rs.person_id END) COMMENT = 'Patients with Model of Care activity in the last 12 months',
    rs.avg_ltc_lcs_conditions AS AVG(rs.number_of_ltc_lcs_conditions) COMMENT = 'Average number of LTC LCS conditions'
)

COMMENT = 'OLIDS LTC LCS Semantic View - case-finding candidates, and adult MoC (including Check & Test-only LR) and CYP-only asthma members with Model of Care progress. Grain: one row per person in each element; cf (case finding) and rs (risk summary) are person-grain but DIFFERENT populations - never mix their elements in one query. Pathway-neutral stage flags avoid double counting the same review event. Date dimensions are moc_mdt_review_date (latest actual annual MDT review within the inclusive trailing 12 months; null if absent), cyp_annual_review_date (latest non-future CYP-only asthma annual review date; may be outside the completed window; null for adult pathways or no event) and cyp_desktop_review_date (latest qualifying CYP-only HR desktop review or linked annual review date; null without completion or for other pathways). The *_12m flags remain boolean recency indicators.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. Case finding and risk summary are different populations; never mix cf and rs elements. Example: SELECT borough_resident, AGG(stratified_patients) FROM SEM_OLIDS_LTC_LCS WHERE is_active = TRUE GROUP BY borough_resident. Example linkage: reduce case-finding patients here and DNA patients in sem_olids_appointments before joining. Risk summary includes inactive and deceased people. Use the pathway-neutral Model of Care flags; hypertension has no indicator 64.'
AI_QUESTION_CATEGORIZATION 'Use this view for: LTC LCS case-finding indicators and candidates, risk stratification (HRC/HR/MR/LR), Model of Care pathway progress including check and test, care plans, discussions, follow-ups, CYP annual and desktop review, and cycle completion.'
