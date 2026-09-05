{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    OLIDS Prescribing Semantic View
    ================================

    Medication order-level semantic model for prescribing analysis. OLIDS is
    the One London Integrated Data Set — primary care data from system suppliers
    (currently EMIS Web, with TPP to follow), unified by the One London team.

    Grain: One row per medication order (issue)

    Core table (int_medication_order_bnf) pre-joins:
    - SNOMED → BNF code mapping for chapter/section filtering
    - Medication statement for prescription type (acute/repeat) and active status

    BNF hierarchy: bnf_chapter (2-digit) → bnf_section (4-digit) → bnf_paragraph (6-digit) → bnf_code (full).
    The chatbot has a BNF lookup tool — ask it to resolve drug class names to BNF codes.

    Pre-defined medication sets (joined via medication_order_id):
    - Statins: with intensity classification (high/moderate/combination)
    - Antihypertensives, ACE inhibitors, ARBs, beta-blockers
    - Anticoagulants: with DOAC/VKA classification
    - Antiplatelets
    - Antipsychotics, antidepressants, lithium, epilepsy drugs
    - Inhaled corticosteroids (ICS)
    - Valproate: with product type, indication, dose category (clinical safety)
    - Diabetes medications
    - GLP-1 receptor agonists: by active ingredient (semaglutide, tirzepatide, etc.)
    - SGLT2 inhibitors: by active ingredient (dapagliflozin, empagliflozin, etc.)
    - DPP-4 inhibitors: by active ingredient (sitagliptin, linagliptin, etc.)
    - Metformin: incl. fixed-dose combinations (combo-inclusive count)
    - Respiratory: inhaled corticosteroids (ICS)
    - NSAIDs, PPIs
    - Antibacterials

    Use Cases:
    - Prescribing volume/cost by BNF chapter, practice, PCN
    - Statin prescribing rates and intensity mix
    - Antibiotic prescribing patterns
    - Valproate safety monitoring (pregnancy risk)
    - Repeat vs acute prescribing patterns
    - Cost per patient by therapeutic area
    - Equity: prescribing by deprivation/ethnicity
#}

TABLES(
    rx AS {{ ref('int_medication_order_bnf') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'All medication orders enriched with BNF classification and prescription type. bnf_chapter is the top-level filter (e.g. 02=Cardiovascular, 04=CNS, 06=Endocrine). The chatbot has a BNF lookup tool to resolve drug names to codes.',

    demographics AS {{ ref('dim_person_demographics') }}
        PRIMARY KEY (person_id)
        COMMENT = 'Patient demographics core (current snapshot). Condition, vulnerability, and polypharmacy cohorts come from sem_olids_population via a person_id CTE join.',

    practice AS {{ ref('dim_practice') }}
        PRIMARY KEY (practice_code)
        COMMENT = 'Practice details for the prescribing practice',

    -- Pre-defined medication category models (each filtered to specific drug class)
    statins AS {{ ref('int_statin_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Statin orders (BNF 2.12) with intensity classification: HIGH_INTENSITY (atorvastatin, rosuvastatin), MODERATE_INTENSITY (simvastatin, pravastatin, fluvastatin), COMBINATION (statin+ezetimibe)',

    antihypertensives AS {{ ref('int_antihypertensive_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antihypertensive medication orders (SNOMED cluster: ANTIHYPERTENSIVE_MEDICATIONS)',

    anticoagulants AS {{ ref('int_anticoagulant_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Anticoagulant orders with DOAC/VKA classification',

    antiplatelets AS {{ ref('int_antiplatelet_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antiplatelet medication orders',

    antipsychotics AS {{ ref('int_antipsychotic_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antipsychotic medication orders (BNF 4.2, excludes lithium)',

    antidepressants AS {{ ref('int_antidepressant_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antidepressant medication orders',

    valproate AS {{ ref('int_valproate_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Valproate orders with product type (sodium valproate, valproic acid), indication (anti-epileptic, mood stabiliser), dose category, and teratogenic risk flag',

    diabetes_meds AS {{ ref('int_diabetes_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Diabetes medication orders (BNF 6.1). GLP-1 RAs broken out via glp1_drug / is_glp1_ra.',

    glp1 AS {{ ref('int_glp1_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'GLP-1 receptor agonist orders (SNOMED cluster GLP1RA_RX) categorised to active ingredient (glp1_drug). Captures both diabetes (BNF 6.1) and obesity (BNF 4.5) indications; use is_diabetes_indication to split. Tirzepatide flagged via is_dual_gip_glp1.',

    sglt2 AS {{ ref('int_sglt2_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'SGLT2 inhibitor orders (SNOMED cluster SGLT2I_RX) categorised to active ingredient (sglt2_drug: dapagliflozin, empagliflozin, canagliflozin, ertugliflozin). Includes fixed-dose combinations. Prescribed across diabetes/HF/CKD — indication comes from registers, not the drug.',

    dpp4 AS {{ ref('int_dpp4_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'DPP-4 inhibitor (gliptin) orders (SNOMED cluster DPP4I_RX) categorised to active ingredient (dpp4_drug: sitagliptin, linagliptin, saxagliptin, vildagliptin, alogliptin). Includes fixed-dose combinations.',

    metformin AS {{ ref('int_metformin_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Metformin orders (SNOMED cluster METFORMIN_RX), including fixed-dose combinations that BNF codes outside the biguanide paragraph. Use is_combination to split plain metformin from combinations.',

    antibacterials AS {{ ref('int_antibacterial_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Antibacterial medication orders with class classification (penicillins, cephalosporins, macrolides, etc.)',

    ace_inhibitors AS {{ ref('int_ace_inhibitor_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'ACE inhibitor orders (BNF 2.5.5.1). Cornerstone of HTN, HF, CKD management.',

    arbs AS {{ ref('int_arb_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'ARB (angiotensin II receptor blocker) orders (BNF 2.5.5.2). Alternative to ACE inhibitors.',

    beta_blockers AS {{ ref('int_beta_blocker_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Beta-blocker orders. Used for AF rate control, HF, post-MI, hypertension.',

    lithium AS {{ ref('int_lithium_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Lithium orders (BNF 4.2.3). Requires therapeutic drug monitoring. Small cohort, high safety relevance.',

    ics AS {{ ref('int_inhaled_corticosteroid_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Inhaled corticosteroid (ICS) orders (BNF 3.2). Cornerstone of asthma/COPD preventer therapy.',

    epilepsy_meds AS {{ ref('int_epilepsy_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Anti-epileptic drug orders. Some also used as mood stabilisers (SMI crossover).',

    nsaids AS {{ ref('int_nsaid_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'NSAID medication orders (BNF 10.1)',

    ppis AS {{ ref('int_ppi_medications_all') }}
        PRIMARY KEY (medication_order_id)
        COMMENT = 'Proton pump inhibitor orders (BNF 1.3.5)'
)

RELATIONSHIPS(
    rx (person_id) REFERENCES demographics,
    rx (practice_code) REFERENCES practice (practice_code),
    statins (medication_order_id) REFERENCES rx,
    antihypertensives (medication_order_id) REFERENCES rx,
    anticoagulants (medication_order_id) REFERENCES rx,
    antiplatelets (medication_order_id) REFERENCES rx,
    antipsychotics (medication_order_id) REFERENCES rx,
    antidepressants (medication_order_id) REFERENCES rx,
    valproate (medication_order_id) REFERENCES rx,
    diabetes_meds (medication_order_id) REFERENCES rx,
    glp1 (medication_order_id) REFERENCES rx,
    sglt2 (medication_order_id) REFERENCES rx,
    dpp4 (medication_order_id) REFERENCES rx,
    metformin (medication_order_id) REFERENCES rx,
    antibacterials (medication_order_id) REFERENCES rx,
    ace_inhibitors (medication_order_id) REFERENCES rx,
    arbs (medication_order_id) REFERENCES rx,
    beta_blockers (medication_order_id) REFERENCES rx,
    lithium (medication_order_id) REFERENCES rx,
    ics (medication_order_id) REFERENCES rx,
    epilepsy_meds (medication_order_id) REFERENCES rx,
    nsaids (medication_order_id) REFERENCES rx,
    ppis (medication_order_id) REFERENCES rx
)

FACTS(
    -- Order details
    rx.estimated_cost AS estimated_cost COMMENT = 'Estimated cost of this medication order (GBP).',
    rx.quantity_value AS quantity_value COMMENT = 'Quantity prescribed',
    rx.duration_days AS duration_days COMMENT = 'Duration of prescription in days',
    rx.age_at_event AS age_at_event COMMENT = 'Patient age at time of order'
)

DIMENSIONS(
    -- Person linkage key (on rx so it can be selected alongside order facts)
    rx.person_id AS person_id COMMENT = 'Pseudonymised person key, shared by all sem_olids_* views. Exposed only for cross-view cohort intersection: join CTEs over two views on person_id, then aggregate. Never return person_id in final results.',
    demographics.sk_patient_id AS sk_patient_id COMMENT = 'Representative pseudonymised patient key for linkage to non-OLIDS views (SUS acute activity, cost index, resource index). Every active person has one; the underlying person-patient mapping can be many-to-many, so joins remain approximate at the margins. Join CTEs on sk_patient_id, then aggregate; never return sk_patient_id in final results.',

    -- Order time
    rx.order_date AS order_date WITH SYNONYMS = ('prescription date', 'date') COMMENT = 'Date the medication was issued. Guaranteed between 1990-01-01 and today; dirty source dates are excluded upstream.',
    rx.fiscal_year_start AS fiscal_year_start COMMENT = 'Integer start year of the UK fiscal year (Apr-Mar), e.g. 2024 = FY2024/25. Compare as a number, not a date. Use for annual cost comparisons.',

    -- BNF classification (use chatbot BNF tool to resolve drug names to codes)
    rx.bnf_chapter AS bnf_chapter WITH SYNONYMS = ('BNF chapter', 'therapeutic area') COMMENT = 'BNF chapter (2-digit). Key chapters: 01=GI, 02=Cardiovascular, 03=Respiratory, 04=CNS, 05=Infections, 06=Endocrine, 07=Obstetrics/Gynae/UTI, 08=Malignancy, 09=Nutrition/Blood, 10=Musculoskeletal, 11=Eye, 12=ENT, 13=Skin, 14=Vaccines',
    rx.bnf_section AS bnf_section COMMENT = 'BNF section (4-digit, e.g. 0212=Lipid-Regulating Drugs)',
    rx.bnf_paragraph AS bnf_paragraph COMMENT = 'BNF paragraph (6-digit)',
    rx.bnf_code AS bnf_code COMMENT = 'Full BNF product code (e.g. 0212000B0AAAAAA). 15-character format: chapter(2) + section(2) + paragraph(2) + chemical(3) + product(4) + formulation(2).',
    rx.bnf_name AS bnf_name COMMENT = 'BNF product name',
    rx.medication_name AS medication_name COMMENT = 'Medication name as recorded',
    rx.mapped_concept_display AS mapped_concept_display COMMENT = 'Mapped medication concept description. Prefer BNF fields or pre-defined medication sets where available.',
    rx.mapped_concept_code AS mapped_concept_code COMMENT = 'Mapped medication concept code. Use for exact concept filtering when no BNF or pre-defined medication classification is available.',
    rx.dose AS dose COMMENT = 'Dose as recorded',
    rx.quantity_unit AS quantity_unit COMMENT = 'Unit for quantity_value. Only aggregate quantity within the same medication product and unit; quantities with different products or units are not comparable.',

    -- Prescription type (from statement)
    rx.issue_type AS issue_type WITH SYNONYMS = ('acute', 'repeat', 'prescription type') COMMENT = 'Prescription type (Acute, Repeat, Repeat Dispensing, Automatic). Acute = one-off; Repeat = ongoing repeat prescription; Repeat Dispensing = pharmacy-managed repeat; Automatic = auto-issued.',
    rx.prescription_is_active AS prescription_is_active COMMENT = 'Whether the parent prescription (statement) is currently active (TRUE/FALSE)',
    rx.issue_method AS issue_method COMMENT = 'Order issue channel: Electronic, Print, Outside Other, Handwritten, Outside Hospital, Outside Out Of Hours, Automatic, or Over The Counter.',

    -- Prescribing practice
    rx.prescribing_practice_code AS practice_code WITH SYNONYMS = ('practice code', 'ODS code', 'GP practice') COMMENT = 'ODS code of the prescribing practice',
    practice.prescribing_practice_name AS practice_name COMMENT = 'Name of the prescribing practice',
    practice.prescribing_pcn_code AS pcn_code COMMENT = 'PCN code of the prescribing practice',
    practice.prescribing_pcn_name AS pcn_name WITH SYNONYMS = ('PCN', 'primary care network') COMMENT = 'PCN name of the prescribing practice',
    practice.prescribing_pcn_name_with_borough AS pcn_name_with_borough COMMENT = 'Prescribing PCN with borough prefix',
    practice.prescribing_borough AS borough_registered WITH SYNONYMS = ('borough') COMMENT = 'Borough of the prescribing practice',
    practice.sub_icb_code AS sub_icb_code COMMENT = 'Sub-ICB / place-based partnership ODS code of the prescribing practice: 93C = NHS North Central London (Camden, Islington, Barnet, Enfield, Haringey); W2U3Z = NHS North West London (Brent, Ealing, Hammersmith and Fulham, Harrow, Hillingdon, Hounslow, Kensington and Chelsea, Westminster). NULL outside the WNL footprint.',
    practice.sub_icb_name AS sub_icb_name COMMENT = 'Sub-ICB display name (NHS North Central London or NHS North West London) of the prescribing practice. NULL outside the WNL footprint.',

    -- Patient demographics core (current snapshot; richer demographics, conditions,
    -- vulnerability and polypharmacy cohorts come from sem_olids_population via person_id)
    demographics.gender AS gender COMMENT = 'Patient gender (Male, Female, Unknown)',
    demographics.age_band_5y AS age_band_5y COMMENT = 'Current 5-year age band (drifts — use age_at_event for historical cohorting)',
    demographics.age_band_10y AS age_band_10y COMMENT = 'Current 10-year age band (drifts)',
    demographics.age_band_nhs AS age_band_nhs COMMENT = 'Current NHS standard age band (drifts)',
    demographics.ethnicity_category AS ethnicity_category COMMENT = 'Ethnicity category (Asian or Asian British, Black or Black British, Mixed, Other, White, Unknown)',
    demographics.is_active AS is_active COMMENT = 'Patient currently registered',

    -- Patient geography and deprivation (residence-based)
    demographics.borough_resident AS borough_resident COMMENT = 'Patient borough of residence',
    demographics.neighbourhood_resident AS neighbourhood_resident COMMENT = 'Patient NCL neighbourhood of residence',
    demographics.imd_decile_25 AS imd_decile_25 COMMENT = 'IMD 2025 decile (1=most deprived, 10=least). NULL if LSOA not mapped.',
    demographics.imd_quintile_25 AS imd_quintile_25 COMMENT = 'IMD 2025 quintile (1 - Most Deprived to 5 - Least Deprived, Unknown)',

    -- Statin classification (only populated for statin orders)
    statins.statin_intensity AS statin_intensity COMMENT = 'Statin intensity (HIGH_INTENSITY, MODERATE_INTENSITY, COMBINATION, OTHER_STATIN). Only for statin orders.',
    statins.is_combination_therapy AS is_combination_therapy COMMENT = 'Statin + ezetimibe combination. Only for statin orders.',

    -- Anticoagulant classification (only populated for anticoagulant orders)
    anticoagulants.anticoagulant_type AS anticoagulant_type COMMENT = 'Anticoagulant type (DOAC, VKA, Other). Only for anticoagulant orders.',
    anticoagulants.is_doac AS is_doac COMMENT = 'Direct oral anticoagulant. Only for anticoagulant orders.',

    -- Valproate classification (only populated for valproate orders — clinical safety)
    valproate.valproate_product_type AS valproate_product_type COMMENT = 'Valproate type (SODIUM_VALPROATE, VALPROIC_ACID, EPILIM, DEPAKOTE). Only for valproate orders.',
    valproate.clinical_indication AS clinical_indication COMMENT = 'Valproate indication (ANTI_EPILEPTIC, MOOD_STABILISER). Only for valproate orders.',
    valproate.dose_category AS dose_category COMMENT = 'Valproate dose category (LOW, MODERATE, HIGH, UNKNOWN). Only for valproate orders.',
    valproate.formulation_type AS formulation_type COMMENT = 'Valproate formulation type. Only for valproate orders.',

    -- Antibacterial classification (only populated for antibacterial orders)
    antibacterials.antibacterial_class AS antibacterial_class COMMENT = 'Antibacterial class, such as penicillin, cephalosporin or macrolide. Only for antibacterial orders.',

    -- GLP-1 RA classification (only populated for GLP-1 orders)
    glp1.glp1_drug AS glp1_drug COMMENT = 'GLP-1 RA active ingredient (SEMAGLUTIDE, DULAGLUTIDE, LIRAGLUTIDE, EXENATIDE, LIXISENATIDE, ALBIGLUTIDE, TIRZEPATIDE, OTHER_GLP1RA). Only for GLP-1 orders.',
    glp1.is_dual_gip_glp1 AS is_dual_gip_glp1 COMMENT = 'Tirzepatide (dual GIP/GLP-1 agonist). Only for GLP-1 orders.',
    glp1.is_diabetes_indication AS is_diabetes_indication COMMENT = 'GLP-1 order under BNF 6.1 (diabetes) vs obesity/other. Only for GLP-1 orders.',

    -- SGLT2 inhibitor classification (only populated for SGLT2 orders)
    sglt2.sglt2_drug AS sglt2_drug COMMENT = 'SGLT2 inhibitor active ingredient (DAPAGLIFLOZIN, EMPAGLIFLOZIN, CANAGLIFLOZIN, ERTUGLIFLOZIN, OTHER_SGLT2I). Only for SGLT2 orders.',

    -- DPP-4 inhibitor classification (only populated for DPP-4 orders)
    dpp4.dpp4_drug AS dpp4_drug COMMENT = 'DPP-4 inhibitor active ingredient (SITAGLIPTIN, LINAGLIPTIN, SAXAGLIPTIN, VILDAGLIPTIN, ALOGLIPTIN, OTHER_DPP4I). Only for DPP-4 orders.',

    -- Metformin classification (only populated for metformin orders)
    metformin.is_combination AS is_combination COMMENT = 'Metformin fixed-dose combination (vs plain metformin). Only for metformin orders.'
)

METRICS(
    -- Volume
    rx.order_count AS COUNT(rx.medication_order_id) COMMENT = 'Total medication orders (issues)',
    rx.patient_count AS COUNT(DISTINCT rx.person_id) COMMENT = 'Distinct patients',

    -- Cost
    rx.total_cost AS SUM(rx.estimated_cost) COMMENT = 'Total estimated prescribing cost (GBP). Orders without a mapped estimated cost are excluded.',
    rx.avg_cost_per_order AS AVG(rx.estimated_cost) COMMENT = 'Average cost per order (GBP)',

    -- Prescription type
    rx.repeat_order_count AS COUNT(CASE WHEN rx.issue_type IN ('Repeat', 'Repeat Dispensing') THEN rx.medication_order_id END) COMMENT = 'Repeat prescription orders (includes Repeat Dispensing)',
    rx.acute_order_count AS COUNT(CASE WHEN rx.issue_type = 'Acute' THEN rx.medication_order_id END) COMMENT = 'Acute (one-off) prescription orders',

    -- Category counts (non-null when order is in that category)
    statins.statin_order_count AS COUNT(statins.medication_order_id) COMMENT = 'Statin orders',
    statins.statin_patient_count AS COUNT(DISTINCT statins.person_id) COMMENT = 'Patients with statin orders',
    antihypertensives.antihypertensive_order_count AS COUNT(antihypertensives.medication_order_id) COMMENT = 'Antihypertensive orders',
    antihypertensives.antihypertensive_patient_count AS COUNT(DISTINCT antihypertensives.person_id) COMMENT = 'Patients with antihypertensive orders',
    anticoagulants.anticoagulant_order_count AS COUNT(anticoagulants.medication_order_id) COMMENT = 'Anticoagulant orders',
    anticoagulants.anticoagulant_patient_count AS COUNT(DISTINCT anticoagulants.person_id) COMMENT = 'Patients with anticoagulant orders',
    antiplatelets.antiplatelet_order_count AS COUNT(antiplatelets.medication_order_id) COMMENT = 'Antiplatelet orders',
    antiplatelets.antiplatelet_patient_count AS COUNT(DISTINCT antiplatelets.person_id) COMMENT = 'Patients with antiplatelet orders',
    antipsychotics.antipsychotic_order_count AS COUNT(antipsychotics.medication_order_id) COMMENT = 'Antipsychotic orders',
    antipsychotics.antipsychotic_patient_count AS COUNT(DISTINCT antipsychotics.person_id) COMMENT = 'Patients with antipsychotic orders',
    antidepressants.antidepressant_order_count AS COUNT(antidepressants.medication_order_id) COMMENT = 'Antidepressant orders',
    antidepressants.antidepressant_patient_count AS COUNT(DISTINCT antidepressants.person_id) COMMENT = 'Patients with antidepressant orders',
    antibacterials.antibacterial_order_count AS COUNT(antibacterials.medication_order_id) COMMENT = 'Antibacterial orders',
    antibacterials.antibacterial_patient_count AS COUNT(DISTINCT antibacterials.person_id) COMMENT = 'Patients with antibacterial orders',
    diabetes_meds.diabetes_med_order_count AS COUNT(diabetes_meds.medication_order_id) COMMENT = 'Diabetes medication orders',
    diabetes_meds.diabetes_med_patient_count AS COUNT(DISTINCT diabetes_meds.person_id) COMMENT = 'Patients with diabetes medication orders',
    glp1.glp1_order_count AS COUNT(glp1.medication_order_id) COMMENT = 'GLP-1 receptor agonist orders',
    glp1.glp1_patient_count AS COUNT(DISTINCT glp1.person_id) COMMENT = 'Patients with GLP-1 receptor agonist orders',
    sglt2.sglt2_order_count AS COUNT(sglt2.medication_order_id) COMMENT = 'SGLT2 inhibitor orders',
    sglt2.sglt2_patient_count AS COUNT(DISTINCT sglt2.person_id) COMMENT = 'Patients with SGLT2 inhibitor orders',
    dpp4.dpp4_order_count AS COUNT(dpp4.medication_order_id) COMMENT = 'DPP-4 inhibitor orders',
    dpp4.dpp4_patient_count AS COUNT(DISTINCT dpp4.person_id) COMMENT = 'Patients with DPP-4 inhibitor orders',
    metformin.metformin_order_count AS COUNT(metformin.medication_order_id) COMMENT = 'Metformin orders (incl. combinations)',
    metformin.metformin_patient_count AS COUNT(DISTINCT metformin.person_id) COMMENT = 'Patients with metformin orders (incl. combinations)',
    ace_inhibitors.ace_inhibitor_order_count AS COUNT(ace_inhibitors.medication_order_id) COMMENT = 'ACE inhibitor orders',
    ace_inhibitors.ace_inhibitor_patient_count AS COUNT(DISTINCT ace_inhibitors.person_id) COMMENT = 'Patients with ACE inhibitor orders',
    arbs.arb_order_count AS COUNT(arbs.medication_order_id) COMMENT = 'ARB orders',
    arbs.arb_patient_count AS COUNT(DISTINCT arbs.person_id) COMMENT = 'Patients with ARB orders',
    beta_blockers.beta_blocker_order_count AS COUNT(beta_blockers.medication_order_id) COMMENT = 'Beta-blocker orders',
    beta_blockers.beta_blocker_patient_count AS COUNT(DISTINCT beta_blockers.person_id) COMMENT = 'Patients with beta-blocker orders',
    lithium.lithium_order_count AS COUNT(lithium.medication_order_id) COMMENT = 'Lithium orders',
    lithium.lithium_patient_count AS COUNT(DISTINCT lithium.person_id) COMMENT = 'Patients with lithium orders',
    ics.ics_order_count AS COUNT(ics.medication_order_id) COMMENT = 'Inhaled corticosteroid orders',
    ics.ics_patient_count AS COUNT(DISTINCT ics.person_id) COMMENT = 'Patients with inhaled corticosteroid orders',
    epilepsy_meds.epilepsy_med_order_count AS COUNT(epilepsy_meds.medication_order_id) COMMENT = 'Anti-epileptic drug orders',
    epilepsy_meds.epilepsy_med_patient_count AS COUNT(DISTINCT epilepsy_meds.person_id) COMMENT = 'Patients with anti-epileptic drug orders',
    nsaids.nsaid_order_count AS COUNT(nsaids.medication_order_id) COMMENT = 'NSAID orders',
    nsaids.nsaid_patient_count AS COUNT(DISTINCT nsaids.person_id) COMMENT = 'Patients with NSAID orders',
    ppis.ppi_order_count AS COUNT(ppis.medication_order_id) COMMENT = 'PPI orders',
    ppis.ppi_patient_count AS COUNT(DISTINCT ppis.person_id) COMMENT = 'Patients with PPI orders',
    valproate.valproate_order_count AS COUNT(valproate.medication_order_id) COMMENT = 'Valproate orders',
    valproate.valproate_patient_count AS COUNT(DISTINCT valproate.person_id) COMMENT = 'Patients with valproate orders'
)

COMMENT = 'OLIDS Prescribing Semantic View - All medication orders with BNF classification, prescription type, prescribing-practice attribution, core patient demographics, and pre-defined drug category flags. Source: OLIDS (One London Integrated Data Set). Grain: one row per medication order. BNF chapter is the primary therapeutic filter — the chatbot has a BNF lookup tool to resolve drug class names. Condition, vulnerability, and polypharmacy cohorts come from sem_olids_population via person_id CTE joins.'
AI_SQL_GENERATION 'LINKAGE: query each view in its own CTE, reduce to one row per person before joining on person_id, then aggregate; keep person_id out of the final output. This is medication-order grain. Example: SELECT bnf_chapter, AGG(order_count), AGG(total_cost) FROM SEM_OLIDS_PRESCRIBING WHERE order_date >= DATEADD(year, -1, CURRENT_DATE) GROUP BY bnf_chapter. Example linkage: reduce active diabetes people in sem_olids_population and SGLT2 orders here before joining. Default treatment exposure is the last 12 months of order_date. For dimension-backed classes, filter the classification dimension in WHERE (antibacterial_class, sglt2_drug, glp1_drug, dpp4_drug, statin_intensity, anticoagulant_type, valproate_product_type or metformin is_combination). For metric-only medication sets, use person-grain HAVING AGG(<class>_order_count) > 0. Never put a metric in WHERE. Every pre-defined medication set has <class>_order_count and <class>_patient_count metrics — use AGG(<class>_patient_count) for "patients with X orders" headcounts. Prefer the pre-defined medication sets over BNF filtering for known drug classes. BNF codes are compact, not dotted: bnf_chapter 2-digit (02 = Cardiovascular), bnf_section 4-digit (0205 = Hypertension and heart failure), bnf_code 15-character product code — use the BNF lookup tool to resolve drug class names, then WHERE bnf_section = result or bnf_code LIKE result || ''%''. order_date is clean (1990-01-01 to today). fiscal_year_start is an integer year (2024 = FY2024/25), not a date. quantity_value is meaningful only within the same medication product and quantity_unit. Practice dimensions are prescribing practice; registered practice is in population. Demographics are current snapshot — use age_at_event for historical age cohorting.'
AI_QUESTION_CATEGORIZATION 'Use this view for: medication orders, prescribing volume and estimated cost by BNF classification, medication, practice or PCN; exposed medication classes; repeat versus acute issues; and prescribing equity by deprivation or ethnicity. It does not contain dispensing, adherence, allergy or monitoring-test records. For current population health (conditions, demographics) without prescribing use sem_olids_population. For clinical biomarkers use sem_olids_observations. Questions needing cohorts from TWO domains (e.g. medication x biomarker control, medication x appointment access, treated vs untreated gaps) are answerable by joining this view to the other sem_olids_* views on person_id in CTEs, with aggregate-only output.'
