-- Dynamically generated metadata query from source_mappings.yml
-- Query all databases and schemas defined in your source configuration
--
-- Usage:
--   1. Copy this entire query
--   2. Paste into Snowflake UI
--   3. Execute query
--   4. Export results as CSV to table_metadata.csv

WITH schema_metadata AS (
    -- pds: Personal Demographics Service data
  SELECT 
    'DATA_LAKE' as database_name,
    'PDS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'PDS'
  
  UNION ALL
  
    -- csds: Community services dataset
  SELECT 
    'DATA_LAKE' as database_name,
    'CSDS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'CSDS'
  
  UNION ALL
  
    -- iapt: NHS Talking Therapies (IAPT) dataset
  SELECT
    'DATA_LAKE' as database_name,
    'IAPT' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'IAPT'

  UNION ALL

    -- wl: Waiting lists and patient pathway data
  SELECT 
    'DATA_LAKE' as database_name,
    'WL' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'WL'
  
  UNION ALL
  
    -- sus_op: SUS outpatient appointments and activity
  SELECT 
    'DATA_LAKE' as database_name,
    'SUS_UNIFIED_OP' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SUS_UNIFIED_OP'
  
  UNION ALL
  
    -- sus_apc: SUS admitted patient care episodes and procedures
  SELECT 
    'DATA_LAKE' as database_name,
    'SUS_UNIFIED_APC' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SUS_UNIFIED_APC'
  
  UNION ALL
  
    -- sus_ecds: SUS emergency care attendances and activity
  SELECT 
    'DATA_LAKE' as database_name,
    'SUS_UNIFIED_ECDS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SUS_UNIFIED_ECDS'
  
  UNION ALL
  
    -- eRS_primary_care: Primary care referrals data
  SELECT 
    'DATA_LAKE' as database_name,
    'ERS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'ERS'
  
  UNION ALL
  
    -- epd_primary_care: Primary care medications and prescribing data
  SELECT 
    'DATA_LAKE' as database_name,
    'EPD_PRIMARY_CARE' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'EPD_PRIMARY_CARE'
  
  UNION ALL
  
    -- mhsds: Mental Health Services Data Set (MHSDS)
  SELECT 
    'DATA_LAKE' as database_name,
    'MHSDS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'MHSDS'
  
  UNION ALL
  
    -- registries_deaths: Register of deaths
  SELECT 
    'DATA_LAKE' as database_name,
    'DEATHS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'DEATHS'
  
  UNION ALL
  
    -- registries_births: Register of births
  SELECT 
    'DATA_LAKE' as database_name,
    'BIRTHS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'BIRTHS'
  
  UNION ALL
  
    -- adult_social_care_cld: National submission from local authorities for adult social care (client level dataset)
  SELECT 
    'DATA_LAKE' as database_name,
    'SOCIAL_CARE' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SOCIAL_CARE'
  
  UNION ALL
  
    -- reference_trud_terminology: SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.
  SELECT 
    'DATA_LAKE' as database_name,
    'TERMINOLOGY' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'TERMINOLOGY'
  
  UNION ALL
  
    -- dictionary_dbo: Reference data including PDS and lookup tables
  SELECT 
    'Dictionary' as database_name,
    'dbo' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'dbo'
  
  UNION ALL
  
    -- dictionary_ecds: Reference data for ECDS
  SELECT 
    'Dictionary' as database_name,
    'ECDS_ETOS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'ECDS_ETOS'
  
  UNION ALL
  
    -- dictionary_op: Reference data for outpatient procedures and treatments
  SELECT 
    'Dictionary' as database_name,
    'OP' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'OP'
  
  UNION ALL
  
    -- dictionary_ip: Reference data for inpatient procedures and treatments
  SELECT 
    'Dictionary' as database_name,
    'IP' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'IP'
  
  UNION ALL
  
    -- dictionary_eRS: Primary care referrals lookups
  SELECT 
    'Dictionary' as database_name,
    'E-Referral' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'E-Referral'
  
  UNION ALL
  
    -- olids: OLIDS stable layer - cleaned and filtered patient records
  SELECT 
    'DATA_LAKE' as database_name,
    'OLIDS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'OLIDS'
  
  UNION ALL
  
    -- dictionary_snomed: Reference data for snomed
  SELECT 
    'Dictionary' as database_name,
    'Snomed' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'Snomed'
  
  UNION ALL
  
    -- nhsd_snomed: NHS Digital SNOMED CT reporting model with concept status and history
  SELECT 
    'Dictionary' as database_name,
    'NHSD_SnomedReportingModel' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "Dictionary".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'NHSD_SnomedReportingModel'
  
  UNION ALL
  
    -- reference_terminology: Reference terminology data including SNOMED, BNF, and other code sets
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'TERMINOLOGY' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'TERMINOLOGY'
  
  UNION ALL
  
    -- ptl: ptl data
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'PTL' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'PTL'
  
  UNION ALL
  
    -- reference_data_management: Data management reference datasets
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'DATA_MANAGEMENT' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'DATA_MANAGEMENT'
  
  UNION ALL
  
    -- reference_cancer_cwt_alliance: Cancer waiting times alliance data
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'CANCER__CWT_ALLIANCE' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'CANCER__CWT_ALLIANCE'
  
  UNION ALL
  
    -- reference_cancer_emis: Cancer EMIS data
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'CANCER__EMIS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'CANCER__EMIS'
  
  UNION ALL
  
    -- reference_cancer_screening: Cancer screening data
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'CANCER__SCREENING' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'CANCER__SCREENING'
  
  UNION ALL
  
    -- reference_fingertips: Fingertips indicator data
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'FINGERTIPS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'FINGERTIPS'
  
  UNION ALL
  
    -- phenolab: Phenolab supporting data
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'PHENOLAB_DEV' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'PHENOLAB_DEV'
  
  UNION ALL
  
    -- reference_primary_care: Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships
  SELECT 
    'REFERENCE' as database_name,
    'PRIMARY_CARE' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "REFERENCE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'PRIMARY_CARE'
  
  UNION ALL
  
    -- reference_geo: Curated geography lookups - LSOA to neighbourhood, ward, MSOA and ICB hierarchies
  SELECT 
    'REFERENCE' as database_name,
    'GEO' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "REFERENCE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'GEO'
  
  UNION ALL
  
    -- local_provider_flows: Providers submissions from PID environment via MESH
  SELECT 
    'DATA_LAKE' as database_name,
    'LOCAL_PROVIDER_FLOWS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'LOCAL_PROVIDER_FLOWS'
  
  UNION ALL
  
    -- cltcs_emis_extract: EMIS extract for c-ltcs pipeline from PID environment via MESH and other local lookups
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'CLTCS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'CLTCS'
  
  UNION ALL
  
    -- fact_patient: Patient fact tables
  SELECT 
    'DATA_LAKE' as database_name,
    'FACT_PATIENT' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'FACT_PATIENT'
  
  UNION ALL
  
    -- fact_practice: Practice fact tables
  SELECT 
    'DATA_LAKE' as database_name,
    'FACT_PRACTICE' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'FACT_PRACTICE'
  
  UNION ALL
  
    -- sdl: ServicesDataLocal canonical-named feeds (mental health, community, 111, etc.)
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'SDL' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SDL'
  
  UNION ALL
  
    -- sdl_wnl: ServicesDataLocal canonical-named feeds, WNL footprint (SLAM contract monitoring)
  SELECT 
    'DATA_LAKE' as database_name,
    'SDL' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'SDL'
  
  UNION ALL
  
    -- reference_analyst_managed: Analyst-managed reference datasets and business rules
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'ANALYST_MANAGED' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'ANALYST_MANAGED'
  
  UNION ALL
  
    -- pod_group_manager: POD Group Manager app - governed POD group mappings
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'POD_GROUP_MANAGER' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'POD_GROUP_MANAGER'
  
  UNION ALL
  
    -- myria: Project Myria (BEVC) canonical inputs - enrolled and propensity-matched patients
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'MYRIA' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'MYRIA'
  
  UNION ALL
  
    -- nhs_payments_gp: NHS Payments to General Practice - annual practice-level registered and Carr-Hill weighted list sizes
  SELECT 
    'DATA_LAKE__NCL' as database_name,
    'NHS_PAYMENTS_GP' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE__NCL".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'NHS_PAYMENTS_GP'
  
  UNION ALL
  
    -- reference_lookup_ncl: Analyst-managed reference datasets and business rules in the MODELLING environment
  SELECT 
    'MODELLING' as database_name,
    'LOOKUP_NCL' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "MODELLING".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'LOOKUP_NCL'
  
  UNION ALL
  
    -- c_ltcs: C-LTCS tables
  SELECT 
    'PUBLISHED_REPORTING__DIRECT_CARE' as database_name,
    'C_LTCS' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "PUBLISHED_REPORTING__DIRECT_CARE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'C_LTCS'
  
  UNION ALL
  
    -- pmct: Central Performance Analytics Team (PMCT)
  SELECT 
    'DATA_LAKE' as database_name,
    'PMCT' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'PMCT'
  
  UNION ALL
  
    -- tat: Imaging diagnostic turnaround-time provider submissions (raw landing)
  SELECT 
    'DATA_LAKE' as database_name,
    'TAT' as schema_name,
    table_name,
    column_name,
    data_type,
    numeric_precision,
    numeric_scale,
    ordinal_position
  FROM "DATA_LAKE".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = 'TAT'
)

SELECT 
  database_name as "DATABASE_NAME",
  schema_name as "SCHEMA_NAME", 
  table_name as "TABLE_NAME",
  column_name as "COLUMN_NAME",
  CASE
    WHEN UPPER(data_type) IN ('NUMBER', 'DECIMAL', 'NUMERIC') AND numeric_precision IS NOT NULL
      THEN UPPER(data_type) || '(' || numeric_precision || ',' || COALESCE(numeric_scale, 0) || ')'
    ELSE data_type
  END as "DATA_TYPE",
  ordinal_position as "ORDINAL_POSITION"
FROM schema_metadata
ORDER BY database_name, schema_name, table_name, ordinal_position;
