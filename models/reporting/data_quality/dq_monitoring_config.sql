{{ config(materialized='table') }}

-- =====================================================
-- CSDS Referral
-- Monthly snapshot refreshed from CSDS submissions.
-- =====================================================

select

    -- Identity
    'CSDS_REFERRAL'            as table_id,
    'REPORTING'                as database_name,
    'COMMUNITY'                as schema_name,
    'FCT_CSDS_REFERRAL'        as table_name,

    -- Monitoring
    true                       as is_active,
    'REFRESH_DATE'             as refresh_column,
    'MONTHLY'                  as frequency_type,

    -- Freshness thresholds
    40                         as warn_stale_days,
    60                         as critical_stale_days,

    -- Volume thresholds
    10                         as warn_row_change_pct,
    25                         as critical_row_change_pct,

    -- Classification
    'CSDS'                     as business_area,
    'MONTHLY_SNAPSHOT'         as object_type,

    -- Metadata
    'Monthly CSDS table' as notes

union all

-- =====================================================
-- CSDS Care Contacts
-- Monthly snapshot refreshed from CSDS submissions.
-- =====================================================

select

    -- Identity
    'CSDS_CARE_CONTACT'        as table_id,
    'REPORTING'                as database_name,
    'COMMUNITY'                as schema_name,
    'FCT_CSDS_CARE_CONTACT'    as table_name,

    -- Monitoring
    true                       as is_active,
    'REFRESH_DATE'             as refresh_column,
    'MONTHLY'                  as frequency_type,

    -- Freshness thresholds
    40                         as warn_stale_days,
    60                         as critical_stale_days,

    -- Volume thresholds
    10                         as warn_row_change_pct,
    25                         as critical_row_change_pct,

    -- Classification
    'CSDS'                     as business_area,
    'MONTHLY_SNAPSHOT'         as object_type,

    -- Metadata
    'Monthly CSDS table' as notes

union all

-- =====================================================
-- CSDS Care Contact Activity
-- Monthly snapshot refreshed from CSDS submissions.
-- =====================================================

select

    -- Identity
    'CSDS_CARE_CONTACT_ACTIVITY' as table_id,
    'REPORTING'                  as database_name,
    'COMMUNITY'                  as schema_name,
    'FCT_CSDS_CARE_ACTIVITY'     as table_name,

    -- Monitoring
    true                       as is_active,
    'REFRESH_DATE'             as refresh_column,
    'MONTHLY'                  as frequency_type,

    -- Freshness thresholds
    40                         as warn_stale_days,
    60                         as critical_stale_days,

    -- Volume thresholds
    10                         as warn_row_change_pct,
    25                         as critical_row_change_pct,

    -- Classification
    'CSDS'                     as business_area,
    'MONTHLY_SNAPSHOT'         as object_type,

    -- Metadata
    'Monthly CSDS table' as notes

union all

-- =====================================================
-- SUS Unified Inpatient (APC)
-- Dynamic table refreshed daily (~05:00).
-- =====================================================

select

    -- Identity
    'SUS_IP'                   as table_id,
    'STAGING'                  as database_name,
    'SUS'                      as schema_name,
    'STG_SUS_APC_SPELL'        as table_name,

    -- Monitoring
    true                       as is_active,
    'REFRESH_DATE'             as refresh_column,
    'DAILY'                    as frequency_type,

    -- Freshness thresholds
    2                          as warn_stale_days,
    5                          as critical_stale_days,

    -- Volume thresholds
    10                         as warn_row_change_pct,
    25                         as critical_row_change_pct,

    -- Classification
    'SUS'                      as business_area,
    'DAILY_OPERATIONAL'        as object_type,

    -- Metadata
    'Daily dynamic inpatient table' as notes
    
union all

-- =====================================================
-- SLAM LSACM - Aggregated.
-- =====================================================

select

    'SLAM_LSACM'                       as table_id,
    'STAGING'                          as database_name,
    'SLAM'                             as schema_name,
    'STG_LSACM_LATEST'                 as table_name,

    true                               as is_active,
    'DV_DATEANDTIMEDATASETCREATED'     as refresh_column,
    'MONTHLY'                          as frequency_type,

    40                                 as warn_stale_days,
    60                                 as critical_stale_days,

    10                                 as warn_row_change_pct,
    25                                 as critical_row_change_pct,

    'SLAM'                             as business_area,
    'MONTHLY_AGGREGATE'                as object_type,

    'Monthly SLAM aggregate contract monitoring data'
                                       as notes

union all

-- =====================================================
-- SLAM LSPLCM - Patient Level.
-- =====================================================

select

    'SLAM_LSPLCM'                       as table_id,
    'STAGING'                           as database_name,
    'SLAM'                              as schema_name,
    'STG_LSPLCM_LATEST'                 as table_name,

    true                               as is_active,
    'DV_DATEANDTIMEDATASETCREATED'     as refresh_column,
    'MONTHLY'                          as frequency_type,

    40                                 as warn_stale_days,
    60                                 as critical_stale_days,

    10                                 as warn_row_change_pct,
    25                                 as critical_row_change_pct,

    'SLAM'                             as business_area,
    'MONTHLY_PLD'                      as object_type,

    'Monthly SLAM PLD contract monitoring data'
                                       as notes
                                       