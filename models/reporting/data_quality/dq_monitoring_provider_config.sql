{{ config(materialized='table') }}

-- =====================================================
-- SUS / SLAM Core Provider Monitoring
--
-- Core hospital providers already monitored by the
-- existing SUS Unified monitoring framework.
-- =====================================================

with core_hospital_providers as (

    select column1 as provider_code
    from values

        ('RRV'),
        ('RKE'),
        ('RAL'),
        ('RP4'),
        ('RP6'),
        ('RAN'),
        ('RYJ'),
        ('RQM'),
        ('R1K'),
        ('RAS')

),

provider_lookup as (

    select
        organisation_code,
        organisation_name

    from {{ ref('organisation_nhs_provider') }}

),

provider_details as (

    select
        p.provider_code,
        coalesce(
            l.organisation_name,
            p.provider_code
        ) as provider_name

    from core_hospital_providers p

    left join provider_lookup l
        on p.provider_code = l.organisation_code

)

-- =====================================================
-- SUS
-- =====================================================

select
    'SUS'                   as dataset_group,
    provider_code,
    provider_name,
    'CORE'                  as provider_scope,
    true                    as monitor_individually,
    true                    as alert_enabled

from provider_details


union all


-- =====================================================
-- SLAM
-- =====================================================

select
    'SLAM'                  as dataset_group,
    provider_code,
    provider_name,
    'CORE'                  as provider_scope,
    true                    as monitor_individually,
    true                    as alert_enabled

from provider_details
