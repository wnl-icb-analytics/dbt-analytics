--CTE to get 62 day records
with base_62_day_data as (
    select * from {{ref('int_cwt_pathway__62d_allocation_method')}}
),

--CTE for 'Solo' blame
blame_solo as (
    select
        record_id,
        performance__date_full,

        --62 Day Methodology Fields
        'Solo' as performance__allocation_method,
        null as performance__accountable_org_type,
        null as performance__scenario_id,

        --Use investigating trust as the responsible trust
        d62__treating_trust_code as performance__trust_code,
        null as performance__site_code,

        --Performance
        '62 Day' as performance__metric,
        performance__pathway_length_days,
        performance__numerator_base as performance__numerator,
        performance__denominator_base as performance__denominator,
        performance__denominator - performance__numerator as performance__breach
        
    from base_62_day_data

    where d62__allocation_method = 'Solo'
),

--CTEs for '5050' blame
blame_5050_diag as (
    select
        record_id,
        performance__date_full,

        --62 Day Methodology Fields
        '50-50' as performance__allocation_method,
        'Investigating' as performance__accountable_org_type,
        null as performance__scenario_id,
        
        --Use investigating trust as the responsible trust
        d62__investigating_trust_code as performance__trust_code,
        null as performance__site_code,

        --Performance
        '62 Day' as performance__metric,
        performance__pathway_length_days,
        0.5 * performance__numerator_base as performance__numerator,
        0.5 * performance__denominator_base as performance__denominator,
        performance__denominator - performance__numerator as performance__breach
        
    from base_62_day_data

    where d62__allocation_method = '50-50'
),

blame_5050_treat as (
    select
        record_id,
        performance__date_full,

        --62 Day Methodology Fields
        '50-50' as performance__allocation_method,
        'Treating' as performance__accountable_org_type,
        null as performance__scenario_id,
    
        --Use treating trust as the responsible trust
        d62__treating_trust_code as performance__trust_code,
        null as performance__diag_code,

        --Performance
        '62 Day' as performance__metric,
        performance__pathway_length_days,
        0.5 * performance__numerator_base as performance__numerator,
        0.5 * performance__denominator_base as performance__denominator,
        performance__denominator - performance__numerator as performance__breach
        
    from base_62_day_data

    where d62__allocation_method = '50-50'
),

--CTE for the Investigating blame
blame_6s_diag as (
    select
        record_id,
        performance__date_full,
        
        --62 Day Methodology Fields
        '6 Scenarios' as performance__allocation_method,
        'Investigating' as performance__accountable_org_type,
        scenario_id as performance__scenario_id,

        --Accountable organisation Fields
        d62__investigating_trust_code as performance__trust_code,
        null as performance__site_code,

        --Performance Fields
        '62 Day' as performance__metric,
        performance__pathway_length_days,
        performance__numerator_base * performance_investigating_accountability_factor as performance__numerator,
        performance__denominator_base * performance_investigating_accountability_factor as performance__denominator,
        performance__denominator - performance__numerator as performance__breach
        
    from {{ref('int_cwt_pathway__62d_scenario_id')}}
),

blame_6s_treat as (
    select
        record_id,
        performance__date_full,
        
        --62 Day Methodology Fields
        '6 Scenarios' as performance__allocation_method,
        'Treating' as performance__accountable_org_type,
        scenario_id as performance__scenario_id,
        
        --Accountable organisation Fields
        d62__treating_trust_code as performance__trust_code,
        null as performance__site_code,
        
        --Performance Fields
        '62 Day' as performance__metric,
        performance__pathway_length_days,
        performance__numerator_base * performance_treating_accountability_factor as performance__numerator,
        performance__denominator_base * performance_treating_accountability_factor as performance__denominator,
        performance__denominator - performance__numerator as performance__breach
        
    from {{ref('int_cwt_pathway__62d_scenario_id')}}
)

select * from blame_solo

union all

select * from blame_5050_diag

union all

select * from blame_5050_treat

union all

select * from blame_6s_diag

union all

select * from blame_6s_treat