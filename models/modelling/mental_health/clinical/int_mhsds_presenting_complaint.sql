with keyed as (
    select
        a.*
        , {{ dbt_utils.generate_surrogate_key(['a.org_id_prov','a.uniq_serv_req_id','a.person_id',
            'a.find_scheme_in_use','a.pres_comp','a.pres_comp_date',
            "iff(a.org_id_prov is null or a.uniq_serv_req_id is null or a.person_id is null or a.find_scheme_in_use is null or a.pres_comp is null or a.pres_comp_date is null, a.mhs609_uniq_id, null)"]) }} as source_record_id
    from {{ ref('stg_mhsds_presenting_complaint') }} as a
)
select
    a.*
    , min(reporting_period_end_date) over (partition by source_record_id) as first_reported_period_end_date
    , max(reporting_period_end_date) over (partition by source_record_id) as last_reported_period_end_date
    , count(*) over (partition by source_record_id) as accepted_source_record_count
    , count(distinct reporting_period_end_date) over (partition by source_record_id) as reported_period_count
    , scheme.description as finding_scheme_description
    , case a.find_scheme_in_use
        when '01' then icd.description when '04' then snomed.preferred_term
        when '02' then legacy.term when '03' then legacy.term
    end as complaint_description
from keyed as a
left join {{ ref('mhsds_care_activity_code_lookup') }} as scheme
    on a.find_scheme_in_use = scheme.code and scheme.code_set_name = 'finding_scheme'
left join {{ ref('icd10_code') }} as icd
    on replace({{ clean_icd10_code('upper(trim(a.pres_comp))') }}, '.', '') = icd.code and a.find_scheme_in_use = '01'
left join {{ ref('snomed_concept') }} as snomed on trim(a.pres_comp) = snomed.snomed_code and a.find_scheme_in_use = '04'
left join {{ ref('read_code') }} as legacy
    on trim(a.pres_comp) = legacy.code
    and legacy.coding_system = case a.find_scheme_in_use when '02' then 'read_v2' when '03' then 'ctv3' end
qualify row_number() over (
    partition by source_record_id order by reporting_period_end_date desc,
        effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs609_uniq_id desc
) = 1
