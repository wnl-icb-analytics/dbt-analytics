with checked as (
    select
        'diagnosis' as record_group
        , count_if(d.description is distinct from c.clinical_description) as mismatched_labels
    from {{ ref('fct_mhsds_clinical_record') }} as c
    inner join {{ ref('stg_dictionary_dbo_diagnosis') }} as d
        on {{ clean_icd10_code('upper(trim(c.clinical_code))') }} = upper(d.code)
    where c.coding_scheme_kind = 'diagnosis'
        and c.coding_scheme_code = '02'

    union all

    select
        'activity_finding'
        , count_if(d.description is distinct from a.finding_description)
    from {{ ref('fct_mhsds_care_activity') }} as a
    inner join {{ ref('stg_dictionary_dbo_diagnosis') }} as d
        on {{ clean_icd10_code('upper(trim(a.finding_code))') }} = upper(d.code)
    where a.finding_scheme_code = '01'
)

select * from checked where mismatched_labels > 0
