-- Preserve clinical definitions; the legacy empty-key "Unknown" entry is a placeholder.
with checked as (
    select 'mhsds_diagnosis' as record_group, c.clinical_code,
        d.code as dictionary_code, d.description as dictionary_description
    from {{ ref('fct_mhsds_clinical_record') }} as c
    inner join {{ ref('stg_dictionary_dbo_diagnosis') }} as d
        on {{ clean_icd10_code('upper(trim(c.clinical_code))') }} = upper(d.code)
    where c.coding_scheme_kind = 'diagnosis' and c.coding_scheme_code = '02'
        and nullif(trim(c.clinical_code), '') is not null
        and nullif(trim(d.code), '') is not null
        and d.description is not null and c.clinical_description is null

    union all

    select 'csds_finding', c.clinical_code, d.code, d.description
    from {{ ref('fct_csds_clinical_record') }} as c
    inner join {{ ref('stg_dictionary_dbo_diagnosis') }} as d
        on {{ clean_icd10_code('upper(trim(c.clinical_code))') }} = upper(d.code)
    where c.clinical_code_system = 'ICD-10'
        and nullif(trim(c.clinical_code), '') is not null
        and nullif(trim(d.code), '') is not null
        and d.description is not null and c.clinical_description is null
)
select * from checked
