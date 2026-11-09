with cleanse_description as (
    select 
        main_code_text,
        replace(main_description, 'Suspected ') as trim_prefix,
        case 
            when trim_prefix like '%cancers' then replace(trim_prefix, ' cancers')
            when trim_prefix like '%cancer' then replace(trim_prefix, ' cancer')
            when trim_prefix like '%tumours' then replace(trim_prefix, ' tumours')
            else trim_prefix
        end as clean_string,
        initcap(clean_string) as short_name_initcap,
        replace(
            replace(
                short_name_initcap,
                'And', 'and'
            ),
            'Or', 'or'
        ) as main_description_shortened
    from {{ ref('raw_ukhfd_data_dictionary_two_week_wait_cancer_or_symptomatic_breast_referral_type') }}
    where is_latest = 1
)

select
    attr_name,
    valid_from,
    valid_to,
    raw.main_code_text,
    sub_code1_text,
    sub_code2_text,
    sub_code3_text,
    major_category,
    category,
    main_description,
    main_description_60_chars,
    main_description_shortened,
    sub1_description,
    sub2_description,
    sub3_description,
    notes,
    in_source_table,
    unique_column,
    import_date,
    created_date,
    is_latest,
    effective_from,
    effective_to
    
from {{ ref('raw_ukhfd_data_dictionary_two_week_wait_cancer_or_symptomatic_breast_referral_type') }} raw

left join cleanse_description cd
on raw.main_code_text = cd.main_code_text

where raw.is_latest = 1