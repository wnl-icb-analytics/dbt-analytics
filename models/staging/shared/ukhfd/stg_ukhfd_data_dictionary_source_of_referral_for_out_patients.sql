with stg_base as (
{{ select_ukhfd_data_dictionary_code_set('raw_ukhfd_data_dictionary_source_of_referral_for_out_patients') }}
),

--CTE to clean the description column
cleaned_description as (
    select 
        code,
        description,
        
        case 
            when description like '%Consultant initiated%'
            then ' (Consultant Initiated)'
            else null
        end as shorter_suffix,
    
        case
            when description like '%ollowing an%'
            then position('ollowing an ', description) + len('ollowing an ')
            when description like '%ollowing a %'
            then position('ollowing a ', description) + len('ollowing a ')
            when description like '%nitiated: %'
            then position('nitiated: ', description) + len('nitiated: ')
            else null
        end as start_of_cat,
    
        case 
            when start_of_cat is null then description
            else substr(description, start_of_cat)
        end as description_trimmed,

        regexp_replace(description_trimmed, ' \\(.*?\\)', '') as description_main,

        case
            when shorter_suffix is null
            then description_main
            else concat(description_main, shorter_suffix)
        end as description_clean,

        replace(
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace(initcap(description_clean),
                                    ' A ', ' a '),
                                ' An ', ' an '),
                            ' From ', ' from '),
                        ' With ', ' with '),
                    ' Than ', ' than '),
                ' In ', ' in '),
            ' Of ', ' of '
        ) as description_sentence,
        
        regexp_replace(
            regexp_substr(description_trimmed, '\\(.*?\\)'),
            '[()]',
            ''
        ) as notes
        
    from stg_base
    where is_latest_definition = true
    order by code
)

select 
    sb.*, 
    cd.description_sentence as description_clean,
    cd.notes as description_clean_notes

from stg_base sb
left join cleaned_description cd
on cd.code = sb.code