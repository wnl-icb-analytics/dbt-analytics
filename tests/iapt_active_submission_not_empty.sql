-- An empty delivery must not withdraw all retained history.
select 1 as missing_accepted_submissions
where not exists (
    select 1 from {{ ref('stg_iapt_activesubmission') }}
)
