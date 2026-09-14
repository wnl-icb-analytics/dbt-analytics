-- Aggregate-only quality checks. Dates are retained in the source fact.
select count(*) as row_count,count(distinct source_record_id) as unique_referrals,
 count_if(try_to_number(mode) is not null) as numeric_modes,
 count_if(mode is not null and not regexp_like(mode,'[[:alpha:] _/-]+')) as modes_outside_plain_words,
 count_if(clinical_effective_date < '1900-01-01') as dates_before_1900,
 count_if(clinical_effective_date::date > current_date()) as future_clinical_dates,
 count_if(trim(receiving_organisation_name)='') as blank_receiving_names,
 count_if(is_outgoing_referral is null) as unknown_direction
from {{ ref('fct_gp_referral_request') }};

