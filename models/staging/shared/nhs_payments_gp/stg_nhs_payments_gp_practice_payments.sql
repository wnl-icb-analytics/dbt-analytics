-- NHS Payments to General Practice: one row per practice per financial year.
-- financial_year_start (1 April) makes latest-year and trend sorting robust.
select
    practice_code,
    practice_name,
    financial_year,
    date_from_parts(left(financial_year, 4)::int, 4, 1) as financial_year_start,
    commissioner_code,
    commissioner_name,
    average_registered_patients as registered_patients,
    average_weighted_patients   as weighted_patients,
    total_nhs_payments
from {{ ref('raw_nhs_payments_gp_practice_payments') }}
where practice_code is not null
  and average_weighted_patients is not null
  -- zero/null registered list (closing/merging practices) would yield a
  -- ratio of 0 downstream; dropping the row lets is_latest fall back to the
  -- practice's last real year, or mean imputation if none exists
  and average_registered_patients > 0
