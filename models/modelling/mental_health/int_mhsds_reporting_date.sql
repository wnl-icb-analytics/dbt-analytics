select max(reporting_period_end_date) as as_of_date
from {{ ref('stg_mhsds_activesubmission') }}
