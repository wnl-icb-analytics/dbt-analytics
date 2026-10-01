-- Actual registered patient counts per GP practice by sex and 5-year age band,
-- from the NHS England "Patients Registered at a GP Practice" monthly
-- publication (via UKHFD). Granular rows only: ALL sex/age rollups are
-- excluded, so totals are safe to derive by summing.
{{ config(materialized = 'table') }}

select
    org_code as practice_code,
    snapshot_date,
    sex,
    age_band,
    number_of_patients as registered_patients,
    source_file_version,
    snapshot_date = max(snapshot_date) over (partition by org_code) as is_latest
from {{ ref('stg_ukhfd_registered_patients_by_gp_practice') }}
where org_type = 'GP'
  and sex in ('MALE', 'FEMALE')
  and age_band != 'ALL'
qualify row_number() over (
    partition by org_code, snapshot_date, sex, age_band
    order by source_file_version desc
) = 1
