-- A stale or missing date spine must not silently leave reporting a month behind.
select max(analysis_month) as latest_analysis_month
from {{ ref('person_month_analysis_base') }}
having coalesce(max(analysis_month), '1900-01-01'::date)
    <> last_day(dateadd('month', -1, current_date))
