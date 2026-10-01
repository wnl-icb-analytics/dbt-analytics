-- LTC LCS: Model of Care activity - Check & Test appointment completed
-- Flags persons with a Chronic disease initial assessment event in the last 12 months.
-- Used by HRCS1 / HRS1 / MRS1 / LRS1 EMIS searches.
--
-- Matches the EMIS source code 552991000006110 "Chronic disease initial assessment" only.
-- That code maps to SNOMED 170549007 "Chronic disease monitoring", and the value set's
-- include-children expansion covers every chronic disease review (diabetes, CKD,
-- anticoagulation, ...), which over-counts stage 1.

with events as (
    select
        person_id,
        clinical_effective_date
    from ({{ get_ltc_lcs_observations_latest("check_test_appointment_completed_vs1", match_paths='source') }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
      and clinical_effective_date <= current_date()
)

select
    person_id,
    max(clinical_effective_date) as latest_completed_date,
    true as check_test_completed
from events
group by person_id
