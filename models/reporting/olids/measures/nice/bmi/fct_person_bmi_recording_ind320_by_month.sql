{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

-- NICE IND320: https://www.nice.org.uk/indicators/ind320
-- BMI recorded in 12 months for people with CHD, stroke/TIA, diabetes, non-diabetic hyperglycaemia, hypertension, PAD, heart failure, COPD, dyslipidaemia, learning disability, obstructive sleep apnoea or SMI.
{{ nice_ind320('by_month') }}
