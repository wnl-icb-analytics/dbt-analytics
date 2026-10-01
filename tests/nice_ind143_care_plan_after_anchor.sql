-- NICE IND143 counts a mental health care plan only when it is dated on or
-- after the plan anchor: the latest diagnosis where a remission code exists,
-- otherwise the first diagnosis (QOF MH002 reading). A numerator row whose
-- plan precedes its anchor breaks that rule.
SELECT person_id, latest_record_date, plan_anchor_date
FROM {{ ref('fct_person_smi_care_plan_ind143') }}
WHERE is_in_numerator
    AND (latest_record_date < plan_anchor_date OR plan_anchor_date IS NULL)
