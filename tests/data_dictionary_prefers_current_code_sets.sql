-- Code lists combining a current and a retired UKHFD code set must keep every
-- code, including retired-only codes, and use the current definition wherever
-- the current set holds the code.
{% set code_lists = [
    ('nhs_dd_admission_source', 'Admission_Source'),
    ('nhs_dd_planned_discharge_destination', 'Planned_Destination_Of_Discharge'),
    ('nhs_dd_discharge_destination', 'Destination_Of_Discharge'),
    ('csds_activity_type', 'Community_Care_Activity_Type'),
    ('csds_referral_closure_reason', 'Referral_Closure_Reason'),
] %}

{% for model_name, preferred_code_set_name in code_lists %}
select
    '{{ model_name }}' as model_name
    , history.code
from {{ ref(model_name ~ '_history') }} as history
left join {{ ref(model_name) }} as selected
    on history.code = selected.code
where history.is_latest_definition
    and (
        selected.code is null
        or (
            history.source_code_set_name = '{{ preferred_code_set_name }}'
            and selected.source_code_set_name is distinct from history.source_code_set_name
        )
    )
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
