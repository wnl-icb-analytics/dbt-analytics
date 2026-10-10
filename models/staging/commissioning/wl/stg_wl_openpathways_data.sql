{{
    config(materialized = 'table')
}}

{# Previous Sunday, or the date itself when it is a Sunday. #}
{% set census_week_ending_date %}
case
            when dayofweekiso(week_ending_date) = 7 then week_ending_date
            else dateadd('day', -dayofweek(week_ending_date), week_ending_date)
        end
{%- endset %}

{% set valid_group_keys %}
where week_ending_date is not null
        and pseudo_nhs_number is not null
        and referral_identifier is not null
        and patient_pathway_identifier is not null
        and activity_treatment_function_code is not null
        and organisation_identifier_code_of_provider is not null
        and organisation_site_identifier_of_treatment is not null
        and referral_to_treatment_period_start_date is not null
        and date_and_time_data_set_created is not null
{%- endset %}

-- Sunday census date is already a partition column, so the week-ending
-- predicates can sit before the pick without changing which duplicate wins.
-- Closed pathways must be dropped after the pick: end date is not a
-- partition column, so filtering first would change the max pair.
-- Null group keys are excluded, as the original inner join on them did.
with census as (
    select
        src.*,
        {{ census_week_ending_date }} as census_week_ending_date
    from {{ ref('raw_wl_wl_openpathways_data') }} as src
    {{ valid_group_keys }}
),
-- Few groups hold more than one row, so the max pair is resolved for those
-- groups only rather than windowing every wide row.
duplicate_groups as (
    select
        pseudo_nhs_number,
        referral_identifier,
        patient_pathway_identifier,
        activity_treatment_function_code,
        organisation_identifier_code_of_provider,
        organisation_site_identifier_of_treatment,
        referral_to_treatment_period_start_date,
        census_week_ending_date,
        date_and_time_data_set_created,
        max(der_submission_id) as max_submission_id,
        max(der_row_id) as max_row_id
    from (
        -- Key columns only, read from source so the wide census rows are not buffered twice.
        select
            pseudo_nhs_number,
            referral_identifier,
            patient_pathway_identifier,
            activity_treatment_function_code,
            organisation_identifier_code_of_provider,
            organisation_site_identifier_of_treatment,
            referral_to_treatment_period_start_date,
            date_and_time_data_set_created,
            der_submission_id,
            der_row_id,
            {{ census_week_ending_date }} as census_week_ending_date
        from {{ ref('raw_wl_wl_openpathways_data') }}
        {{ valid_group_keys }}
    ) as keys
    where census_week_ending_date <= current_date
    group by
        pseudo_nhs_number,
        referral_identifier,
        patient_pathway_identifier,
        activity_treatment_function_code,
        organisation_identifier_code_of_provider,
        organisation_site_identifier_of_treatment,
        referral_to_treatment_period_start_date,
        census_week_ending_date,
        date_and_time_data_set_created
    having count(*) > 1
),
picked as (
    select c.*
    from census as c
    left join duplicate_groups as d
        on c.pseudo_nhs_number = d.pseudo_nhs_number
        and c.referral_identifier = d.referral_identifier
        and c.patient_pathway_identifier = d.patient_pathway_identifier
        and c.activity_treatment_function_code = d.activity_treatment_function_code
        and c.organisation_identifier_code_of_provider = d.organisation_identifier_code_of_provider
        and c.organisation_site_identifier_of_treatment = d.organisation_site_identifier_of_treatment
        and c.referral_to_treatment_period_start_date = d.referral_to_treatment_period_start_date
        and c.census_week_ending_date = d.census_week_ending_date
        and c.date_and_time_data_set_created = d.date_and_time_data_set_created
    where c.census_week_ending_date <= current_date
        and (
            -- A single-row group keeps its row unless either id is null,
            -- as null never equals the group max.
            (d.pseudo_nhs_number is null
                and c.der_submission_id is not null
                and c.der_row_id is not null)
            or (c.der_submission_id = d.max_submission_id
                and c.der_row_id = d.max_row_id)
        )
)
select
    census_week_ending_date as week_ending_date,
    {{ consistent_sk_patient_id_format('pseudo_nhs_number') }} as sk_patient_id,
    local_patient_identifier,
    person_stated_gender_code,
    ethnic_category,
    der_lsoa2021 as lsoa_2021,
    der_age_week_ending_date as age_at_week_ending_date,
    der_age_at_referral_to_treatment_period_start_date as age_at_referral_to_treatment_period_start_date,
    der_age_band_week_ending_date as age_band_week_ending_date,
    der_age_band_at_referral_to_treatment_period_start_date as age_band_at_referral_to_treatment_period_start_date,
    organisation_identifier_code_of_commissioner as commissioner_code,
    der_practice_code as practice_code,
    der_ccg_of_practice as ccg_of_practice,
    der_ccg_of_residence as ccg_of_residence,
    waiting_list_type,
    organisation_identifier_code_of_provider as provider_code,
    organisation_site_identifier_of_treatment as provider_site_code,
    organisation_identifier_referring_organisation as referring_organisation_code,
    referral_request_received_date,
    original_referral_request_received_date,
    referral_to_treatment_period_start_date,
    current_pathway_period_start_date,
    referral_identifier,
    patient_pathway_identifier,
    organisation_code_patient_pathway_identifier_issuer,
    source_of_referral,
    main_specialty_code,
    activity_treatment_function_code as treatment_function_code,
    consultant_code,
    outpatient_future_appointment_date,
    due_date,
    outpatient_appointment_date,
    date_last_attended,
    last_dna_date,
    cancellation_date,
    outcome_of_attendance_code,
    tci_date,
    referral_to_treatment_period_status,
    decision_to_admit_date,
    proposed_procedure_opcs_code,
    admission_method_code_hospital_provider_spell as admission_method_code,
    priority_type_code,
    procedure_priority_code,
    diagnostic_priority_code,
    date_of_last_priority_review,
    last_pas_validation_date,
    inclusion_on_cancer_ptl,
    dm_icb_commissioner,
    dm_sub_icb_commissioner,
    intended_management_code,
    asa_physical_status_classification_system_code,
    transfer_status,
    mutual_aid_accepting_provider,
    first_activity_date,
    first_activity_type,
    decision_to_treat_date,
    tci_date_provided,
    preliminary_screening_and_risk_assessment_date,
    action_following_preliminary_screening_and_risk_assessment,
    date_and_time_data_set_created,
    der_submission_id as submission_id,
    der_row_id as row_id,
    1 as open_pathways
from picked
where referral_to_treatment_period_end_date is null
