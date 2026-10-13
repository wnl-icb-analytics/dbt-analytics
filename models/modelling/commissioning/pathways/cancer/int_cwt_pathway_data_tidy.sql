--CTE to create trust site map
--Note historic mentions of NMUH ('RAP' trust code) is remapped to RFL ('RAL' trust code and 'RAL27' site code)
-- 333761 is the NMUH SK Organisation ID for the historic trust
with org_par as (
    select
        child.sk_organisation_id = 333761 or child.sk_organisation_id_parent_org = 333761 as nmuh_flag,
        child.organisation_code as site_code_original,
        case 
            when nmuh_flag
            then 'RAL27'
            else child.organisation_code
        end as site_code, 
        case
            when nmuh_flag
            then 'RAL'
            when child.sk_organisation_type_id = 41
            then child.organisation_code
            else par.organisation_code
        end as trust_code,
        child.organisation_primary_role as child_role
        
    from {{ ref('stg_dictionary_dbo_organisation') }} child

    left join {{ ref('stg_dictionary_dbo_organisation') }} par 
    on child.sk_organisation_id_parent_org = par.sk_organisation_id
)

select
    --Entry identifiers
    cwt.recordid as record_id,
    cwt.nhs_number_pseudo::number as sk_patient_id,

    --Date Fields
    date(cwt.consultantupgradedate) as date__consultant_upgrade_date,
    date(cwt.dectorefdate) as date__decision_to_refer_date,
    date(cwt.crtpdate) as date__cancer_referral_to_treatment_period_start_date,
    date(cwt.datefirstseen) as date__date_first_seen,
    date(cwt.cancerfasterdiagnosispathwayenddate) as date__fds_pathway_end_date,
    date(cwt.transfertotreatmentdate) as date__transfer_to_treatment_date,
    date(cwt.cancertreatmentperiodstartdate) as date__cancer_treatment_period_date,
    date(cwt.treatmentstartdatecancer) as date__treatment_start_date,

    --Organisation Fields
    site_map__consultant_upgrade.trust_code as org__consultant_upgrade_trust_code,
    site_map__consultant_upgrade.site_code as org__consultant_upgrade_site_code,

    site_map__first_seen.trust_code as org__first_seen_trust_code,
    site_map__first_seen.site_code as org__first_seen_site_code,

    site_map__faster_diagnosis_pathway_end.trust_code as org__faster_diagnosis_pathway_end_trust_code,
    site_map__faster_diagnosis_pathway_end.site_code as org__faster_diagnosis_pathway_end_site_code,

    coalesce(
        site_map__patient_pathway_identifier.trust_code,
        cwt.orgppi
    ) as org__patient_pathway_identifier_issuer_trust_code,
    
    case 
        when cwt.accountableinvestigatingprovider = 'RAP' then 'RAL' 
        else cwt.accountableinvestigatingprovider
    end as org__accountable_investigating_trust_code_preset,
    
    coalesce(
        org__accountable_investigating_trust_code_preset,
        org__consultant_upgrade_trust_code,
        org__first_seen_trust_code
    ) as org__accountable_investigating_trust_code,
    
    site_map__accountable_treating.trust_code as org__accountable_treating_trust_code,
    site_map__accountable_treating.site_code as org__accountable_treating_site_code,

    --Referral Fields
    to_varchar(cwt.reftype) as referral__cancer_referral_type_code,
    to_varchar(cwt.sourceofreferralforoutpatient) as referral__source_of_referral_for_outpatient_code,
    to_varchar(cwt.prioritytypecode) as referral__priority_type_code,

    --Diagnosis Fields
    to_varchar(cwt.fdpdiagsite) as diagnosis__fds_suspected_cancer_type_code,
    to_varchar(cwt.fdpendreason) as diagnosis__fds_end_reason_code,
    to_varchar(cwt.fdpexclusionreason) as diagnosis__fds_end_reason_exclusion_code,
    to_varchar(cwt.fdpoutcomemethod) as diagnosis__fds_outcome_method_of_communication_code,
    to_varchar(cwt.fdpoutcomeproftype) as diagnosis__fds_outcome_profession_type_code,

    --Treatment
    cwt.primarydiagnosisicd as treatment__primary_icd10_code,
    to_varchar(cwt.cancertreatmentmodality) as treatment__modality_code,
    to_varchar(cwt.cancertreatmenteventtype) as treatment__event_type_code,
    to_varchar(cwt.cancercaresettingtreatment) as treatment__care_setting_code,

    --Performance
    cwt.waitingtimeadjustmentfirstseen as performance__wta_first_seen_date_adjustment,
    to_varchar(cwt.wtafirstseenreason) as performance__wta_first_seen_date_reason_code,
    cwt.waitingtimeadjustmenttreatment as performance__wta_treatment_start_date_adjustment,
    to_varchar(cwt.waitingtimeadjustmentreasontreatment) as performance__wta_treatment_start_date_reason_code,

    --Delays
    to_varchar(cwt.delayreffsreason) as cancer_care_spell_delay_reason__first_seen_code,
    to_varchar(cwt.delayfdpreason) as cancer_care_spell_delay_reason__fdp_code,
    to_varchar(cwt.delaydtttreatreason) as cancer_care_spell_delay_reason__dtt_code,
    to_varchar(cwt.delayreftreatreason) as cancer_care_spell_delay_reason__treatment_code,
    to_varchar(cwt.delayconstreatreason) as cancer_care_spell_delay_reason__consultant_upgrade_code,

    --Status
    cwt.patientstatus as status__patient_status_code,

    --Patient Details at time of event
    cwt.pds_gpcode as at_event__registered__practice_code,
    cwt.lsoa_of_residence as at_event__residence__lsoa_code,

    --Metadata
    cwt.pds_gpcode,
    cwt.la_of_residence,
    cwt.organisationidentifierreceiving1,
    cwt.organisationidentifierreceiving2,
    cwt.organisationidentifierreceiving3,
    cwt.organisationidentifierreceiving4,
    cwt.organisationidentifierreceiving5,
    cwt.organisationidentifierreceiving6,
    cwt.organisationidentifierreceiving7,
    cwt.organisationidentifierreceiving8,
    cwt.organisationidentifierreceiving9,
    cwt.organisationidentifierreceiving10,
    cwt.organisationidentifierreferring1,
    cwt.organisationidentifierreferring2,
    cwt.organisationidentifierreferring3,
    cwt.organisationidentifierreferring4,
    cwt.organisationidentifierreferring5,
    cwt.organisationidentifierreferring6,
    cwt.organisationidentifierreferring7,
    cwt.organisationidentifierreferring8,
    cwt.organisationidentifierreferring9,
    cwt.organisationidentifierreferring10
    

from {{ ref('stg_cwt_pathway_cwt001data') }} as cwt

--Joins to map organisation sites into trusts
left join org_par site_map__consultant_upgrade
on cwt.orgconsupgrade = site_map__consultant_upgrade.site_code_original

left join org_par site_map__first_seen
on cwt.orgfirstseen = site_map__first_seen.site_code_original

left join org_par site_map__faster_diagnosis_pathway_end
on cwt.orgfdpend = site_map__faster_diagnosis_pathway_end.site_code_original

left join org_par site_map__accountable_treating
on cwt.orgtreatstart = site_map__accountable_treating.site_code_original

--SPECIAL CASE to account for this field inconsistently using trust and site codes
left join org_par site_map__patient_pathway_identifier
on cwt.orgppi = site_map__patient_pathway_identifier.site_code_original
and (
    site_map__patient_pathway_identifier.child_role in ('RO176', 'RO198')
    or site_map__patient_pathway_identifier.nmuh_flag
)

--Clause to remove inactive records
where exists (
    select null
    from {{ ref('stg_cwt_pathway_activesystemid') }} asi
    where asi.dmic_system_id = cwt.dmic_system_id
)