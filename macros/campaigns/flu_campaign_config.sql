/*
Flu Campaign Configuration - Single Source of Truth

This macro provides all campaign-specific dates and parameters in one place.
Instead of scattered hardcoded dates, everything is defined here clearly.

CAMPAIGNS:
The models build every campaign in flu_reported_campaign_ids(); this macro returns the
parameters for one of them.

Available campaigns:
- 'Flu 2024-25' - 2024-25 Flu Vaccination Campaign
- 'Flu 2025-26' - 2025-26 Flu Vaccination Campaign
- 'Flu 2026-27' - 2026-27 Flu Vaccination Campaign (default)

Usage:
- Specific campaign: {{ flu_campaign_config('Flu 2026-27') }}
- Current season: {{ flu_current_config() }} / {{ flu_previous_config() }}
- All reported campaigns: {{ flu_reported_campaigns() }}

SOURCE:
UKHSA Seasonal Influenza Vaccine Uptake Reporting Specification, PRIMIS v16.0,
23 July 2026 (titled 2526 but published for the 2026-27 collection year).

DATE REFERENCES:
The spec distinguishes three dates and this config carries all three.
- campaign_reference_date is REF_DAT (31 March), used for the 65 and over threshold.
- run_date is RUN_DAT, the extraction date, used for the search population age floor.
- audit_end_date is AUDITEND_DAT, the submission cut-off that bounds clinical evidence
  and vaccinations.
Both run_date and audit_end_date roll forward with CURRENT_DATE while a campaign is
running and stop at campaign_end_date, so closed seasons do not keep absorbing newly
recorded events. Note the deviation: the spec sets AUDITEND_DAT to the five ImmForm
submission month ends, whereas this rolls daily so the dashboard reflects the data as
it lands. Figures taken mid-month therefore sit between two submission points and are
not an ImmForm submission.

IMMUNOSUPPRESSION ADMIN CODES:
Spec v16.0 widened IMMADM_DAT from a fixed six-month floor to any code in the three
years before AUDITEND_DAT. immuno_admin_lookback_date carries that per campaign, so
closed seasons keep the six-month rule they were reported under. Because AUDITEND_DAT
rolls forward during a season, the 2026-27 admin-code window floor moves with it: a
person whose only IMMADM_COD is close to three years old can leave the cohort as the
season progresses. That is the rule the spec states, not a data problem.

CHILD AGE GROUPS:
- Preschool: ages 2-3 at CHILD_DAT (31 August in the campaign year)
- School age: ages 4-15 at CHILD_DAT
- Age-agnostic model names allow for consistent year-to-year comparisons
*/

{% macro flu_campaign_config(campaign_id='Flu 2026-27') %}
    {%- if campaign_id == 'Flu 2024-25' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2024-25 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates
            '2024-09-01'::DATE AS campaign_start_date,
            '2025-03-31'::DATE AS campaign_reference_date,
            '2024-08-31'::DATE AS child_reference_date,
            '2025-02-28'::DATE AS campaign_end_date,

            -- Medication lookback dates
            '2023-09-01'::DATE AS asthma_medication_lookback_date,
            '2024-03-01'::DATE AS immuno_medication_lookback_date,
            '2024-03-01'::DATE AS immuno_admin_lookback_date,

            -- Child age group birth date ranges (campaign-specific)
            '2020-09-01'::DATE AS child_preschool_birth_start,
            '2022-08-31'::DATE AS child_preschool_birth_end,
            '2008-09-01'::DATE AS child_school_age_birth_start,
            '2020-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates
            '2024-08-31'::DATE AS flu_vaccination_after_date,
            '2024-08-31'::DATE AS laiv_vaccination_after_date,

            -- Long-stay residential care was a flu indicator up to spec v15.6
            TRUE AS eligible_long_term_residential_care,

            -- RUN_DAT: the extraction date, capped at the campaign end
            LEAST(CURRENT_DATE, '2025-02-28'::DATE) AS run_date,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            -- UKHSA flu SCT codeclusters version this season is reported under (last release
            -- on or before the final AUDITEND_DAT); see stg_reference_ukhsa_codecluster_versions
            '5.6' AS terminology_version,

            LEAST(CURRENT_DATE, '2025-02-28'::DATE) AS audit_end_date
    {%- elif campaign_id == 'Flu 2025-26' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2025-26 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates (shifted +1 year)
            '2025-09-01'::DATE AS campaign_start_date,
            '2026-03-31'::DATE AS campaign_reference_date,
            '2025-08-31'::DATE AS child_reference_date,
            '2026-02-28'::DATE AS campaign_end_date,

            -- Medication lookback dates (shifted +1 year)
            '2024-09-01'::DATE AS asthma_medication_lookback_date,
            '2025-03-01'::DATE AS immuno_medication_lookback_date,
            '2025-03-01'::DATE AS immuno_admin_lookback_date,

            -- Child age group birth date ranges (campaign-specific)
            '2021-09-01'::DATE AS child_preschool_birth_start,
            '2023-08-31'::DATE AS child_preschool_birth_end,
            '2009-09-01'::DATE AS child_school_age_birth_start,
            '2021-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates (shifted +1 year)
            '2025-08-31'::DATE AS flu_vaccination_after_date,
            '2025-08-31'::DATE AS laiv_vaccination_after_date,

            -- Legacy local extension. Spec v15.7 removed the long-stay residential care
            -- indicator, so 2025-26 was already outside the spec when it was reported with
            -- this cohort. It stays on to preserve figures already published locally, and
            -- retiring it needs business agreement rather than a code change.
            TRUE AS eligible_long_term_residential_care,

            -- RUN_DAT: the extraction date, capped at the campaign end
            LEAST(CURRENT_DATE, '2026-02-28'::DATE) AS run_date,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            -- UKHSA flu SCT codeclusters version this season is reported under (last release
            -- on or before the final AUDITEND_DAT); see stg_reference_ukhsa_codecluster_versions
            '6.2' AS terminology_version,

            LEAST(CURRENT_DATE, '2026-02-28'::DATE) AS audit_end_date
    {%- elif campaign_id == 'Flu 2026-27' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            '2026-27 Flu Vaccination Campaign' AS campaign_name,

            -- Core campaign dates (spec 2.1: START_DAT 01/09/26, REF_DAT 31/03/27,
            -- CHILD_DAT 31/08/26, final AUDITEND_DAT 28/02/27)
            '2026-09-01'::DATE AS campaign_start_date,
            '2027-03-31'::DATE AS campaign_reference_date,
            '2026-08-31'::DATE AS child_reference_date,
            '2027-02-28'::DATE AS campaign_end_date,

            -- Medication lookback dates (spec 2.3)
            '2025-09-01'::DATE AS asthma_medication_lookback_date,      -- ASTMED_DAT and ASTRX_DAT from 01/09/25
            '2026-03-01'::DATE AS immuno_medication_lookback_date,      -- IMMRX_DAT and DXT_CHEMO_DAT from 01/03/26
            DATEADD('year', -3, LEAST(CURRENT_DATE, '2027-02-28'::DATE)) AS immuno_admin_lookback_date,
                                                                        -- IMMADM_DAT: 3 years before AUDITEND_DAT

            -- Child age group birth date ranges: ages 2-3 and 4-15 at CHILD_DAT (spec 4.1)
            '2022-09-01'::DATE AS child_preschool_birth_start,
            '2024-08-31'::DATE AS child_preschool_birth_end,
            '2010-09-01'::DATE AS child_school_age_birth_start,
            '2022-08-31'::DATE AS child_school_age_birth_end,

            -- Vaccination tracking dates (spec: vaccination fields are after 31/08/26)
            '2026-08-31'::DATE AS flu_vaccination_after_date,
            '2026-08-31'::DATE AS laiv_vaccination_after_date,

            -- Local extension. Spec v15.7 removed the long-stay residential care indicator
            -- from the flu return, but residents remain eligible (UKHSA autumn 2026
            -- eligibility guide) and their vaccinations are coordinated through a
            -- provider, so the cohort is kept and drives eligibility. Not part of the
            -- ImmForm return.
            TRUE AS eligible_long_term_residential_care,

            -- RUN_DAT: the extraction date, capped at the campaign end
            LEAST(CURRENT_DATE, '2027-02-28'::DATE) AS run_date,

            -- Audit end date (AUDITEND_DAT): rolls forward in season, then pins to campaign end
            -- UKHSA flu SCT codeclusters version this season is reported under (last release
            -- on or before the final AUDITEND_DAT); see stg_reference_ukhsa_codecluster_versions
            '7.0' AS terminology_version,

            LEAST(CURRENT_DATE, '2027-02-28'::DATE) AS audit_end_date
    {%- else -%}
        -- Default to current campaign if unknown campaign_id
        {{ flu_campaign_config('Flu 2026-27') }}
    {%- endif -%}
{% endmacro %}

/*
Helper macro to get a specific campaign date
Usage: {{ flu_get_campaign_date('campaign_reference_date') }}
*/
{% macro flu_get_campaign_date(date_name, campaign_id=none) %}
    {%- set campaign_id = campaign_id or var('flu_current_campaign', 'Flu 2026-27') -%}
    (SELECT {{ date_name }} FROM ({{ flu_campaign_config(campaign_id) }}))
{% endmacro %}
