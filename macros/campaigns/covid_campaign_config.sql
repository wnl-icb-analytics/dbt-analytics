/*
COVID Campaign Configuration - Single Source of Truth

This macro provides all campaign-specific dates and parameters in one place.
Instead of scattered hardcoded dates, everything is defined here clearly.

CAMPAIGNS:
The models build every campaign in covid_reported_campaign_ids(); this macro returns the
parameters for one of them.

Available campaigns:
- 'COVID Autumn 2024' - Autumn 2024 COVID Vaccination Campaign
- 'COVID Spring 2025' - Spring 2025 COVID Vaccination Campaign
- 'COVID Autumn 2025' - Autumn 2025 COVID Vaccination Campaign
- 'COVID Spring 2026' - Spring 2026 COVID Vaccination Campaign
- 'COVID Autumn 2026' - Autumn 2026 COVID Vaccination Campaign
- 'COVID Spring 2027' - Spring 2027 COVID Vaccination Campaign

Usage:
- Specific campaign: {{ covid_campaign_config('COVID Autumn 2026') }}
- Current season: {{ covid_autumn_config() }} / {{ covid_spring_config() }}
- All reported campaigns: {{ covid_reported_campaigns() }}

AUDIT END DATE:
audit_end_date is the spec RUN_DAT for that period: 31 March for an autumn campaign and
30 June for a spring one. It caps how late an observation can be and still count toward
the campaign, so an autumn cohort does not absorb evidence recorded during the following
spring. It is a per-campaign literal rather than a shared var, because one var with a
different default in each branch moves every season together when supplied.

CAMPAIGN ELIGIBILITY DIFFERENCES:
- 2024/25: Broader eligibility (age 65+ autumn, clinical risk groups)
- 2025/26: Restricted eligibility (age 75+, immunosuppressed, care home 65+ only)
- 2026/27: Same restricted cohorts as 2025/26 (UKHSA COVID spec v4.0, section 5.1.1)

SOURCE:
Business Rules for UKHSA SARS-CoV2 (COVID-19) Vaccine Uptake Reporting 2026/27,
PRIMIS v4.0, 7 August 2026. Section references in the campaign blocks below point
at that document.

IMMUNOSUPPRESSION LOOKBACKS:
The spec defines two immunosuppression groups and this config carries both.
IMMUNO_GROUP, for uptake monitoring, anchors its medication and admin lookbacks on
START_DAT: immuno_medication_lookback_date and immuno_admin_lookback_date.
RECALL_IMMUNO_GROUP, which the campaign offer selects (5.1.1 Group M), anchors them on
RUN_DAT: recall_immuno_medication_lookback_date and recall_immuno_admin_lookback_date.
int_covid_immunosuppression implements the recall group, because its consumer is the
eligible cohort. RUN_DAT is a fixed audit date per period, not the build date, so this is
just as stable across rebuilds as the START_DAT anchor.

COMPLEX ASTHMA STEROID WINDOWS:
Three overlapping 2-year windows to capture repeated steroid use across campaign periods.
From 2026/27 the windows are the absolute collection-year windows the spec states, so
the autumn and spring periods share one set.
*/

{% macro covid_campaign_config(campaign_id='COVID Autumn 2026') %}
    {%- if campaign_id == 'COVID Autumn 2024' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            'Autumn 2024 COVID Vaccination Campaign' AS campaign_name,
            'covid_2024_25' AS campaign_year,
            'autumn' AS campaign_period,
            
            -- Core campaign dates
            '2024-09-01'::DATE AS campaign_start_date,
            '2025-03-31'::DATE AS campaign_end_date,
            '2025-03-31'::DATE AS campaign_reference_date,
            
            -- Medication lookback dates (from START_DAT)
            '2024-03-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2022-09-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start  
            '2022-09-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start
            '2021-09-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start
            
            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2022-09-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before autumn start
            '2024-08-31'::DATE AS asthma_steroid_window_1_end,          -- Up to autumn 2024
            '2023-04-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from spring 2023
            '2025-03-31'::DATE AS asthma_steroid_window_2_end,          -- Up to spring 2025  
            '2023-07-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2025-06-30'::DATE AS asthma_steroid_window_3_end,          -- Up to spring 2025 end
            
            -- Vaccination tracking dates
            '2024-09-01'::DATE AS vaccination_tracking_start,
            '2025-03-31'::DATE AS vaccination_tracking_end,
            '2024-08-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2025-06-30'::DATE AS decline_tracking_end,                 -- Through spring period
            
            -- Pregnancy tracking (campaign-specific) 
            '2024-01-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign
            '2024-09-01'::DATE AS pregnancy_current_start,              -- Campaign period start
            '2025-06-30'::DATE AS pregnancy_current_end,                -- Through spring period
            '2024-01-14'::DATE AS gestational_diabetes_start,          -- Gestational diabetes tracking
            
            
            -- Immunosuppression age cap (NULL = no upper age limit)
            NULL AS immuno_max_age_years,                               -- No age cap in 2024/25

            -- Individual condition eligibility flags (2024/25 campaigns). Known gap: the
            -- Autumn 2024 offer (spec appendix Group I) covered clinical risk groups from
            -- 6 months, but the clinical models apply the 5-year floor of the ImmForm
            -- uptake indicators, so at-risk children under 5 are absent from this season.
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,
            TRUE AS eligible_care_home,
            TRUE AS eligible_asthma,
            TRUE AS eligible_chronic_heart_disease,
            TRUE AS eligible_chronic_kidney_disease,
            TRUE AS eligible_diabetes,
            TRUE AS eligible_chronic_liver_disease,
            TRUE AS eligible_chronic_neurological_disease,
            TRUE AS eligible_chronic_respiratory_disease,
            TRUE AS eligible_morbid_obesity,
            TRUE AS eligible_asplenia,
            TRUE AS eligible_learning_disability,
            TRUE AS eligible_severe_mental_illness,
            TRUE AS eligible_pregnancy,
            TRUE AS eligible_gestational_diabetes,
            TRUE AS eligible_homeless,

            -- Minimum age for the age-based cohort
            65 AS age_based_min_age,                                    -- Autumn 2024 offer was 65+

            -- Minimum age for the care home resident cohort
            18 AS care_home_min_age,                                    -- Autumn 2024 offer covered adult residents

            -- RUN_DAT for this period (spec 2.1). Caps how late an observation can be
            -- and still count toward this campaign.
            -- RECALL_IMMUNO_GROUP lookbacks, measured from RUN_DAT (spec table 10).
            -- Group M and its predecessors select the recall group, not IMMUNO_GROUP.
            '2024-09-30'::DATE AS recall_immuno_medication_lookback_date,   -- RUN_DAT - 6 months
            '2022-03-31'::DATE AS recall_immuno_admin_lookback_date,        -- RUN_DAT - 3 years

            -- UKHSA COVID SCT codeclusters version this season is reported under (last release
            -- on or before RUN_DAT); see stg_reference_ukhsa_codecluster_versions
            '3.4' AS terminology_version,

            '2025-03-31'::DATE AS audit_end_date

    {%- elif campaign_id == 'COVID Spring 2025' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            'Spring 2025 COVID Vaccination Campaign' AS campaign_name,
            'covid_2024_25' AS campaign_year,
            'spring' AS campaign_period,

            -- Core campaign dates
            '2025-04-01'::DATE AS campaign_start_date,
            '2025-06-30'::DATE AS campaign_end_date,
            '2025-06-30'::DATE AS campaign_reference_date,

            -- Medication lookback dates (from START_DAT)
            '2024-10-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2024-04-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start
            '2023-04-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start
            '2022-04-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start

            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2023-04-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before spring start
            '2025-03-31'::DATE AS asthma_steroid_window_1_end,          -- Up to spring 2025
            '2023-10-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from autumn 2023
            '2025-09-30'::DATE AS asthma_steroid_window_2_end,          -- Up to autumn 2025
            '2024-01-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2025-12-31'::DATE AS asthma_steroid_window_3_end,          -- Extended window

            -- Vaccination tracking dates
            '2025-04-01'::DATE AS vaccination_tracking_start,
            '2025-06-30'::DATE AS vaccination_tracking_end,
            '2025-03-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2025-06-30'::DATE AS decline_tracking_end,                 -- Through spring period

            -- Pregnancy tracking (campaign-specific)
            '2024-08-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign
            '2025-04-01'::DATE AS pregnancy_current_start,              -- Campaign period start
            '2025-06-30'::DATE AS pregnancy_current_end,                -- Campaign period end
            '2025-01-14'::DATE AS gestational_diabetes_start,          -- Gestational diabetes tracking

            -- Immunosuppression age cap (NULL = no upper age limit)
            75 AS immuno_max_age_years,                                 -- under 75; 75+ covered by the age group

            -- Individual condition eligibility flags. The Spring 2025 offer (spec appendix
            -- Group J) was 75 and over, care home residents aged 65 and over, and
            -- immunosuppressed people aged 6 months to 74, so the wider clinical risk
            -- groups were not offered a vaccine.
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,
            TRUE AS eligible_care_home,
            FALSE AS eligible_asthma,
            FALSE AS eligible_chronic_heart_disease,
            FALSE AS eligible_chronic_kidney_disease,
            FALSE AS eligible_diabetes,
            FALSE AS eligible_chronic_liver_disease,
            FALSE AS eligible_chronic_neurological_disease,
            FALSE AS eligible_chronic_respiratory_disease,
            FALSE AS eligible_morbid_obesity,
            FALSE AS eligible_asplenia,
            FALSE AS eligible_learning_disability,
            FALSE AS eligible_severe_mental_illness,
            FALSE AS eligible_pregnancy,
            FALSE AS eligible_gestational_diabetes,
            FALSE AS eligible_homeless,

            -- Minimum age for the age-based cohort
            75 AS age_based_min_age,

            -- Minimum age for the care home resident cohort
            65 AS care_home_min_age,

            -- RUN_DAT for this period (spec 2.1). Caps how late an observation can be
            -- and still count toward this campaign.
            -- RECALL_IMMUNO_GROUP lookbacks, measured from RUN_DAT (spec table 10).
            -- Group M and its predecessors select the recall group, not IMMUNO_GROUP.
            '2024-12-30'::DATE AS recall_immuno_medication_lookback_date,   -- RUN_DAT - 6 months
            '2022-06-30'::DATE AS recall_immuno_admin_lookback_date,        -- RUN_DAT - 3 years

            -- UKHSA COVID SCT codeclusters version this season is reported under (last release
            -- on or before RUN_DAT); see stg_reference_ukhsa_codecluster_versions
            '3.4' AS terminology_version,

            '2025-06-30'::DATE AS audit_end_date

    {%- elif campaign_id == 'COVID Spring 2026' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            'Spring 2026 COVID Vaccination Campaign' AS campaign_name,
            'covid_2025_26' AS campaign_year,
            'spring' AS campaign_period,

            -- Core campaign dates
            '2026-04-01'::DATE AS campaign_start_date,
            '2026-06-30'::DATE AS campaign_end_date,
            '2026-06-30'::DATE AS campaign_reference_date,

            -- Medication lookback dates (from START_DAT)
            '2025-10-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2025-04-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start
            '2024-04-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start
            '2023-04-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start

            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2024-04-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before spring start
            '2026-03-31'::DATE AS asthma_steroid_window_1_end,          -- Up to spring 2026
            '2024-10-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from autumn 2024
            '2026-09-30'::DATE AS asthma_steroid_window_2_end,          -- Up to autumn 2026
            '2025-01-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2026-12-31'::DATE AS asthma_steroid_window_3_end,          -- Extended window

            -- Vaccination tracking dates
            '2026-04-01'::DATE AS vaccination_tracking_start,
            '2026-06-30'::DATE AS vaccination_tracking_end,
            '2026-03-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2026-06-30'::DATE AS decline_tracking_end,                 -- Through spring period

            -- Pregnancy tracking (campaign-specific)
            '2025-08-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign
            '2026-04-01'::DATE AS pregnancy_current_start,              -- Campaign period start
            '2026-06-30'::DATE AS pregnancy_current_end,                -- Campaign period end
            '2026-01-14'::DATE AS gestational_diabetes_start,           -- Gestational diabetes tracking


            -- Immunosuppression age cap (NULL = no upper age limit)
            75 AS immuno_max_age_years,                                 -- <75 in 2025/26, 75+ covered by age group

            -- Individual condition eligibility flags (Spring 2026 restricted)
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,
            TRUE AS eligible_care_home,
            FALSE AS eligible_asthma,                    -- Not eligible in Spring 2026
            FALSE AS eligible_chronic_heart_disease,     -- Not eligible in Spring 2026
            FALSE AS eligible_chronic_kidney_disease,    -- Not eligible in Spring 2026
            FALSE AS eligible_diabetes,                  -- Not eligible in Spring 2026
            FALSE AS eligible_chronic_liver_disease,     -- Not eligible in Spring 2026
            FALSE AS eligible_chronic_neurological_disease, -- Not eligible in Spring 2026
            FALSE AS eligible_chronic_respiratory_disease, -- Not eligible in Spring 2026
            FALSE AS eligible_morbid_obesity,            -- Not eligible in Spring 2026
            FALSE AS eligible_asplenia,                  -- Not eligible in Spring 2026
            FALSE AS eligible_learning_disability,       -- Not eligible in Spring 2026
            FALSE AS eligible_severe_mental_illness,     -- Not eligible in Spring 2026
            FALSE AS eligible_pregnancy,                 -- Not eligible in Spring 2026
            FALSE AS eligible_gestational_diabetes,      -- Not eligible in Spring 2026
            FALSE AS eligible_homeless,                  -- Not eligible in Spring 2026

            -- Minimum age for the age-based cohort
            75 AS age_based_min_age,

            -- Minimum age for the care home resident cohort
            65 AS care_home_min_age,

            -- RUN_DAT for this period (spec 2.1). Caps how late an observation can be
            -- and still count toward this campaign.
            -- RECALL_IMMUNO_GROUP lookbacks, measured from RUN_DAT (spec table 10).
            -- Group M and its predecessors select the recall group, not IMMUNO_GROUP.
            '2025-12-30'::DATE AS recall_immuno_medication_lookback_date,   -- RUN_DAT - 6 months
            '2023-06-30'::DATE AS recall_immuno_admin_lookback_date,        -- RUN_DAT - 3 years

            -- UKHSA COVID SCT codeclusters version this season is reported under (last release
            -- on or before RUN_DAT); see stg_reference_ukhsa_codecluster_versions
            '3.7.2' AS terminology_version,

            '2026-06-30'::DATE AS audit_end_date

    {%- elif campaign_id == 'COVID Autumn 2025' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            'Autumn 2025 COVID Vaccination Campaign' AS campaign_name,
            'covid_2025_26' AS campaign_year,
            'autumn' AS campaign_period,
            
            -- Core campaign dates
            '2025-09-01'::DATE AS campaign_start_date,
            '2026-03-31'::DATE AS campaign_end_date,
            '2026-03-31'::DATE AS campaign_reference_date,
            
            -- Medication lookback dates (from START_DAT)
            '2025-03-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2024-09-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start
            '2023-09-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start
            '2022-09-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start
            
            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2023-09-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before autumn start
            '2025-08-31'::DATE AS asthma_steroid_window_1_end,          -- Up to autumn 2025
            '2024-04-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from spring 2024
            '2026-03-31'::DATE AS asthma_steroid_window_2_end,          -- Up to spring 2026
            '2024-07-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2026-06-30'::DATE AS asthma_steroid_window_3_end,          -- Up to spring 2026 end
            
            -- Vaccination tracking dates
            '2025-09-01'::DATE AS vaccination_tracking_start,
            '2026-03-31'::DATE AS vaccination_tracking_end,
            '2025-08-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2026-06-30'::DATE AS decline_tracking_end,                 -- Through spring period
            
            -- Pregnancy tracking (campaign-specific)
            '2025-01-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign
            '2025-09-01'::DATE AS pregnancy_current_start,              -- Campaign period start  
            '2026-06-30'::DATE AS pregnancy_current_end,                -- Through spring period
            '2025-01-14'::DATE AS gestational_diabetes_start,          -- Gestational diabetes tracking
            
            -- Immunosuppression age cap (NULL = no upper age limit)
            75 AS immuno_max_age_years,                                 -- <75 in 2025/26, 75+ covered by age group

            -- Individual condition eligibility flags (2025/26 restricted campaigns)
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,
            TRUE AS eligible_care_home,
            FALSE AS eligible_asthma,                    -- Not eligible in 2025/26
            FALSE AS eligible_chronic_heart_disease,     -- Not eligible in 2025/26
            FALSE AS eligible_chronic_kidney_disease,    -- Not eligible in 2025/26
            FALSE AS eligible_diabetes,                  -- Not eligible in 2025/26
            FALSE AS eligible_chronic_liver_disease,     -- Not eligible in 2025/26
            FALSE AS eligible_chronic_neurological_disease, -- Not eligible in 2025/26
            FALSE AS eligible_chronic_respiratory_disease, -- Not eligible in 2025/26
            FALSE AS eligible_morbid_obesity,            -- Not eligible in 2025/26
            FALSE AS eligible_asplenia,                  -- Not eligible in 2025/26
            FALSE AS eligible_learning_disability,       -- Not eligible in 2025/26
            FALSE AS eligible_severe_mental_illness,     -- Not eligible in 2025/26
            FALSE AS eligible_pregnancy,                 -- Not eligible in 2025/26
            FALSE AS eligible_gestational_diabetes,      -- Not eligible in 2025/26
            FALSE AS eligible_homeless,                  -- Not eligible in 2025/26

            -- Minimum age for the age-based cohort
            75 AS age_based_min_age,

            -- Minimum age for the care home resident cohort. The Group K denominator in
            -- spec v3.5 (2025/26) reads AGE >= 65 at 31/03/2026 AND LONGRES_GROUP. The
            -- historical appendix in v3.6 and v4.0 summarises Group K as "aged 18 years
            -- or more", which contradicts that table; the denominator table is followed.
            65 AS care_home_min_age,

            -- RUN_DAT for this period (spec 2.1). Caps how late an observation can be
            -- and still count toward this campaign.
            -- RECALL_IMMUNO_GROUP lookbacks, measured from RUN_DAT (spec table 10).
            -- Group M and its predecessors select the recall group, not IMMUNO_GROUP.
            '2025-09-30'::DATE AS recall_immuno_medication_lookback_date,   -- RUN_DAT - 6 months
            '2023-03-31'::DATE AS recall_immuno_admin_lookback_date,        -- RUN_DAT - 3 years

            -- UKHSA COVID SCT codeclusters version this season is reported under (last release
            -- on or before RUN_DAT); see stg_reference_ukhsa_codecluster_versions
            '3.7.1' AS terminology_version,

            '2026-03-31'::DATE AS audit_end_date

    {%- elif campaign_id == 'COVID Autumn 2026' -%}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            'Autumn 2026 COVID Vaccination Campaign' AS campaign_name,
            'covid_2026_27' AS campaign_year,
            'autumn' AS campaign_period,

            -- Core campaign dates (spec 2.1: START_DAT 01/09/26, REF_DAT and RUN_DAT 31/03/27)
            '2026-09-01'::DATE AS campaign_start_date,
            '2027-03-31'::DATE AS campaign_end_date,
            '2027-03-31'::DATE AS campaign_reference_date,

            -- Medication and admin lookbacks, measured from START_DAT (spec 2.3)
            '2026-03-01'::DATE AS immuno_medication_lookback_date,      -- START_DAT - 6 months
            '2025-08-31'::DATE AS asthma_medication_lookback_date,      -- START_DAT - 366 days
            '2024-08-31'::DATE AS asthma_admission_lookback_date,       -- START_DAT - 731 days
            '2023-09-01'::DATE AS immuno_admin_lookback_date,           -- START_DAT - 3 years

            -- Asthma oral steroid windows: absolute for the collection year and shared by
            -- the autumn and spring periods (spec ASTRXM2E1/L1, E2/L2, E3/L3)
            '2024-09-01'::DATE AS asthma_steroid_window_1_start,        -- from 1/9/2024
            '2026-08-31'::DATE AS asthma_steroid_window_1_end,          -- before 1/9/2026
            '2025-04-01'::DATE AS asthma_steroid_window_2_start,        -- from 1/4/2025
            '2027-03-31'::DATE AS asthma_steroid_window_2_end,          -- before 1/4/2027
            '2025-07-01'::DATE AS asthma_steroid_window_3_start,        -- from 1/7/2025
            '2027-06-30'::DATE AS asthma_steroid_window_3_end,          -- before 1/7/2027

            -- Vaccination tracking dates (spec COVADM1_DAT and COVRX1_DAT)
            '2026-09-01'::DATE AS vaccination_tracking_start,
            '2027-03-31'::DATE AS vaccination_tracking_end,
            '2026-08-01'::DATE AS decline_tracking_start,               -- spec COVDECL_DAT from 01/08/26
            '2027-06-30'::DATE AS decline_tracking_end,                 -- spec COVDECL_DAT to 30/06/27

            -- Pregnancy tracking (spec PREGDEL26_DAT, PREG26A_DAT, PREG26B_DAT)
            '2026-01-01'::DATE AS pregnancy_lookback_start,             -- 8 months before START_DAT
            '2026-09-01'::DATE AS pregnancy_current_start,
            '2027-06-30'::DATE AS pregnancy_current_end,
            '2026-01-14'::DATE AS gestational_diabetes_start,           -- spec GDIAB_DAT from 14/01/26

            -- Immunosuppression age cap (NULL = no upper age limit)
            75 AS immuno_max_age_years,                                 -- under 75; 75+ covered by the age group

            -- Individual condition eligibility flags. The Autumn 2026 offer is 75 and over,
            -- care home residents aged 65 and over, and recently or currently immunosuppressed
            -- people under 75 (spec 5.1.1 Group M), so the wider clinical risk groups are not
            -- offered a vaccine this campaign.
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,
            TRUE AS eligible_care_home,
            FALSE AS eligible_asthma,
            FALSE AS eligible_chronic_heart_disease,
            FALSE AS eligible_chronic_kidney_disease,
            FALSE AS eligible_diabetes,
            FALSE AS eligible_chronic_liver_disease,
            FALSE AS eligible_chronic_neurological_disease,
            FALSE AS eligible_chronic_respiratory_disease,
            FALSE AS eligible_morbid_obesity,
            FALSE AS eligible_asplenia,
            FALSE AS eligible_learning_disability,
            FALSE AS eligible_severe_mental_illness,
            FALSE AS eligible_pregnancy,
            FALSE AS eligible_gestational_diabetes,
            FALSE AS eligible_homeless,

            -- Minimum age for the age-based cohort
            75 AS age_based_min_age,

            -- Minimum age for the care home resident cohort (spec Group M denominator)
            65 AS care_home_min_age,

            -- RUN_DAT for this period (spec 2.1). Caps how late an observation can be
            -- and still count toward this campaign.
            -- RECALL_IMMUNO_GROUP lookbacks, measured from RUN_DAT (spec table 10).
            -- Group M and its predecessors select the recall group, not IMMUNO_GROUP.
            '2026-09-30'::DATE AS recall_immuno_medication_lookback_date,   -- RUN_DAT - 6 months
            '2024-03-31'::DATE AS recall_immuno_admin_lookback_date,        -- RUN_DAT - 3 years

            -- UKHSA COVID SCT codeclusters version this season is reported under (last release
            -- on or before RUN_DAT); see stg_reference_ukhsa_codecluster_versions
            '4.1' AS terminology_version,

            '2027-03-31'::DATE AS audit_end_date

    {%- elif campaign_id == 'COVID Spring 2027' -%}
        {#- Not published: this campaign is deliberately absent from
            covid_reported_campaign_ids(). Spec v4.0 gives the spring dates but defines
            call and recall for Autumn 2026 only, so the eligibility flags below are a
            placeholder carried from autumn and must be confirmed against the spring
            offer before the campaign is added to the reported list. #}
        SELECT
            '{{ campaign_id }}' AS campaign_id,
            'Spring 2027 COVID Vaccination Campaign' AS campaign_name,
            'covid_2026_27' AS campaign_year,
            'spring' AS campaign_period,

            -- Core campaign dates (spec 2.1: START_DAT 01/04/27, REF_DAT and RUN_DAT 30/06/27)
            '2027-04-01'::DATE AS campaign_start_date,
            '2027-06-30'::DATE AS campaign_end_date,
            '2027-06-30'::DATE AS campaign_reference_date,

            -- Medication and admin lookbacks, measured from START_DAT (spec 2.3)
            '2026-10-01'::DATE AS immuno_medication_lookback_date,      -- START_DAT - 6 months
            '2026-03-31'::DATE AS asthma_medication_lookback_date,      -- START_DAT - 366 days
            '2025-03-31'::DATE AS asthma_admission_lookback_date,       -- START_DAT - 731 days
            '2024-04-01'::DATE AS immuno_admin_lookback_date,           -- START_DAT - 3 years

            -- Asthma oral steroid windows: the same absolute collection-year windows as
            -- Autumn 2026 (spec ASTRXM2E1/L1, E2/L2, E3/L3)
            '2024-09-01'::DATE AS asthma_steroid_window_1_start,        -- from 1/9/2024
            '2026-08-31'::DATE AS asthma_steroid_window_1_end,          -- before 1/9/2026
            '2025-04-01'::DATE AS asthma_steroid_window_2_start,        -- from 1/4/2025
            '2027-03-31'::DATE AS asthma_steroid_window_2_end,          -- before 1/4/2027
            '2025-07-01'::DATE AS asthma_steroid_window_3_start,        -- from 1/7/2025
            '2027-06-30'::DATE AS asthma_steroid_window_3_end,          -- before 1/7/2027

            -- Vaccination tracking dates (spec COVADM2_DAT and COVRX2_DAT)
            '2027-04-01'::DATE AS vaccination_tracking_start,
            '2027-06-30'::DATE AS vaccination_tracking_end,
            -- COVDECL_DAT is one window for the whole collection year (spec 2.3), so this
            -- matches Autumn 2026 rather than being narrowed to the spring period the way
            -- earlier spring campaigns in this file are. A decline recorded at any point in
            -- the year counts against the spring offer, which is what the spec states.
            '2026-08-01'::DATE AS decline_tracking_start,
            '2027-06-30'::DATE AS decline_tracking_end,

            -- Pregnancy tracking. Like the steroid windows, PREG26_GROUP is defined once for
            -- the collection year and shared by the autumn and spring periods, so these match
            -- Autumn 2026 rather than anchoring on the spring start date the way earlier
            -- spring campaigns in this file do (spec PREGDEL26_DAT, PREG26A_DAT, PREG26B_DAT).
            '2026-01-01'::DATE AS pregnancy_lookback_start,
            '2026-09-01'::DATE AS pregnancy_current_start,
            '2027-06-30'::DATE AS pregnancy_current_end,
            '2026-01-14'::DATE AS gestational_diabetes_start,           -- spec GDIAB_DAT from 14/01/26

            -- Immunosuppression age cap (NULL = no upper age limit)
            75 AS immuno_max_age_years,                                 -- under 75; 75+ covered by the age group

            -- Placeholder cohorts carried from Autumn 2026. Not reported: see the note at
            -- the head of this branch.
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,
            TRUE AS eligible_care_home,
            FALSE AS eligible_asthma,
            FALSE AS eligible_chronic_heart_disease,
            FALSE AS eligible_chronic_kidney_disease,
            FALSE AS eligible_diabetes,
            FALSE AS eligible_chronic_liver_disease,
            FALSE AS eligible_chronic_neurological_disease,
            FALSE AS eligible_chronic_respiratory_disease,
            FALSE AS eligible_morbid_obesity,
            FALSE AS eligible_asplenia,
            FALSE AS eligible_learning_disability,
            FALSE AS eligible_severe_mental_illness,
            FALSE AS eligible_pregnancy,
            FALSE AS eligible_gestational_diabetes,
            FALSE AS eligible_homeless,

            -- Minimum age for the age-based cohort
            75 AS age_based_min_age,

            -- Minimum age for the care home resident cohort
            65 AS care_home_min_age,

            -- RUN_DAT for this period (spec 2.1). Caps how late an observation can be
            -- and still count toward this campaign.
            -- RECALL_IMMUNO_GROUP lookbacks, measured from RUN_DAT (spec table 10).
            -- Group M and its predecessors select the recall group, not IMMUNO_GROUP.
            '2026-12-30'::DATE AS recall_immuno_medication_lookback_date,   -- RUN_DAT - 6 months
            '2024-06-30'::DATE AS recall_immuno_admin_lookback_date,        -- RUN_DAT - 3 years

            -- UKHSA COVID SCT codeclusters version this season is reported under (last release
            -- on or before RUN_DAT); see stg_reference_ukhsa_codecluster_versions
            '4.1' AS terminology_version,

            '2027-06-30'::DATE AS audit_end_date

    {%- else -%}
        -- Default to current campaign if unknown campaign_id
        {{ covid_campaign_config('COVID Autumn 2026') }}
    {%- endif -%}
{% endmacro %}

/*
Helper macro to get a specific campaign date
Usage: {{ covid_get_campaign_date('campaign_reference_date') }}
*/
{% macro covid_get_campaign_date(date_name, campaign_id=none) %}
    {%- set campaign_id = campaign_id or var('covid_current_campaign', 'COVID Autumn 2026') -%}
    (SELECT {{ date_name }} FROM ({{ covid_campaign_config(campaign_id) }}))
{% endmacro %}