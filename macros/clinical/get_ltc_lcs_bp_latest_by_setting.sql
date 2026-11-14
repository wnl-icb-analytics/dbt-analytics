{% macro get_ltc_lcs_bp_latest_by_setting(as_at='current_date()', population=none) %}
{# Latest LTC LCS blood pressure readings by setting.
   Default: one row per person with BP evidence, as at as_at.
   population: optional query returning person_id and reporting_date. Returns one row per
   population row as at its reporting_date, with reporting_date added.
   EMIS links BP components by calendar date before ranking eligible records by date
   (save time, then observation ID, break same-day ties). Each day's winner depends only on
   that day's records, so the latest qualifying day on or before the reference date gives
   the EMIS selection. Only the home-setting wrapper ranks before linking. Clinic/home pools
   use the CKD PG2 value-set instances shared by the HTN, PAD and Stroke chains.
   Diabetes retains its distinct BP_COD and component pools. No plausibility fallback applies. #}
with bp as (
    select
        person_id,
        observation_id,
        clinical_effective_date::date as bp_date,
        date_recorded,
        concept_code,
        result_value,
        boolor_agg(valueset_id = 'b0862f52-94d7-4831-9c3c-fa2cc8f8ca98') as is_clinic_marker,
        boolor_agg(valueset_id = 'b2592e74-731e-0d97-247a-ac3839913b70') as is_home_marker,
        boolor_agg(valueset_id = 'c2e9b41e-e9f9-5eee-f816-ee2543dddbdd' and result_value > 0) as is_systolic,
        boolor_agg(valueset_id = '3ddd36f2-7c5b-e43f-4620-46a0eb1283ee' and result_value > 0) as is_diastolic,
        boolor_agg(valueset_id = 'f27b2fe6-df28-de7d-5baa-089badef474d' and result_value > 0) as is_clinic_systolic,
        boolor_agg(valueset_id = '4c90ff98-51ec-2b89-e0c7-dc2ab98daed9' and result_value > 0) as is_clinic_diastolic,
        boolor_agg(valueset_id = '830a121c-275c-0ebc-990e-e9699b9875c4' and result_value > 0) as is_home_systolic,
        boolor_agg(valueset_id = '5aad8a47-dfac-2d96-493d-44fc69d61770' and result_value > 0) as is_home_diastolic,
        boolor_agg(valueset_id = '09a3d8a7-e54b-d710-9a86-58b3de69d1f4') as is_diabetes_marker,
        boolor_agg(valueset_id = '8da5f7b1-024b-c20c-97e9-415ea3b9a64a') as is_diabetes_link_systolic,
        boolor_agg(valueset_id = '5083eb15-91a9-f20e-7fb1-06e908a70e5f') as is_diabetes_systolic,
        boolor_agg(valueset_id = 'dbace9e1-ca62-3166-049c-82206d3e041d') as is_diabetes_diastolic
    from ({{ get_ltc_lcs_observations(
        "b0862f52-94d7-4831-9c3c-fa2cc8f8ca98,
         b2592e74-731e-0d97-247a-ac3839913b70,
         c2e9b41e-e9f9-5eee-f816-ee2543dddbdd,
         3ddd36f2-7c5b-e43f-4620-46a0eb1283ee,
         f27b2fe6-df28-de7d-5baa-089badef474d,
         4c90ff98-51ec-2b89-e0c7-dc2ab98daed9,
         830a121c-275c-0ebc-990e-e9699b9875c4,
         5aad8a47-dfac-2d96-493d-44fc69d61770,
         09a3d8a7-e54b-d710-9a86-58b3de69d1f4,
         8da5f7b1-024b-c20c-97e9-415ea3b9a64a,
         5083eb15-91a9-f20e-7fb1-06e908a70e5f,
         dbace9e1-ca62-3166-049c-82206d3e041d",
        include_date_recorded=true,
        exclude_result_headers=true
    ) }})
    {% if population is none %}
    where clinical_effective_date::date <= {{ as_at }}
    {% endif %}
    group by person_id, observation_id, bp_date, date_recorded, concept_code, result_value
),

abpm_codes as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where cluster_id = 'ABPM_COD'
),
bp_days as (
    select
        person_id,
        bp_date,
        boolor_agg(is_clinic_marker) as has_clinic_marker,
        boolor_agg(is_home_marker) as has_home_marker,
        boolor_agg(is_systolic) as has_systolic,
        boolor_agg(is_clinic_systolic) as has_clinic_systolic,
        boolor_agg(is_home_systolic) as has_home_systolic,
        boolor_agg(is_diabetes_marker) as has_diabetes_marker,
        boolor_agg(is_diabetes_link_systolic) as has_diabetes_link_systolic
    from bp
    group by person_id, bp_date
),


-- Day winners: one record per person and qualifying date, latest save time then ID.
day_diastolic as (
    select person_id, bp_date, is_clinic_marker
    from bp
    where is_diastolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days
            where has_systolic and (has_clinic_marker or has_home_marker)
        )
    qualify row_number() over (partition by person_id, bp_date order by date_recorded desc nulls last, observation_id desc) = 1
),

day_clinic as (
    select
        d.person_id,
        d.bp_date,
        d.date_recorded,
        d.observation_id,
        d.result_value as diastolic_value,
        s.bp_date as systolic_bp_date,
        s.result_value as systolic_value
    from bp as d
    left join (
        select person_id, bp_date, result_value
        from bp
        where is_clinic_systolic
        qualify row_number() over (partition by person_id, bp_date order by date_recorded desc nulls last, observation_id desc) = 1
    ) as s
        on d.person_id = s.person_id and d.bp_date = s.bp_date
    where d.is_clinic_diastolic
        and (d.person_id, d.bp_date) in (
            select person_id, bp_date from bp_days where has_clinic_systolic and has_clinic_marker
        )
    qualify row_number() over (partition by d.person_id, d.bp_date order by d.date_recorded desc nulls last, d.observation_id desc) = 1
),

day_home as (
    select
        d.person_id,
        d.bp_date,
        d.date_recorded,
        d.observation_id,
        d.result_value as diastolic_value,
        d.concept_code in (select code from abpm_codes) as is_abpm,
        s.bp_date as systolic_bp_date,
        s.result_value as systolic_value
    from bp as d
    left join (
        select person_id, bp_date, result_value
        from bp
        where is_home_systolic
        qualify row_number() over (partition by person_id, bp_date order by date_recorded desc nulls last, observation_id desc) = 1
    ) as s
        on d.person_id = s.person_id and d.bp_date = s.bp_date
    where d.is_home_diastolic
        and (d.person_id, d.bp_date) in (
            select person_id, bp_date from bp_days where has_home_systolic and has_home_marker
        )
    qualify row_number() over (partition by d.person_id, d.bp_date order by d.date_recorded desc nulls last, d.observation_id desc) = 1
),

day_diabetes_systolic as (
    select person_id, bp_date, result_value
    from bp
    where is_diabetes_systolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days where has_diabetes_marker and has_diabetes_link_systolic
        )
    qualify row_number() over (partition by person_id, bp_date order by date_recorded desc nulls last, observation_id desc) = 1
),

day_diabetes_diastolic as (
    select person_id, bp_date, result_value
    from bp
    where is_diabetes_diastolic
        and (person_id, bp_date) in (
            select person_id, bp_date from bp_days where has_diabetes_marker and has_diabetes_link_systolic
        )
    qualify row_number() over (partition by person_id, bp_date order by date_recorded desc nulls last, observation_id desc) = 1
),

targets as (
    {% if population is none %}
    select distinct person_id, ({{ as_at }})::date as reporting_date from bp
    {% else %}
    select person_id, reporting_date from ({{ population }})
    {% endif %}
),

-- Latest qualifying day on or before each reference date, one pool per step.
with_ld as (
    select t.person_id, t.reporting_date,
        ld.bp_date as ld_bp_date, ld.is_clinic_marker as ld_is_clinic_marker
    from targets as t
    asof join day_diastolic as ld
        match_condition (t.reporting_date >= ld.bp_date)
        on t.person_id = ld.person_id
),

with_lc as (
    select w.*,
        lc.bp_date as lc_bp_date, lc.date_recorded as lc_date_recorded,
        lc.observation_id as lc_observation_id, lc.diastolic_value as lc_diastolic_value,
        lc.systolic_bp_date as lc_systolic_bp_date, lc.systolic_value as lc_systolic_value
    from with_ld as w
    asof join day_clinic as lc
        match_condition (w.reporting_date >= lc.bp_date)
        on w.person_id = lc.person_id
),

with_lh as (
    select w.*,
        lh.bp_date as lh_bp_date, lh.date_recorded as lh_date_recorded,
        lh.observation_id as lh_observation_id, lh.diastolic_value as lh_diastolic_value,
        lh.is_abpm as lh_is_abpm,
        lh.systolic_bp_date as lh_systolic_bp_date, lh.systolic_value as lh_systolic_value
    from with_lc as w
    asof join day_home as lh
        match_condition (w.reporting_date >= lh.bp_date)
        on w.person_id = lh.person_id
),

with_ds as (
    select w.*, ds.bp_date as ds_bp_date, ds.result_value as ds_value
    from with_lh as w
    asof join day_diabetes_systolic as ds
        match_condition (w.reporting_date >= ds.bp_date)
        on w.person_id = ds.person_id
),

selected as (
    select w.*, dd.bp_date as dd_bp_date, dd.result_value as dd_value
    from with_ds as w
    asof join day_diabetes_diastolic as dd
        match_condition (w.reporting_date >= dd.bp_date)
        on w.person_id = dd.person_id
)

select
    p.person_id,
    {% if population is not none %}
    p.reporting_date,
    {% endif %}
    -- Clinic setting: latest linked positive diastolic is clinic-coded.
    p.ld_bp_date as latest_reading_date,
    coalesce(p.ld_is_clinic_marker and ldd.has_systolic, false) as is_latest_reading_clinic,
    -- Home setting: latest home reading is valid and no valid clinic reading is later.
    coalesce(
        p.lh_systolic_bp_date = p.lh_bp_date
        and lhdd.has_home_marker
        and (
            p.lc_bp_date is null
            or not (lcdd.has_clinic_systolic and lcdd.has_clinic_marker)
            or p.lc_bp_date < p.lh_bp_date
            or (p.lc_bp_date = p.lh_bp_date and (
                p.lc_date_recorded < p.lh_date_recorded
                or (p.lc_date_recorded is null and p.lh_date_recorded is not null)
                or (equal_null(p.lc_date_recorded, p.lh_date_recorded)
                    and p.lc_observation_id <= p.lh_observation_id)
            ))
        ),
        false
    ) as is_latest_reading_home,
    -- Clinic reading: latest complete linked date, then latest systolic on that date.
    iff(p.lc_bp_date = p.lc_systolic_bp_date and lcdd.has_clinic_marker, p.lc_systolic_bp_date, null) as clinic_reading_date,
    iff(p.lc_bp_date = p.lc_systolic_bp_date and lcdd.has_clinic_marker, p.lc_systolic_value, null) as clinic_systolic_value,
    iff(p.lc_bp_date = p.lc_systolic_bp_date and lcdd.has_clinic_marker, p.lc_diastolic_value, null) as clinic_diastolic_value,
    -- Home reading: same rule with the home sets.
    iff(p.lh_bp_date = p.lh_systolic_bp_date and lhdd.has_home_marker, p.lh_systolic_bp_date, null) as home_reading_date,
    iff(p.lh_bp_date = p.lh_systolic_bp_date and lhdd.has_home_marker, p.lh_systolic_value, null) as home_systolic_value,
    iff(p.lh_bp_date = p.lh_systolic_bp_date and lhdd.has_home_marker, p.lh_diastolic_value, null) as home_diastolic_value,
    coalesce(p.lh_is_abpm, false) as is_home_reading_abpm,
    -- Diabetes ranks each component among dates with BP_COD and linked systolic evidence.
    iff(dss.has_diabetes_marker and dss.has_diabetes_link_systolic, p.ds_value, null) as diabetes_systolic_value,
    iff(dsd.has_diabetes_marker and dsd.has_diabetes_link_systolic, p.dd_value, null) as diabetes_diastolic_value
from selected as p
left join bp_days as ldd on ldd.person_id = p.person_id and ldd.bp_date = p.ld_bp_date
left join bp_days as lcdd on lcdd.person_id = p.person_id and lcdd.bp_date = p.lc_bp_date
left join bp_days as lhdd on lhdd.person_id = p.person_id and lhdd.bp_date = p.lh_bp_date
left join bp_days as dss on dss.person_id = p.person_id and dss.bp_date = p.ds_bp_date
left join bp_days as dsd on dsd.person_id = p.person_id and dsd.bp_date = p.dd_bp_date
{% endmacro %}
