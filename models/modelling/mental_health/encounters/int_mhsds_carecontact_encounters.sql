select
    {{ dbt_utils.generate_surrogate_key([
        'c.uniq_serv_req_id',
        'c.uniq_care_cont_id'
    ]) }} as encounter_id
    , b.sk_patient_id
    , c.org_id_prov
    , c.attend_status
    , case
        when c.attend_status in ('5', '6') then 'Attended'
        when c.attend_status in ('3', '7') then 'DNA/Late'
        when c.attend_status = '2' then 'Cancelled by Patient'
        when c.attend_status = '4' then 'Cancelled by Provider'
        else 'Unknown'
    end as attendance_status
    , org.organisation_name as provider_name
    , case
        when c.org_id_prov = 'G6V2S' then 'NLFT'
        when c.org_id_prov = 'TAF' then 'C&I'
        when c.org_id_prov = 'RNK' then 'T&P'
        when c.org_id_prov = 'RRP' then 'BEH'
        when c.org_id_prov = 'RAT' then 'NELFT'
        when c.org_id_prov = 'RWK' then 'ELFT'
        when c.org_id_prov = 'RAL' then 'RFL'
        when c.org_id_prov = 'RKE' then 'WHIT'
        when c.org_id_prov = 'RKL' then 'WLT'
        when c.org_id_prov = 'RV5' then 'SLAM'
        when c.org_id_prov = 'RV3' then 'CNWL'
        when c.org_id_prov = 'RQY' then 'SWLSTG'
        else 'Other'
    end as provider_short_name
    , c.care_cont_date as start_date
    , c.clin_cont_dur_of_care_cont as duration
    , c.dm_icb_commissioner
    -- £302 National Cost Collection average, adjusted by the 15.7% NCL uplift.
    , 302 * 1.157 as proxy_cost
    , 'MHSDS' as source
from {{ ref('int_mhsds_latest_care_contact') }} as c
left join {{ ref('stg_dictionary_dbo_organisation') }} as org
    on c.org_id_prov = org.organisation_code
left join {{ ref('stg_mhsds_bridging') }} as b
    on c.person_id = b.person_id
