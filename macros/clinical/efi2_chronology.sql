{% macro efi2_chronology(
    rules_relation,
    patient_list_relation,
    weights_relation,
    medication_order_relation
) %}
{# Shared eFI2 chronology for current and monthly scoring. #}

with
    rules_to_score as (
        select *
        from {{ rules_relation }}
        {% if is_incremental() %}
        where {{ rebuild_month_window('end_date') }}
        {% endif %}
    ),

    patients_to_score as (
        select *
        from {{ patient_list_relation }}
        {% if is_incremental() %}
        where {{ rebuild_month_window('end_date') }}
        {% endif %}
    ),

    deficit_weights as (
        select
            id.person_id,
            upper(id.deficit) as deficit,
            id.other_instructions,
            id.has_deficit,
            id.sub_deficit,
            id.end_date,
            id.last_date,
            ew.detail_key,
            ew.weight
        from rules_to_score id
        left join {{ weights_relation }} ew
            on lower(id.deficit) = lower(ew.deficit)
        where weight is not null
    ),

    -- Annoying dementia rules
    cognitive as (
        select person_id, end_date, max(weight) as weight, 'MAX_DEMENTIA' as deficit
        from deficit_weights
        where
            deficit in ('MEMORY CONCERNS', 'COGNITIVE IMPAIRMENT', 'DEMENTIA')
            and has_deficit = true
        group by person_id, end_date
    ),

    max_dementia as (
        select
            c.person_id,
            c.deficit,
            dw.other_instructions,
            dw.has_deficit,
            dw.sub_deficit,
            dw.end_date,
            dw.last_date,
            dw.detail_key,
            c.weight
        from cognitive c
        left join
            deficit_weights dw
            on c.person_id = dw.person_id
            and c.end_date = dw.end_date
            and c.weight = dw.weight
        where
            c.deficit = 'MAX_DEMENTIA'
            and sub_deficit in (
                'Dementia', 'COGNITIVE_SCORE', 'COGNITIVE_IMPAIRMENT', 'DEMENTIA', 'MEMORY_CONCERNS'
            )
    ),

    -- Alcohol — map each person's drinking history over the lookback window to one of
    -- the weighted alcohol states. The window (5 years per spec p.3) is enforced
    -- upstream by the alcohol codelist's time_constraint_years in
    -- int_efi2_cohort_snomed_codes, so it is not re-applied here.
    --
    -- The weights table keys alcohol as HARMFUL / PREVIOUS_HIGHER_HARMFUL / MISSING,
    -- NOT the raw reading bands ('Harmful drinking', 'Higher risk drinking', ...), so
    -- the state cannot be string-matched from the band; it is derived from the reading
    -- bands and their recency:
    --   PREVIOUS_HIGHER_HARMFUL : any EARLIER-dated reading in the window was higher-risk
    --                             or harmful. This takes precedence over HARMFUL and
    --                             carries the larger weight (0.162 vs 0.027) regardless of
    --                             the current reading — a documented history of heavy
    --                             drinking is the stronger frailty signal, whether or not
    --                             the person still drinks harmfully now.
    --   HARMFUL                 : the most recent reading is harmful (49+ units/week) and
    --                             there is no earlier higher/harmful reading (e.g. a lone
    --                             current harmful reading, or an escalation from lower).
    --   (neither)               : only ever zero / lower-risk -> no alcohol deficit
    -- MISSING (no reading at all) is handled separately by missing_alcohol.
    --
    -- "Previous" is defined on earlier-dated readings only, so that a single current
    -- harmful reading with no history still resolves to HARMFUL rather than making that
    -- state unreachable. Confirm the precedence and the current-vs-previous split
    -- against spec p.3 / the lds definition.
    alcohol_readings as (
        -- one row per person per alcohol reading (distinct collapses the weight
        -- fan-out in deficit_weights), with the reading's band ranked by severity
        select distinct
            person_id,
            end_date,
            last_date,
            case lower(other_instructions)
                when 'harmful drinking' then 3
                when 'higher risk drinking' then 2
                when 'lower risk drinking' then 1
                when 'zero alcohol' then 0
            end as band_rank
        from deficit_weights
        where deficit = 'ALCOHOL' and other_instructions is not null
    ),

    alcohol_ranked as (
        select
            person_id,
            end_date,
            last_date,
            band_rank,
            max(last_date) over (partition by person_id, end_date) as latest_date
        from alcohol_readings
        where band_rank is not null
    ),

    alcohol_state as (
        select
            person_id,
            end_date,
            case
                -- a prior (earlier-dated) higher-risk/harmful reading wins, taking the
                -- larger weight regardless of current status
                when
                    max(case when last_date < latest_date and band_rank >= 2 then 1 else 0 end)
                    = 1
                then 'PREVIOUS_HIGHER_HARMFUL'
                -- else currently harmful (any latest-dated reading is harmful) with no
                -- prior higher/harmful history
                when
                    max(case when last_date = latest_date and band_rank = 3 then 1 else 0 end)
                    = 1
                then 'HARMFUL'
            end as detail_key
        from alcohol_ranked
        group by person_id, end_date
    ),

    max_alcohol as (
        select
            s.person_id,
            'MAX_ALCOHOL' as deficit,
            cast(null as varchar) as other_instructions,
            true as has_deficit,
            'ALCOHOL' as sub_deficit,
            s.end_date,
            s.end_date as last_date,
            s.detail_key,
            ew.weight
        from alcohol_state s
        left join {{ weights_relation }} ew
            on lower(ew.deficit) = 'alcohol' and ew.detail_key = s.detail_key
        where s.detail_key is not null
    ),

    -- Full scored population (one row per person), sourced from the patient_list
    -- spine, NOT int_efi2_cohort_snomed_codes. The cohort table is an inner join to
    -- the codelist, so it only contains persons with >= 1 eFI2 code. The paper
    -- assigns a "missing" lifestyle category (BMI, alcohol) so that patients with no
    -- lifestyle data still receive a score, and evaluates polypharmacy across the
    -- whole population. Drawing from the cohort table instead would deny the
    -- missing-lifestyle weights and polypharmacy to code-sparse persons, understating
    -- their score. Patient_list is already the correct denominator (living, age >= 65).
    person_unique as (
        select distinct person_id, end_date from patients_to_score
    ),

    missing_bmi as (
        select
            pu.person_id,
            'BMI' as deficit,
            dw.other_instructions,
            true as has_deficit,
            dw.sub_deficit,
            pu.end_date,
            dw.last_date,
            'MISSING' as detail_key
        from person_unique pu
        left join
            (select * from deficit_weights where deficit = 'BMI') dw
            on pu.person_id = dw.person_id
            and pu.end_date = dw.end_date
        where deficit is null
    ),

    missing_bmi_score as (
        select
            person_id,
            mb.deficit,
            other_instructions,
            has_deficit,
            sub_deficit,
            end_date,
            end_date as last_date,
            mb.detail_key,
            ew.weight
        from missing_bmi mb
        left join
            {{ weights_relation }} ew
            on lower(mb.deficit) = lower(ew.deficit)
            and lower(mb.detail_key) = lower(ew.detail_key)
    ),

    missing_alcohol as (
        select
            pu.person_id,
            'ALCOHOL' as deficit,
            dw.other_instructions,
            true as has_deficit,
            dw.sub_deficit,
            pu.end_date,
            dw.last_date,
            'MISSING' as detail_key
        from person_unique pu
        left join
            (select * from deficit_weights where deficit = 'ALCOHOL') dw
            on pu.person_id = dw.person_id
            and pu.end_date = dw.end_date
        where deficit is null
    ),

    missing_alcohol_score as (
        select
            person_id,
            ma.deficit,
            other_instructions,
            has_deficit,
            sub_deficit,
            end_date,
            end_date as last_date,
            ma.detail_key,
            ew.weight
        from missing_alcohol ma
        left join
            {{ weights_relation }} ew
            on lower(ma.deficit) = lower(ew.deficit)
            and ma.detail_key = ew.detail_key
    ),

    -- Present BMI — re-add persons who DO have a BMI reading in a deficit band
    -- (obese / underweight), carrying the weight for that band. Counterpart to
    -- missing_bmi_score, which only covers persons with no BMI reading at all.
    -- BMI is excluded from the deficit_weights passthrough in all_scores because
    -- the weights join (on deficit only) fans each BMI person across every BMI
    -- detail_key (Obese / Underweight / MISSING); here we keep the single row
    -- whose detail_key matches the person's band. Matched case-insensitively,
    -- consistent with the deficit_weights join convention. Without this CTE only
    -- the MISSING weight is ever scored and the obese / underweight weights reach
    -- nobody.
    present_bmi_score as (
        select
            person_id,
            deficit,
            other_instructions,
            has_deficit,
            sub_deficit,
            end_date,
            last_date,
            detail_key,
            weight
        from deficit_weights
        where
            deficit = 'BMI'
            and has_deficit = true
            and lower(detail_key) = lower(sub_deficit)
    ),

    -- Polypharmacy — count of distinct BNF sub-subchapters (level 3 - note not
    -- real level so this is the agreed interpretation) among meds
    -- prescribed in the 90 days before end_date, per the eFI2 paper: "Number of
    -- medications from different BNF sub-subchapters (level 3) prescribed in
    -- previous 90 days". Level 3 = BNF paragraph = the first 6 characters of the
    -- BNF code (chars 1-2 chapter, 3-4 section, 5-6 paragraph) - the same
    -- definition as int_medication_order_bnf.bnf_paragraph.
    --
    -- NOTE - no code exclusions applied here (differs from this repo's
    -- int_polypharmacy_* pipeline). That pipeline scopes to NHSBSA in-scope BNF
    -- chapters via the bnf_polypharmacy_exclusions seed; the lds pipeline
    -- counts across ALL BNF sub-subchapters, so we deliberately do NOT apply those
    -- exclusions - every prescribed med with a BNF code counts. Applying
    -- the exclusion list as minimising divergence.
    relevant_meds as (
        select
            im.person_id,
            pu.end_date,
            left(im.bnf_code, 6) as bnf_sub_subchapter
        from {{ medication_order_relation }} im
        left join person_unique pu on im.person_id = pu.person_id
        where
            im.bnf_code is not null
            and pu.end_date is not null
            -- Bound the medication scan to the earliest 90-day window being
            -- scored before probing it against every month-end. No order the
            -- window predicate below would keep can fall before this date.
            and im.clinical_effective_date
            >= dateadd('day', -90, (select min(end_date) from person_unique))
            and datediff('day', im.clinical_effective_date, pu.end_date) between 0 and 90
    ),

    polypharmacy_counts as (
        select
            person_id,
            end_date,
            count(distinct bnf_sub_subchapter) as sub_subchapter_count
        from relevant_meds
        group by person_id, end_date
    ),

    polypharmacy as (
        select
            person_id,
            end_date,
            'POLYPHARMACY' as deficit,
            case
                when sub_subchapter_count between 5 and 9 then '5-9'
                when sub_subchapter_count >= 10 then '10+'
            end as detail_key,
            -- detail_key is non-null exactly when sub_subchapter_count >= 5
            sub_subchapter_count >= 5 as has_deficit
        from polypharmacy_counts
    ),

    polypharmacy_score as (
        select
            p.person_id,
            p.deficit,
            'Polypharmacy: 0-4, 5-9, 10+ meds from different BNF Chapters' as otherinstructions,
            p.has_deficit,
            'POLYPHARMACY' as sub_deficit,
            p.end_date,
            p.end_date as last_date,
            p.detail_key,
            ew.weight
        from polypharmacy p
        left join
            {{ weights_relation }} ew
            on lower(p.deficit) = lower(ew.deficit)
            and p.detail_key = ew.detail_key
    ),

    -- Now do smoking score - have to have most recent smoking status
    smoking as (
        select distinct person_id, end_date, deficit, last_date
        from deficit_weights
        where upper(deficit) in ('SMOKER (CURRENT)', 'SMOKER (EX)') and has_deficit = true
        qualify row_number() over (partition by person_id, end_date order by last_date desc) = 1
    ),

    smoking_score as (
        select
            s.person_id,
            s.deficit,
            'Cannot be current and ex smoker' as otherinstructions,
            case when s.deficit = 'SMOKER (CURRENT)' then true else false end as has_deficit,
            'SMOKING' as sub_deficit,
            s.end_date,
            s.last_date,
            null as detail_key,
            ew.weight
        from smoking s
        left join {{ weights_relation }} ew on lower(s.deficit) = lower(ew.deficit)
    ),

    -- Now union all the scores
    all_scores as (
        select *
        from deficit_weights
        where
            upper(deficit) not in (
                'SMOKER (CURRENT)',
                'SMOKER (EX)',
                'MEMORY CONCERNS',
                'COGNITIVE IMPAIRMENT',
                'DEMENTIA',
                'ALCOHOL',
                'BMI'
            )
            and has_deficit = true

        union all

        {% for rule_cte in [
            "max_dementia",
            "max_alcohol",
            "missing_bmi_score",
            "present_bmi_score",
            "missing_alcohol_score",
            "polypharmacy_score",
            "smoking_score",
        ] %}
            select * from {{ rule_cte }} where has_deficit = true {{ "union all" if not loop.last }}
        {% endfor %}
    ),
    -- as some of the rules are not properly handled upstream, a deficit that isn't explicitly
    -- handled can be added as many times as it is mentioned. One example of this is `Skin
    -- Ulcers`. To get around this we need to take an aggregated value for each person, deficit,
    -- end date, and weight.
    -- TODO: we could fix this in the future however should probably go hand in hand with a larger
    -- bit of work in refactoring the eFIv2 code in general
    aggregated_values as (
        select distinct person_id, deficit, end_date, detail_key, weight from all_scores
    )

select person_id, end_date, sum(weight) as efi
from aggregated_values
group by person_id, end_date
{% endmacro %}
