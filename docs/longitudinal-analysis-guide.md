# Longitudinal analysis guide

Use this guide with [the shared model definitions](longitudinal-activity.md).
`fct_person_healthcare_event` has one row per source milestone;
`fct_person_clinical_record` has one row per source clinical item. Different
milestones and different feeds can describe the same care. Temporal proximity
is association evidence, not causation.

The business owner is Eddie Davison. Each metric needs an approved protocol;
this guide does not choose a programme's journey rule or evaluation threshold.
IAPT is a prepared feed and is not unioned into these shared outputs.

## Coverage before a metric

`dq_person_activity_coverage` retains one row per output, dataset and source
record type. Its applicability counters identify which branches supply sites,
referral direction, status, attendance, outcome, recorded parents, maps and
results. These rules do not cover every source type listed in the
reconciliation limits below. For covered types, a zero applicable count means
the field is not supplied for that source/type. Numeric parse failures can be
legitimate enumerated non-score responses. Unit checks apply to populated
observations, not diagnoses, and need a concept-specific assessment before they
are called defects.

`dq_person_activity_provider_period` has one row per dataset, provider authority,
provider code and activity month for the last 24 completed calendar months.
It scans only the prepared event adapters. It retains null/local providers and
intervening gaps between observed activity or accepted-submission endpoints.
It excludes undated, month/year/unknown-precision and current/future-month
milestones; the source/type diagnostic still includes them. A provider absent
from both inputs is unknown, not covered by an invented roster.

The table counts all milestones, including bookings and slots. Those scheduled
counts remain separate even where a current status records attendance. They
are not encounter counts. The table is rebuilt monthly with `monthly-full` and
`person_activity_coverage` tags. Check `profiled_at`; rebuild it before a study
that needs newer evidence.

For MHSDS/CSDS, accepted submissions and submitted contact rows use the calendar
month of the reporting-period end. Activity uses the event month. These are
different time bases: an accepted file can contain older referrals or contacts,
and current facts select across submissions. The statuses are
`accepted_submission_observed`, `no_accepted_submission_observed` and
`submission_completeness_unknown`. None certifies completeness. A zero section,
a drop in volume or an intervening gap needs investigation with the source
submission diagnostic and notices. For sources without submission evidence here,
submission/contact counts are null rather than zero.

Supply the metric's expected provider-period grid, agreed source population,
source notices, successful loads/builds and cut-off before judging a window.
Do not replace null provider codes with current practice. Resolve provider names
through the appropriate authoritative reference; ODS and local identifiers must
remain distinct. Assess an absent qualifying event only when all required providers and the full
follow-up period have usable evidence.

## Observation lag

### Evidence measured on 11 October 2026

Header sample: October 2024 to July 2026 reporting periods, grouped into provider/period after reducing repeated header rows to submission grain. Each provider/period has equal weight. Receipt means `effective_from`; MHSDS/IAPT load means `dmic_date_added`. CSDS's second timestamp is upload, not independently measured warehouse loading.

| Source | First receipt p50 / p90 | Latest receipt p95 | First-to-latest receipt span p95 | Period end to load/upload p95 | Multiple submissions |
|---|---:|---:|---:|---:|---:|
| MHSDS | 19 / 27 days | 292 days | 269 days | 303 days | 42% |
| CSDS | 11 / 15 days | 45 days | 35 days | 45 days (upload) | 71% |
| IAPT | 21 / 27 days | 58 days | 36 days | 62 days | 68% |

These are observed retained submission histories, not national submission deadlines. Recent reporting periods have had less time to receive later replacements, so their histories are right-censored. MHSDS file-type strata confirm a long replacement tail: type 1 receipt p95 was 26 days, type 2 57 days, type 3 88 days, with later numeric file types reaching roughly 239 days in strata with at least 100 files. Receipt-to-load p95 was roughly 23-28 days in those strata. Do not assign a business label to those numeric file types without the source definition.

Activity sample: October 2025 to July 2026. One APC discharge, OP appointment slot or ECDS arrival per source record; selected e-RS actions; OLIDS appointment slots. These are record-weighted retained receipt/extraction ages, not measured first-arrival distributions.

| Source | Activity to retained receipt/extraction p50 / p90 / p95 | Interpretation |
|---|---:|---|
| SUS APC | 81 / 186 / 207 days | Measured from discharge, not admission. Open spells have a different ascertainment problem. |
| SUS OP | 74 / 183 / 203 days | Includes submitted attendance/non-attendance statuses. |
| ECDS | 76 / 173 / 197 days | Arrival basis; do not count later milestones as new attendances. |
| e-RS | 2 / 3 / 4 days | Recorded actions only. Scheduled slots excluded. |
| OLIDS | 141 / 266 / 281 days | Extraction of a current appointment snapshot. Does not estimate first delivery. About 8% of slots were after their extraction date. |

Missing delivery timestamps rounded to 0% in these cohorts. A separate raw acute query grouped retained versions by source key: multiple receipt dates rounded to 0%, and p95 first-to-last spans were zero for all three sources. Thus a measurable acute replacement history was not available. Zero observed span does not prove no resubmissions.

### Starting observation exclusions

These are conservative starting exclusions of recent activity, to be approved in the metric protocol. They are not programme evaluation thresholds or guarantees that the remainder is complete.

| Source | Proposed observation lag | Sensitivity and conditions |
|---|---|---|
| MHSDS | 330 days after period end | Round observed load p95 (~303 days) up to a 30-day block plus margin. Also report a provisional 90-day view and a 365-day view; later replacement remains possible. |
| CSDS | 60 days after period end | Round 45 days up. Compare 90 days. Verify actual load/build completion because headers supply upload/receipt evidence only. |
| IAPT, when unioned | 90 days after period end | Round 62 days up. Compare 120 days; check existing Refresh-to-Primary section-shortfall evidence. |
| SUS APC | 210 days after discharge/activity date | Round ~207 days up. Compare 270 days. Admission-based metrics must address open spells and late completion explicitly. |
| SUS OP | 210 days after appointment date | Round ~203 days up. Compare 270 days; check provider/month volumes and delivery notices. |
| ECDS | 210 days after arrival date | Round ~197 days up. Compare 270 days. |
| e-RS | 7 days after recorded action | Round p95 4 days to a week. Compare 14 days; this says nothing about whether a booked slot was attended. |
| OLIDS | 30 days as an unvalidated provisional buffer | First-delivery lag cannot be estimated from extraction snapshots. Compare 60 days; require practice/publisher confirmation, current extraction and the monthly reconciliation. Do not use this buffer alone to assert an absent clinical event. If absence is material and those checks are unavailable, mark the window unassessable. |

For monthly feeds, apply the cut-off to the end of the required reporting month, not just an event date. Require submissions covering the full target window. For mixed-source measures, the usable end is the earliest supported source/provider end, capped by successful adapter builds and reconciliations. Do not substitute `max(event_date)` or a future appointment for that end. Add the follow-up window to the index before checking eligibility. Freeze the extract/build dates used by the analysis.

The 210-day acute values are deliberately conservative retained-version guidance. Current storage does not support a claim that 95% first arrives within that lag. Re-estimate after delivery-history evidence becomes available. Observation lag cannot repair unlinked people, missing providers or care outside the included populations.

## Reconciliation

Use [the validation record](longitudinal-activity-validation.md) for existing
whole-population fingerprints, incremental replacement/withdrawal checks and
milestone totals. [The emission analysis](../analyses/navigation/source_emission_reconciliation.sql)
compares domain and prepared source counts with emitted rows.
It accounts for primary milestones, optional dates and e-RS action selection.
APC uses `inpatient_decision_to_admit` for the inpatient spell decision; ECDS
keeps `decision_to_admit` for the emergency department decision.
It returns only source/model/type totals and differences. Run it on deployment
or when an adapter changes, rather than adding another daily scan of OLIDS.

Use [clinical mapping coverage](../analyses/navigation/clinical_mapping_coverage.sql)
for recognised and historical SNOMED targets. A missing ICD-10/OPCS-4 reverse
map is deliberate, not a failed match. The prepared OLIDS snapshot is the
reconciliation boundary; do not expand its observations again. Count equality
alone cannot prove correct keys or replacements, so retain the fingerprint and
lifecycle evidence. For adapter changes, compare missing/extra keys with an
aggregate query that returns counts only.

[The e-RS validation guide](ers-analyst-validation.md) and
[appointment-date profile](../analyses/navigation/ers_appointment_date_recovery_profile.sql)
explain consolidated slots and recorded action selection.
`dq_mhsds_provider_submission` and `dq_csds_provider_submission` retain reporting
periods, provider recency and submitted section counts. The CSDS diagnostic also
has section/volume-change flags. Do not compare their contact totals directly
with event-month totals.

The expected-emission and field-applicability rules have not been extended for:

- ECDS coded observations, scored assessments, attendance-level clinical items
  and Mental Health Act legal status.
- MHSDS/CSDS onward referrals and ECDS referrals to services.
- MHSDS legal-status periods, community treatment orders and recalls, leave of
  absence, home leave, absence without leave, restrictive interventions and
  discharge-readiness milestones.
- CSDS diagnoses, childhood immunisations, social circumstances, newborn
  screening, breastfeeding and growth records, and MHSDS social circumstances.

Generic source/type counts and grouped MHSDS/CSDS clinical totals can include
these records, but do not independently validate each added source. An
actual-only emission group has no expected rule; its difference alone does not
establish over-emission. A zero applicability count for these types does not
establish that the field is absent from the source. Extend the rules before
using these diagnostics to validate a metric that depends on those types.
IAPT emissions are outside the expected rules and the shared outputs.

## Worked SQL

These examples use dbt `ref()` names. Compile them in a dbt analysis, or replace
references with their documented reporting/staging relations in an approved
Snowflake session.

These are person-level patterns for a human-controlled Snowflake session approved for patient-level data. Every identifier literal below is synthetic. No query outputs or query logs belong in the public repository. Aggregate monitoring/reconciliation belongs in `analyses/navigation/*.sql`; do not add these person-level examples as scheduled dbt models.

The event selection patterns cover only ECDS arrivals and SUS OP explicitly
attended appointments. They do not define universal completed-care eligibility.
National attendance codes 5/6 mean attended; missing codes are not attended
evidence, even where a cost model includes them. IAPT is not currently unioned.
Booking/slot timestamps alone never prove attendance.

OLIDS slots carry mapped national `attendance_status_code` alongside the original
`status_code`; bookings have no status. A mapped OLIDS DNA can include unanswered
calls or incomplete written work and does not prove literal non-attendance.
See [the OLIDS attendance definitions](longitudinal-activity.md).

Interpret `priority_code` within `source_dataset`. e-RS uses its own priority
codes; CSDS referral milestones and SUS OP slots use NHS DD Priority Type.
Codes are not mapped across lists. Read `priority_name` for the source or
reference label. MHSDS clinical response priority uses a separate list.

### First attended event after an index

Subject and grain: one result per selected index event for a synthetic person. The window is calendar-day based: strictly later day, up to `window_days` inclusive. Same-day candidates are intentionally separate. The selected ID resolves display ties only; `has_earliest_day_tie` makes competing first events visible.

```sql
with params as (
    select 'SYNTHETIC_PERSON_001'::varchar as person_key,
        'SYNTHETIC_INDEX_EVENT_001'::varchar as index_id,
        30::integer as window_days,
        '2026-01-31'::date as supported_observation_end,
        false::boolean as is_observation_window_complete
), index_event as (
    select e.*, p.window_days, p.supported_observation_end,
        p.is_observation_window_complete
    from {{ ref('fct_person_healthcare_event') }} e
    join params p on e.sk_patient_id = p.person_key and e.event_id = p.index_id
    where e.event_time_precision in ('date', 'timestamp')
), eligible as (
    select e.*
    from {{ ref('fct_person_healthcare_event') }} e
    join params p on e.sk_patient_id = p.person_key
    where e.event_time_precision in ('date', 'timestamp')
      and ((e.source_dataset = 'ECDS' and e.event_type = 'emergency_care_arrival')
        or (e.source_dataset = 'SUS_OP' and e.event_type = 'appointment_slot'
            and e.attendance_status_code in ('5', '6')))
), candidates as (
    select i.event_id as index_event_id, t.event_id as target_event_id,
        t.event_date as target_date, t.source_dataset, t.care_setting,
        count(*) over (partition by i.event_id, t.event_date) as same_day_targets
    from index_event i join eligible t on t.sk_patient_id = i.sk_patient_id
    where t.event_date > i.event_date
      and t.event_date <= dateadd(day, i.window_days, i.event_date)
      and t.event_id <> i.event_id
      and not (t.source_dataset = i.source_dataset
          and t.source_record_type = i.source_record_type
          and t.source_record_id = i.source_record_id)
), first_target as (
    select * from candidates
    qualify row_number() over (
        partition by index_event_id order by target_date, target_event_id
    ) = 1
)
select i.event_id as index_event_id, f.target_event_id, f.target_date,
    f.source_dataset, f.care_setting, f.same_day_targets > 1 as has_earliest_day_tie,
    case
        when f.target_event_id is not null then 'observed_time_only_candidate'
        when not i.is_observation_window_complete
          or dateadd(day, i.window_days, i.event_date) > i.supported_observation_end
            then 'unassessable_observation_window'
        else 'no_observed_qualifying_later_day_event'
    end as observation_result
from index_event i left join first_target f on i.event_id = f.index_event_id;
```

The synthetic `false` and observation-end date are demonstration inputs. Replace them only with the protocol's provider/source coverage assessment. The result can miss same-day care by design; report the same-day sensitivity below. Never rename the no-event result to DNA.

### Re-presentation within a parameterised window

Subject and grain: one candidate later ECDS attendance per index attendance. This example uses arrival-to-arrival, not discharge-to-arrival. Change that anchor only in the metric definition. Different milestones of the same attendance are not re-presentations.

```sql
with params as (
    select 'SYNTHETIC_PERSON_001'::varchar as person_key,
        'SYNTHETIC_INDEX_EVENT_001'::varchar as index_id, 7::integer as window_days
), index_event as (
    select e.*, p.window_days
    from {{ ref('fct_person_healthcare_event') }} e join params p
      on e.sk_patient_id = p.person_key and e.event_id = p.index_id
    where e.source_dataset = 'ECDS' and e.event_type = 'emergency_care_arrival'
      and e.event_time_precision in ('date', 'timestamp')
)
select i.event_id as index_event_id, t.event_id as representation_event_id,
    datediff(day, i.event_date, t.event_date) as calendar_days,
    'time_only_candidate' as association_evidence
from index_event i join {{ ref('fct_person_healthcare_event') }} t
    on t.sk_patient_id = i.sk_patient_id
where t.source_dataset = 'ECDS' and t.event_type = 'emergency_care_arrival'
    and t.event_time_precision in ('date', 'timestamp')
    and t.source_record_id <> i.source_record_id
    and t.event_date > i.event_date
    and t.event_date <= dateadd(day, i.window_days, i.event_date);
```

Require a complete follow-up window before estimating a no-representation rate. For an elapsed-hours metric, use genuine timestamps and an agreed source clock/offset policy, not midnight anchors. Same-day re-presentations need separate assessment.

### Provider and setting transitions, with inferred Sankey preparation

Subject and grain: one adjacent day-state pair per synthetic person. This is a sequence of observed evidence, not a recorded transfer. It groups same-day states before ordering, so IDs cannot manufacture within-day direction. Repeated milestones are avoided by selecting one arrival or attended OP slot per source record.

```sql
with eligible as (
    select e.sk_patient_id, e.event_date, e.source_dataset, e.care_setting,
        e.provider_code_authority, e.provider_organisation_code
    from {{ ref('fct_person_healthcare_event') }} e
    where e.sk_patient_id = 'SYNTHETIC_PERSON_001'
      and e.event_date between '2026-01-01' and '2026-01-31'
      and e.event_time_precision in ('date', 'timestamp')
      and ((e.source_dataset = 'ECDS' and e.event_type = 'emergency_care_arrival')
        or (e.source_dataset = 'SUS_OP' and e.event_type = 'appointment_slot'
            and e.attendance_status_code in ('5', '6')))
), states as (
    select *,
        case
            when care_setting is null or provider_organisation_code is null
              or provider_code_authority is null then 'context_unknown'
            else care_setting || '|' ||
                iff(provider_code_authority = 'ODS', 'ODS',
                    source_dataset || ':' || provider_code_authority)
                || '|' || provider_organisation_code
        end as state_key
    from eligible
), days as (
    select sk_patient_id, event_date,
        iff(count(distinct state_key) = 1, min(state_key), 'ambiguous_same_day') as day_state,
        count(*) as observed_records
    from states group by sk_patient_id, event_date
), edges as (
    select *, lead(event_date) over (partition by sk_patient_id order by event_date) as next_date,
        lead(day_state) over (partition by sk_patient_id order by event_date) as next_state
    from days
)
select event_date, next_date, day_state, next_state,
    case
        when day_state in ('context_unknown', 'ambiguous_same_day')
          or next_state in ('context_unknown', 'ambiguous_same_day') then null
        else day_state <> next_state
    end as is_observed_provider_or_setting_change,
    'inferred_temporal_sequence' as association_evidence
from edges where next_date is not null;
```

For a Sankey, replace the final select with grouping by `day_state`, `next_state` and `count(*)`. That measures adjacent day-state pairs, not distinct people or transfers. Keep unknown/ambiguous states visible. In a governed cohort, publish only approved, disclosure-checked aggregates. For setting-only edges, construct the day state from setting alone. For distinct-person weights or collapse of repeated states, declare the changed grain and rule in the metric template. Do not equate local provider codes across datasets. Overlapping episodes and the same occurrence recorded by different feeds remain separate source evidence unless the protocol defines deduplication.

### Same-day ambiguity and date-only records

Subject and grain: one same-day candidate pair for the selected synthetic index. Month/year/unknown precision is excluded. Timestamp ordering below is clock evidence only; common offsets are not guaranteed across sources.

```sql
with index_event as (
    select * from {{ ref('fct_person_healthcare_event') }}
    where sk_patient_id = 'SYNTHETIC_PERSON_001'
      and event_id = 'SYNTHETIC_INDEX_EVENT_001'
      and event_time_precision in ('date', 'timestamp')
)
select i.event_id as index_event_id, t.event_id as candidate_event_id,
    i.event_time_precision as index_precision, t.event_time_precision as target_precision,
    case
        when i.event_time_precision <> 'timestamp' or t.event_time_precision <> 'timestamp'
            then 'within_day_order_unknown'
        when i.source_dataset <> t.source_dataset then 'cross_source_clock_policy_required'
        when t.event_at > i.event_at then 'later_supplied_clock_time'
        when t.event_at = i.event_at then 'equal_supplied_clock_time'
        else 'earlier_supplied_clock_time'
    end as temporal_evidence
from index_event i join {{ ref('fct_person_healthcare_event') }} t
    on t.sk_patient_id = i.sk_patient_id and t.event_date = i.event_date
where t.event_id <> i.event_id
  and t.event_time_precision in ('date', 'timestamp');
```

Apply the same eligible-target predicate as the main metric before calculating sensitivity. A date-only anchor covers the whole calendar day. If a clock-based window ends during that day, inclusion is uncertain. Report both include/exclude bounds; do not compare its midnight anchor as an observed time. Partial clinical dates require separate interval handling, not ordinary day-window joins.

### Recorded bookings versus time-only candidates

Subject and grain: one eligible OP candidate per selected e-RS index event. The relationship model's patient agreement determines whether a UBRN link is usable. A missing-key/mismatched link is not promoted. The query labels the available evidence; it does not prove that the index caused care.

```sql
with index_event as (
    select * from {{ ref('fct_person_healthcare_event') }}
    where sk_patient_id = 'SYNTHETIC_PERSON_001'
      and event_id = 'SYNTHETIC_ERS_INDEX_001'
      and source_dataset = 'ERS'
      and parent_model_name = 'fct_ers_referral'
      and event_time_precision in ('date', 'timestamp')
)
select i.event_id as index_event_id, t.event_id as target_event_id,
    case when exists (
        select 1 from {{ ref('rel_ers_referral_acute_record') }} r
        where r.ubrn_id::varchar = i.parent_record_id
          and r.acute_source = 'sus_outpatient'
          and r.acute_source_record_id = t.source_record_id
          and r.patient_key_agreement = 'agree'
          and r.ers_sk_patient_id = i.sk_patient_id
          and r.acute_sk_patient_id = t.sk_patient_id
    ) then 'recorded_booking_reference_link' else 'time_only_candidate' end as association_evidence
from index_event i join {{ ref('fct_person_healthcare_event') }} t
    on t.sk_patient_id = i.sk_patient_id
where t.source_dataset = 'SUS_OP' and t.event_type = 'appointment_slot'
    and t.attendance_status_code in ('5', '6')
    and t.event_time_precision in ('date', 'timestamp')
    and t.event_date > i.event_date
    and t.event_date <= dateadd(day, 30, i.event_date);
```

The temporal filter defines this metric's candidates. Recorded links outside it are still valid links; report their counts separately when validating window choice. `EXISTS` avoids multiplying a candidate when a relationship table has several matching rows. A shared parent is evidence of recorded context, not automatically a referral from one event to another.

### Clinical source and recorded-parent drill-down

Subject and grain: one selected ECDS coded finding. The shared fields are `source_code_name`, `mapped_code_name`, `result_value`, `result_value_numeric`, `result_unit_code` and `relationship_type`.

```sql
select c.clinical_record_id, c.source_record_id, c.source_model_name,
    c.source_code, c.source_code_name, c.mapped_code, c.mapped_code_name,
    c.clinical_record_date, c.clinical_time_precision,
    c.result_value, c.result_value_numeric, c.result_unit_code,
    c.parent_record_id, c.parent_record_type, c.parent_model_name,
    c.relationship_type, c.parent_start_date, c.parent_end_date,
    d.recorded_at as detailed_recorded_at,
    p.visit_occurrence_id as recorded_attendance_id
from {{ ref('fct_person_clinical_record') }} c
left join {{ ref('stg_sus_ecds_clinical_coded_findings') }} d
    on c.source_record_id = d.source_record_id::varchar
    and c.parent_record_id = d.primarykey_id::varchar
left join {{ ref('obt_encounter_uec') }} p
    on c.parent_model_name = 'obt_encounter_uec'
    and c.parent_record_id = p.visit_occurrence_id::varchar
    and c.sk_patient_id = p.sk_patient_id
where c.sk_patient_id = 'SYNTHETIC_PERSON_001'
    and c.clinical_record_id = 'SYNTHETIC_CLINICAL_RECORD_001'
    and c.source_dataset = 'ECDS' and c.source_record_type = 'clinical_finding'
    and c.source_model_name = 'stg_sus_ecds_clinical_coded_findings';
```

Join a static, named detail relation using its documented native key. Do not execute `source_model_name` as SQL. APC episode parents use `source_record_id`; acute attendances use `visit_occurrence_id`; e-RS referrals use `ubrn_id`; OLIDS encounter parents use `id`; MHSDS/CSDS referral parents use `source_record_id`. This example selects coded findings, not legal-status records. OLIDS clinical detail also needs `source_record_type` alongside `source_record_id`. Retain undated clinical rows, with parent dates as context rather than invented clinical dates.

For an OLIDS appointment's directly recorded clinical items, use the existing bridge rather than a same-day join:

```sql
select e.event_id, c.clinical_record_id, c.source_record_type, c.source_record_id,
    c.source_model_name, c.code, c.code_name, c.result_value,
    c.parent_record_id, c.parent_model_name,
    'recorded_appointment_encounter_link' as association_evidence
from {{ ref('fct_person_healthcare_event') }} e
join {{ ref('dim_person_secondary_use_allowed') }} allowed
    on e.source_person_id = allowed.person_id::varchar
join {{ ref('fct_gp_appointment_clinical_record') }} link
    on e.source_record_id = link.appointment_id::varchar
    and e.sk_patient_id = link.sk_patient_id
join {{ ref('fct_person_clinical_record') }} c
    on c.clinical_record_id = link.clinical_record_id::varchar
    and c.sk_patient_id = e.sk_patient_id and c.source_dataset = 'OLIDS'
where e.sk_patient_id = 'SYNTHETIC_PERSON_001'
    and e.event_id = 'SYNTHETIC_GP_SLOT_001'
    and e.source_dataset = 'OLIDS' and e.source_record_type = 'appointment'
    and e.event_type = 'appointment_slot';
```

Unlinked clinical items remain available for a separately labelled person/time search. Never label such a search a recorded appointment link.

### No observed event versus DNA

Retain three separate outcomes: observed qualifying event; no observed qualifying event within an assessed window; unassessable window. DNA is evidence on a supplied attendance record. For national attendance-coded contacts/OP, codes 3/7 are recorded non-attendance; use source definitions for other feeds. e-RS `appointment_dna_recorded`, `appointment_dna_updated` and `appointment_dna_resolved` are distinct actions, not three new DNAs or a universal final outcome. OLIDS slots carry mapped national `attendance_status_code` alongside original `status_code`; bookings have no status. A mapped OLIDS DNA can mean an unanswered call or incomplete written work, so it does not prove literal non-attendance. OLIDS status is extract-current and lacks reconstructable cancellation history. See [the OLIDS attendance definitions](longitudinal-activity.md).

An absent record with complete feed evidence still means no event observed in the included feeds, not proof that care did not occur anywhere. Never derive an attendance flag from a left join failing to match.

### Population and equity joins

Use approved person-level models and state the purpose/required characteristics in the data-access request. Apply `dim_person_secondary_use_allowed` only to the OLIDS branch, through `source_person_id = person_id`; do not restrict all WNL sources to the NCL OLIDS population. Do not use `dim_person_active_patients` to discard historical/deceased people unless the metric explicitly requires a current active population.

The existing `dim_person_demographics` is current person-grain; `dim_person_demographics_historical` has several periods per person. Use the historical model with its documented effective dates for an index-date attribute and confirm one matching period. Resolve through approved `dim_person_pseudo` where needed. A person-grain join is not necessarily unique at `sk_patient_id` grain: check that mapping before joining. Missing OLIDS demographics for other WNL people stay unknown; do not silently drop them. Current age/practice/geography must not be described as index-date attributes. Keep equity outputs aggregated and disclosure-checked.

## Metric-definition template

Copy this table into each metric protocol and record:

| Item | Required definition |
|---|---|
| Purpose and authority | Named analyst/business owner, evaluation question and data-access purpose. |
| Population | Included sources/providers/geography, historical/current population rule, person linkage, opt-outs and equity attributes. |
| Index | Model, event types/statuses, source key, date/time basis, repeated-index rule and exclusions. |
| Eligible targets | Explicit source/type/status predicates, authoritative code lists, planned versus delivered care, duplicate milestones/occurrences. |
| Window | Start/end inclusivity, day/hour basis, arrival versus discharge anchor, follow-up and censoring. |
| Link evidence | Recorded source parent, recorded booking/referral bridge or time-only candidate. Temporal proximity is association evidence, not causation. |
| Competing matches | All candidates versus first; same-day/clock ties; stable ID for display only; uncertainty bounds. |
| Coverage | Expected provider-period grid, delivery/load/build dates, lag choices, gaps/shortfalls, external care and conditions for an unassessable window. |
| Missing evidence | No observed event versus DNA; missing person/provider/time; partial clinical dates and unlinked parents. |
| Sensitivity | Longer lags/windows, same-day inclusion/exclusion, source restrictions, linkage classes and alternative deduplication. |
| Reproducibility | SQL/version, extract dates, denominator and output grain; aggregate validation and disclosure policy. |

## Reproducing the lag evidence

[The activity lag analysis](../analyses/navigation/activity_delivery_lag.sql)
reproduces source-level retained receipt/extraction ages from the prepared
activity adapters. The cohort dates are fixed so a later run uses the same activity window;
replacements can still change the result. Its minimum group size is 100 records.

The following metadata-only Snowflake query reproduces MHSDS/CSDS header-history
summaries for October 2024 to July 2026. It reads raw header views that retain
submission history and returns source-level rounded distributions, with each
provider/reporting period weighted equally. It does not return provider codes
or source identifiers. IAPT is excluded from these examples and the shared
coverage diagnostics.

```sql
with headers as (
    select 'MHSDS' as dataset, org_id_provider::varchar as provider,
        uniq_submission_id::varchar as submission,
        reporting_period_end_date::date as period_end,
        effective_from::timestamp_ntz as received_at,
        dmic_date_added::timestamp_ntz as loaded_at
    from STAGING.DBT_RAW.RAW_MHSDS_MHS000HEADER
    union all
    select 'CSDS', organisation_code_code_of_provider::varchar,
        unique_submission_id::varchar, reporting_period_end_date::date,
        effective_from::timestamp_ntz, upload_date_time::timestamp_ntz
    from STAGING.DBT_RAW.RAW_CSDS_CYP000HEADER
), submissions as (
    select dataset, provider, submission, period_end,
        min(received_at) as received_at, min(loaded_at) as loaded_at
    from headers
    where period_end between '2024-10-01' and '2026-07-31'
        and provider is not null
    group by 1, 2, 3, 4
), periods as (
    select dataset, provider, period_end, count(*) as versions,
        min(received_at) as first_at, max(received_at) as latest_at,
        max(loaded_at) as latest_loaded_at
    from submissions group by 1, 2, 3
)
select dataset,
    round(percentile_cont(.5) within group
        (order by datediff(day, period_end, first_at))) as first_receipt_p50_days,
    round(percentile_cont(.9) within group
        (order by datediff(day, period_end, first_at))) as first_receipt_p90_days,
    round(percentile_cont(.95) within group
        (order by datediff(day, period_end, latest_at))) as latest_receipt_p95_days,
    round(percentile_cont(.95) within group
        (order by datediff(day, first_at, latest_at))) as replacement_span_p95_days,
    round(percentile_cont(.95) within group
        (order by datediff(day, period_end, latest_loaded_at))) as loaded_or_upload_p95_days,
    round(100.0 * count_if(versions > 1) / count(*)) as multiple_submission_pct
from periods group by dataset having count(*) >= 100;
```
