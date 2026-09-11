# Longitudinal activity

`fct_person_healthcare_event` contains one source milestone per row.
`fct_person_clinical_record` contains one source clinical item per row.
Both retain unlinked and undated records. Neither assigns a journey or merges
similar occurrences from different feeds. The business owner is Eddie Davison.

The two facts and `dq_person_activity_coverage` are in `REPORTING.CROSS_SYSTEM`
(`DEV__REPORTING.CROSS_SYSTEM` for development). These are shared cross-system
models; Intelligent Navigation is one consumer.

## Storage and loading

The shared outputs are views over prepared source branches. MHSDS, CSDS, acute
and e-RS branches are incremental tables in dbt-analytics. OLIDS uses the
existing materialised `healthcare_event` and `clinical_record` tables in
dbt-OLIDS. The shared views do not create another copy of OLIDS clinical data.

Daily increments select source receipt, import or extraction timestamps at or
after the last stored delivery for each source record type. Equal timestamps
are replayed. Missing delivery timestamps are replayed on every run. Clinical
event dates never decide which records load: a newly delivered historical event
can therefore enter the next increment.

For a changed source record, dbt-analytics replaces all its milestones. Removing
a discharge date removes its former discharge milestone. A subsequent narrow
key comparison removes records no longer in the current source population.
OLIDS merges stable event or clinical IDs and also removes withdrawn keys.

The monthly full refresh reconciles deliveries behind the watermark, changed
reference labels and other upstream changes without a new delivery timestamp.
This includes OLIDS statement-only context changes on existing medication orders.
An increment is not a complete change-data-capture history. A failed build must
be rerun before treating its output as current.

The existing dbt-analytics monthly schedule includes these models through
their staging dependencies. dbt-OLIDS refreshes its two stable outputs on the
first day of each month, after its normal upstream build. Pseudonymisation
indexes are excluded from the full refresh so their identifiers are preserved.

Runtime hooks use `WH_WNL_OLIDS_L` for initial builds and full refreshes when
`target.role` is `DBT_ADMIN`, then restore the profile warehouse. Other roles
keep their configured warehouse. Normal increments keep the profile warehouse.
The hooks evaluate at execution, avoiding cached full-refresh flags in model
configuration. No project-wide warehouse configuration changes are needed.

Initial deployment of the upstream text key and clustering needs a full refresh
of the two stable outputs or a rewrite of their existing prepared snapshots.
An incremental merge alone does not change the stored key type. Development
builds in dbt-analytics use the established `dev` target and shared `DEV__` layers.

## Time and clustering

Materialised branches cluster by the cross-system person key `sk_patient_id`, then
`coalesce(event_at, event_date::timestamp_ntz)`. Clinical branches use the
equivalent clinical fields. OLIDS stores the lookup key as text so analytics
does not convert a numeric key for every lookup. Where no source clinical clock exists,
the second clustering field is the clinical date.

This fallback affects storage only. Published timestamps remain null when a
clock time is not established. Clustering helps pruning and retrieval, but SQL
still requires `ORDER BY` to guarantee output order. A stable ID can break display
ties; it does not establish which same-day event happened first.

Both shared views include an `ORDER BY` across their complete source unions
for direct timeline lookups. They sort by `sk_patient_id`, the relevant date,
time and record ID. Date-only records follow timed records on the same day; undated records come
last. These are presentation rules, not additional clinical precision. Sorting
happens at query time. Joins, aggregations and other outer queries can change
the result order; use a top-level `ORDER BY` when order must be guaranteed.

Interpret dates with their precision:

| Precision | Meaning |
|---|---|
| `timestamp` | A recorded clock time accompanies the date. |
| `date` | The day is known; within-day order is unknown. |
| `month`, `year` | A partial OLIDS clinical date. Do not interpret its stored day as exact. |
| `unknown` | No established precision, including missing dates and uncertain MHSDS stored timestamps. |

MHSDS diagnosis and referral-assessment timestamps can lack their original
submitted precision and offset. The shared clinical output retains the source
date, marks precision unknown and leaves the timestamp null. The source fact
retains the stored timestamp, separately supplied date and inconsistency flags.
See the [MHSDS source validation](mhsds-clinical-record-plan.md).

SUS procedures use their supplied procedure dates. Undated diagnoses remain
undated; admission or appointment dates are not substituted. MHSDS and CSDS
clinical dates can use the previously validated recorded activity/contact links.
Their source facts retain the derivation and consistency evidence.

## Included milestones

| Source | Milestones |
|---|---|
| OLIDS | Recorded current booking, scheduled appointment slot and terminology-selected patient referral. No reconstructed booking or cancellation history. |
| MHSDS | Referral received, rejected and discharged; care contact; hospital admission and discharge; ward stay start and end. |
| CSDS | Referral received and discharged; care contact. |
| SUS APC | Spell admission and discharge. |
| SUS OP | Scheduled appointment, including non-attendance and cancellations recorded on the appointment. |
| ECDS | Arrival, initial assessment, seen for treatment, decision to admit and departure. |
| e-RS | Selected recorded actions and scheduled appointment slots. `ers_healthcare_event_type` defines the action selection. |

Primary milestones retain source records with missing dates. Optional milestones
require a recorded date. A booking or scheduled slot never proves attendance.
Status and outcome fields describe their source meanings and can be extract-current.
e-RS reminders, printing, letter edits and unconfirmed cancellation requests are
not independent care milestones. e-RS action labels distinguish recorded and
updated DNA actions. See [e-RS validation](ers-analyst-validation.md).

Clinical items include OLIDS expanded observations and statement-enriched
medication orders; MHSDS and CSDS prepared clinical records; SUS diagnoses and
procedures; and ECDS diagnoses, treatments, investigations, comorbidities and
coded findings. There are no standalone medication statements. Acute code
positions are preserved, including repeated codes in distinct supplied positions.

## Keys and source detail

`sk_patient_id` is the established pseudonymised NHS-number key. A practice
patient ID must not replace it. `source_person_id` is scoped by `source_dataset`.
Missing cross-system linkage does not remove the source record.

Global IDs encode a dataset namespace. OLIDS UUIDs already encode their source
and record identity, so both outputs retain those upstream UUIDs without a text
prefix. Changes to dates preserve milestone IDs
when the underlying source key stays the same. e-RS slot identity itself includes
the supplied slot timestamp, so a new slot is a different source record.

Use `source_model_name`, `source_record_type` and `source_record_id` for detail.
Existing source key names remain available:

| Detail model | Key represented by `source_record_id` |
|---|---|
| MHSDS and CSDS facts | `source_record_id` |
| `fct_gp_appointment` | `appointment_id` |
| `fct_gp_referral_request` | `source_record_id` |
| `fct_gp_clinical_record` | `source_record_id` together with `source_record_type` |
| `fct_ers_referral_action` | `action_id` |
| `fct_ers_appointment` | `appointment_id` |
| `obt_encounter_apc`, `int_sus_op_appointment`, `obt_encounter_uec` | `visit_occurrence_id` |
| SUS diagnosis staging | `diagnosis_id` |
| SUS procedure staging | `procedure_id` |
| ECDS clinical staging | `diagnosis_id` for diagnoses; `source_record_id` for other items |

Recorded parents follow `parent_model_name`. MHSDS/CSDS parents use
`source_record_id`; e-RS referrals use `ubrn_id`; acute encounters use
`visit_occurrence_id`; APC episodes use `source_record_id`; OLIDS encounters
use `id`. A parent link is source evidence, not a completed journey. Additional
links and their consistency flags remain in the detailed models.

Code labels come from existing references and source descriptions. Historical
codes retain the latest supported definition. No query calls a terminology
server. Null labels mean no supported label was found; they do not invalidate
the source code. Provider authorities distinguish ODS from local identifiers.

## Population and observation windows

OLIDS currently contains the filtered NCL spine. Other feeds retain their
available WNL populations and have different submission windows. For secondary
use, apply `dim_person_secondary_use_allowed` to the OLIDS branch through
`source_person_id = person_id`. Do not inner-join the entire cross-source output
to an OLIDS-only population. Apply each source's relevant governance.

`dq_person_activity_coverage` reports counts, missing patient keys, absent or
partial dates, date extremes and latest delivery by output and source record
type. A latest receipt does not prove every provider has supplied a complete
period. MHSDS and CSDS reporting periods and source submission coverage remain
available for choosing observation windows. e-RS future slots are planned
activity; they do not extend the observation window for delivered care.

Define the index event, source populations, completed-care criteria and period
before calculating a time-to-care measure. Treat undated or partially dated
records separately. No observed attendance is not evidence of non-attendance.
