# Expanded OLIDS observations

`DATA_LAKE.OLIDS.OBSERVATION` includes native observations, allergy records and
referral requests. Each row represents one source entity record, not a unique
clinical occurrence. Test requests, procedure requests and medication records
are outside this expansion.

The dbt-olids conformed and stable models own the population and ID derivation.
This project reads that shared output once. Staging continues to exclude records
marked deleted and records without a person identifier.

## Identifiers and clinical context

`id` remains the identifier for joining the expanded observation feed. Native
observation IDs are unchanged. Added entities receive deterministic UUIDs that
include their source entity, so equal original IDs can remain separate records.
Do not deduplicate across entities by person, date, concept or source ID.

`source_entity` identifies `observation`, `allergy_intolerance` or
`referral_request`. `source_record_id` is the original UUID within that entity.
It is distinct from `lds_source_record_id`, which the feed already supplied.
Use the entity and original ID together when tracing a source record.

`allergy_medication_name` retains the medication name on an allergy record. It
does not represent a prescription or medication order. Added entities do not
acquire observation result values, units or problem flags. Null flags mean that
the source has no applicable value; they do not establish a negative result.

Raw and staging expose all three new fields. `get_observations`,
`get_ltc_lcs_observations` and `get_ltc_lcs_observations_latest` carry them into
their results. Existing code-set matching and date rules remain in place.
Concept-specific models that project only their existing fields can still join
their observation ID back to staging for provenance.

The C-LTCS observation feed includes the new fields and retains its existing
one-year window and limit of 100 recent records per patient. That limit applies
across all three entities. A newer allergy or referral can therefore displace an
older observation. A referral request is not evidence that its requested care
was delivered.

## Valproate referral events

`int_valproate_araf_referral_events` previously combined observations and
referral requests itself. It now reads only the expanded observation feed,
avoiding a second copy of every qualifying referral request.

`araf_referral_id` preserves existing event IDs: original referral-request IDs
for referral requests and unchanged observation IDs for native observations.
New allergy rows use their conformed observation ID. The ID alone need not
distinguish original entities. The model's grain uses person and
`araf_referral_observation_id`, which joins directly to staging. Source entity
and original ID remain available. Existing person-level arrays of legacy event
IDs keep their meaning and are not an event count.

## Deployment and historical rebuilds

Use a coordinated release window. Scheduled analytics must not run against the
expanded upstream feed before this companion is deployed: the old valproate
model would read each referral twice. Publish the upstream conformed and stable
changes, including all three new columns, then complete DEV validation and
deploy the companion before the next analytics run. If that sequence cannot
fit between scheduled runs, use the existing job scheduling controls to hold
analytics runs until both changes are ready. This change does not alter those
controls or publish upstream data.

There is no runtime fallback to the old schema. Source YAML describes the
agreed upstream columns so the companion can compile before publication.

After publication, use the existing `dev` target and shared `DEV__` layers:

1. Build the changed observation raw and staging models, valproate referral
   events, their person-level aggregate and C-LTCS observations. Run their tests.
2. Compile the full project and inspect the downstream selection with
   `dbt ls --select stg_olids_observation+`. Macros affect more models than their
   direct callers suggest. Profile changes by source entity and code-set group,
   including register membership, latest-event dates, the C-LTCS cap and the
   global data-refresh date.
3. Rebuild affected table consumers. COVID and flu incremental models retain
   closed campaigns during routine runs. Use the existing
   `covid_flu_rebuild_closed: true` variable or a targeted full refresh when
   restating those campaigns against the expanded feed.
4. `person_month_analysis_base` normally replaces only the latest completed
   month. Its established full-refresh process is needed to restate affected
   older person-months.

Do not apply a blanket downstream full refresh. In particular,
`cltcs_population_monthly_capture` is an append-only record of the population
captured each month. Recreating it would erase historical captures rather than
recompute historical membership.

The scoped raw-generation run uses the repository's
`scripts/sources/3_generate_raw_models.py` with only the OBSERVATION source in a
scratch project. It regenerates the existing raw model without changing other
raw models. Warehouse validation remains dependent on upstream publication;
this companion does not create alternate databases, schemas or source views.

## Companion validation

The full project compiled on 9 September 2026 using the standard `dev` target:
2,100 models, 4,812 tests, 33 snapshots, 39 seeds and 47 analyses, plus hooks,
after merging the latest main branch.
The existing unused-configuration warning was the only warning.

Local synthetic validation executed the rendered valproate model with native,
allergy and referral records sharing an original UUID. It retained three
conformed events, preserved the old native and referral IDs and excluded a
non-referral code. The new source-identity regression passed and detected a
deliberately changed referral ID. This checks the transformation without
patient data; it does not replace the warehouse build after upstream publication.


The [pre-publication impact queries](../scripts/snowflake/profile_olids_observation_companion.sql)
also ran against existing production outputs and conformed source entities.
The 519 existing Valproate events for 284 people all match the 519 qualifying
referral requests by person and original event ID. Dates, concept codes and
labels have zero differences; the retained legacy ID sets are unchanged.
No allergy records match the programme's REFERRAL code category. This comparison
uses the existing materialised Valproate output and the added entities, without
rescanning native observation history.

The global refresh date remains 7 September 2026. A bounded check of native,
allergy and referral dates after that date found no later date supported by at
least 150 practices. These aggregate checks ran before upstream publication;
a warehouse build and its tests remain outstanding.
