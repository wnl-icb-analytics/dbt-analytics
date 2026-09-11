-- Compile for the deployment target, then execute after building the moved references.
-- Verify every copy before removing any old table. No compatibility aliases are created.
{% set moved = ['csds_assessment_scale', 'csds_assessment_response',
    'mhsds_assessment_scale', 'mhsds_assessment_response', 'csds_observation_unit_alias'] %}
execute immediate $$
declare
    mismatch exception (-20001, 'Assessment reference contents differ; old tables retained.');
    old_relation varchar;
    new_relation varchar;
    object_count number;
    old_count number;
    new_count number;
    old_hash number;
    new_hash number;
begin
    {% for name in moved %}
    {% set relation = ref(name) %}
    old_relation := '{{ relation.database }}.TERMINOLOGY.{{ relation.identifier }}';
    new_relation := '{{ relation }}';
    select count(*) into :object_count
    from {{ relation.database }}.information_schema.tables
    where table_schema = 'TERMINOLOGY' and table_name = '{{ relation.identifier | upper }}';
    if (object_count > 0) then
        select count(*), hash_agg(*) into :old_count, :old_hash from identifier(:old_relation);
        select count(*), hash_agg(*) into :new_count, :new_hash from identifier(:new_relation);
        if (old_count <> new_count or old_hash <> new_hash) then
            raise mismatch;
        end if;
    end if;
    {% endfor %}

    {% for name in moved %}
    {% set relation = ref(name) %}
    drop table if exists {{ relation.database }}.TERMINOLOGY.{{ relation.identifier }};
    {% endfor %}
end;
$$;
