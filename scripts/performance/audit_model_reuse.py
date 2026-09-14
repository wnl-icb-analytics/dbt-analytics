"""Combine dbt lineage with optional Snowflake table metadata. No data queries."""

import argparse
import json
import re
from pathlib import Path


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def audit(manifest, tables, min_rows):
    nodes = manifest['nodes']
    storage = {
        (t['TABLE_CATALOG'].upper(), t['TABLE_SCHEMA'].upper(), t['TABLE_NAME'].upper()): t
        for t in tables
    }
    results = []
    for node_id, node in nodes.items():
        if node['resource_type'] != 'model':
            continue
        consumers = sorted(
            nodes[c]['name'] for c in manifest['child_map'].get(node_id, [])
            if c in nodes and nodes[c]['resource_type'] == 'model'
        )
        if len(consumers) < 2:
            continue
        # Development targets retain production schemas and add this database prefix.
        relation = (node['database'].removeprefix('DEV__').upper(),
                    node['schema'].upper(), node['alias'].upper())
        table = storage.get(relation)
        materialized = node['config']['materialized']
        sql = node.get('raw_code', '')
        operations = sorted(set(re.findall(
            r'\b(?:qualify|distinct|join|group\s+by|row_number)\b', sql, re.IGNORECASE
        )))
        is_staging_view = materialized == 'view' and node['name'].startswith('stg_')
        is_large_table = table and (table.get('ROW_COUNT') or 0) >= min_rows
        if not (is_staging_view or is_large_table):
            continue
        results.append({
            'model': node['name'], 'path': node['original_file_path'],
            'materialized': materialized, 'production_relation': '.'.join(relation),
            'direct_model_consumers': consumers, 'consumer_count': len(consumers),
            'configured_cluster_by': node['config'].get('cluster_by'),
            'physical_clustering_key': table.get('CLUSTERING_KEY') if table else None,
            'rows': table.get('ROW_COUNT') if table else None,
            'bytes': table.get('BYTES') if table else None,
            'view_sql_operations': operations if is_staging_view else [],
            'review_reason': (
                'Repeated view transformation candidate; inspect compiled SQL and profiles'
                if is_staging_view and operations else
                'View projection; check underlying physical storage'
                if is_staging_view else
                'Large shared table without an explicit physical clustering key'
                if not table.get('CLUSTERING_KEY') else
                'Large shared table with an existing physical clustering key'
            ),
        })
    return sorted(results, key=lambda r: (r['bytes'] or 0, r['consumer_count']), reverse=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', default='target/manifest.json')
    parser.add_argument('--tables', help='JSON export from model_storage.sql')
    parser.add_argument('--min-rows', type=int, default=10_000_000)
    args = parser.parse_args()
    tables = read_json(args.tables) if args.tables else []
    print(json.dumps(audit(read_json(args.manifest), tables, args.min_rows), indent=2))


if __name__ == '__main__':
    main()
