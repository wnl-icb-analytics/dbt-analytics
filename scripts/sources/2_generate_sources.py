import pandas as pd
import yaml
import os
import sys

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
INPUT_FILE = os.path.join(CURRENT_DIR, 'table_metadata.csv')
OUTPUT_DIR = os.path.join(PROJECT_DIR, 'models', 'sources')
MAPPINGS_FILE = os.path.join(CURRENT_DIR, 'source_mappings.yml')

def _iter_manual_source_files():
    """Yield paths of manual source YAML files.

    Manual sources are defined either in sources.yml or any manual_*.yml file.
    """
    if not os.path.exists(OUTPUT_DIR):
        return
    for filename in sorted(os.listdir(OUTPUT_DIR)):
        if filename == 'sources.yml' or (filename.startswith('manual_') and filename.endswith('.yml')):
            yield os.path.join(OUTPUT_DIR, filename)


def load_manual_sources():
    """Load manually defined sources and tables from sources.yml and manual_*.yml.

    Returns:
        tuple: (set of source names, dict mapping source_name -> set of table names)
    """
    manual_source_names = set()
    manual_source_tables = {}  # source_name -> set of table names

    for file_path in _iter_manual_source_files():
        with open(file_path, 'r') as f:
            sources_data = yaml.safe_load(f)
        if not sources_data or 'sources' not in sources_data or not sources_data['sources']:
            continue
        for source in sources_data['sources']:
            source_name = source['name']
            manual_source_names.add(source_name)
            if 'tables' in source:
                table_names = {table['name'] for table in source['tables']}
                manual_source_tables[source_name] = table_names

    return manual_source_names, manual_source_tables


def check_manual_source_drift(df, mappings):
    """Warn if manually declared sources drift from extracted metadata.

    Only checks manual sources whose (database, schema) is present in
    source_mappings.yml - otherwise the metadata query would not have
    pulled them and any "missing" finding is a false positive.

    Flags:
      - declared tables missing from live metadata (renamed / dropped)
      - declared columns missing from the live table
      - live columns not declared in the manual YAML (new columns)
    """
    # Build set of (db, schema) pairs that were actually queried.
    queried_db_schema = set()
    for source in mappings.values():
        db = source.get('database', '').upper()
        schema = source.get('schema', '').upper()
        if db and schema:
            queried_db_schema.add((db, schema))

    # Build lookup: (db, schema, table) -> set of column names from metadata.
    metadata_columns = {}
    for row in df.itertuples(index=False):
        key = (str(row.DATABASE_NAME), str(row.SCHEMA_NAME), str(row.TABLE_NAME))
        metadata_columns.setdefault(key, set()).add(str(row.COLUMN_NAME))

    warnings = []
    checked_tables = 0
    for file_path in _iter_manual_source_files():
        with open(file_path, 'r') as f:
            sources_data = yaml.safe_load(f) or {}
        for source in sources_data.get('sources', []) or []:
            source_db = normalize_identifier(source.get('database', ''))
            source_schema = normalize_identifier(source.get('schema', ''))
            if not source_db or not source_schema:
                continue
            # Skip if this db/schema was not queried - no metadata to compare against.
            if (source_db.upper(), source_schema.upper()) not in queried_db_schema:
                continue
            for table in source.get('tables', []) or []:
                table_name = normalize_identifier(table.get('identifier') or table.get('name'))
                if not table_name:
                    continue
                checked_tables += 1
                key = (source_db, source_schema, table_name)
                live_cols = metadata_columns.get(key)
                if live_cols is None:
                    warnings.append(
                        f"[drift] {os.path.basename(file_path)}: {source['name']}.{table_name} "
                        f"not found in live metadata ({source_db}.{source_schema}.{table_name})"
                    )
                    continue
                declared = {str(c.get('name', '')).strip() for c in (table.get('columns') or []) if c.get('name')}
                missing_in_source = declared - live_cols
                new_in_source = live_cols - declared
                if missing_in_source:
                    warnings.append(
                        f"[drift] {os.path.basename(file_path)}: {source['name']}.{table_name} "
                        f"declares columns not in source: {sorted(missing_in_source)}"
                    )
                if new_in_source:
                    warnings.append(
                        f"[drift] {os.path.basename(file_path)}: {source['name']}.{table_name} "
                        f"source has undeclared columns: {sorted(new_in_source)}"
                    )

    if warnings:
        print(f"\nManual source drift check - {len(warnings)} warning(s) across {checked_tables} table(s):")
        for w in warnings:
            print(f"  {w}")
        print("Consider updating the manual YAML to match Snowflake, or confirm the drift is intentional.\n")
    else:
        print(f"Manual source drift check: clean ({checked_tables} table(s) checked).")

def load_source_mappings():
    """Load source mappings from YAML file"""
    if not os.path.exists(MAPPINGS_FILE):
        print(f"Error: Mappings file not found at {MAPPINGS_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(MAPPINGS_FILE, 'r') as f:
        mappings_data = yaml.safe_load(f)

    # Convert to lookup dictionaries
    db_schema_to_source = {}

    for source in mappings_data['sources']:
        db = source['database']  # Preserve original case
        schema = source.get('schema', '')  # Preserve original case

        if schema:
            # Specific schema mapping - use uppercase for lookup key only
            key = (db.upper(), schema.upper())
        else:
            # Database-level mapping (all schemas)
            key = (db.upper(), '*')

        db_schema_to_source[key] = source

    return db_schema_to_source

def normalize_identifier(value):
    """Normalize quoted/unquoted identifiers for metadata matching."""
    if value is None:
        return ""
    value = str(value).strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


def should_generate_table(table_name, mapping):
    """Return whether a source table should be generated."""
    if 'tables' in mapping and table_name not in mapping['tables']:
        return False
    return table_name not in mapping.get('exclude_tables', [])

def sync_manual_source_types(df):
    """Sync data_type values in all manual source YAMLs from extracted metadata.

    Iterates every file returned by _iter_manual_source_files() so sources.yml
    and any manual_*.yml file are kept in sync with the live Snowflake schema.
    """
    # Build metadata lookup keyed by exact DB/SCHEMA/TABLE/COLUMN identifiers.
    metadata_lookup = {
        (
            str(row.DATABASE_NAME),
            str(row.SCHEMA_NAME),
            str(row.TABLE_NAME),
            str(row.COLUMN_NAME),
        ): str(row.DATA_TYPE)
        for row in df.itertuples(index=False)
    }

    total_updates = 0
    total_checked = 0

    for file_path in _iter_manual_source_files():
        with open(file_path, 'r') as f:
            sources_data = yaml.safe_load(f) or {}

        sources = sources_data.get('sources', [])
        if not sources:
            continue

        file_updates = 0
        for source in sources:
            source_db = normalize_identifier(source.get('database', ''))
            source_schema = normalize_identifier(source.get('schema', ''))
            if not source_db or not source_schema:
                continue

            for table in source.get('tables', []):
                table_name = normalize_identifier(table.get('identifier') or table.get('name'))
                if not table_name:
                    continue

                for column in table.get('columns', []):
                    column_name = str(column.get('name', '')).strip()
                    if not column_name:
                        continue

                    total_checked += 1
                    key = (source_db, source_schema, table_name, column_name)
                    metadata_type = metadata_lookup.get(key)
                    if metadata_type and column.get('data_type') != metadata_type:
                        column['data_type'] = metadata_type
                        file_updates += 1

        if file_updates > 0:
            with open(file_path, 'w') as f:
                yaml.dump(sources_data, f, sort_keys=False, default_flow_style=False)
            total_updates += file_updates

    return total_updates, total_checked

def find_source_mapping(database, schema, mappings):
    """Find the appropriate source mapping for a database/schema combination"""
    database = database.upper()
    schema = schema.upper()

    # First try exact match
    if (database, schema) in mappings:
        return mappings[(database, schema)]

    # Then try database-level match
    if (database, '*') in mappings:
        return mappings[(database, '*')]

    return None

def get_source_filename(source_name, database, schema):
    """Generate a filename for the source based on database and schema"""
    # Normalize database name
    db_normalized = database.replace('DATA_LAKE__', 'data_lake_').replace('Dictionary', 'dictionary').lower()

    # Normalize schema name
    schema_normalized = schema.lower().replace('_', '_').replace('-', '_')

    # Create filename: auto_database_schema.yml (prefix with 'auto_' for auto-generated sources)
    if db_normalized.startswith('data_lake_ncl'):
        # For DATA_LAKE__NCL, use more specific names
        filename = f"auto_{db_normalized}_{schema_normalized}.yml"
    else:
        # For regular databases
        filename = f"auto_{db_normalized}_{schema_normalized}.yml"

    return filename

def main():
    # Load mappings
    mappings = load_source_mappings()
    print(f"Loaded {len(mappings)} source mappings from {MAPPINGS_FILE}")
    
    # Load manually defined sources and tables from sources.yml (these override auto-generation)
    manual_source_names, manual_source_tables = load_manual_sources()
    if manual_source_names:
        print(f"Found {len(manual_source_names)} manually defined sources in sources.yml (will skip auto-generation): {', '.join(sorted(manual_source_names))}")

    # Try comma first, fallback to tab if error
    try:
        df = pd.read_csv(INPUT_FILE, sep=',')
        df.columns = df.columns.str.strip()
        print("Columns found in file (comma):", df.columns.tolist())
        if len(df.columns) == 1:
            raise ValueError("Only one column found, trying tab delimiter.")
    except Exception:
        df = pd.read_csv(INPUT_FILE, sep='\t')
        df.columns = df.columns.str.strip()
        print("Columns found in file (tab):", df.columns.tolist())

    # Keep manually curated sources in sync with real metadata types.
    # This avoids stale type drift warnings while preserving manual source ownership.
    updated_columns, checked_columns = sync_manual_source_types(df)
    if updated_columns > 0:
        print(f"Updated {updated_columns} manual source column type(s) in sources.yml (checked {checked_columns}).")

    # Warn if any manual YAML has drifted from the live Snowflake schema.
    check_manual_source_drift(df, mappings)

    # Group by database and schema to create sources
    sources_by_file = {}  # Track sources by output filename
    total_tables = 0

    for (database, schema), group in df.groupby(['DATABASE_NAME', 'SCHEMA_NAME']):
        # Find mapping for this database/schema
        mapping = find_source_mapping(database, schema, mappings)

        if not mapping:
            print(f"Warning: No mapping found for {database}.{schema}, skipping...")
            continue

        # Check if source is enabled
        if not mapping.get('enabled', True):  # Default to True if not specified
            print(f"Info: Source '{mapping['source_name']}' is disabled, skipping...")
            continue

        source_name = mapping['source_name']

        # Skip sources explicitly flagged as manual in source_mappings.yml -
        # their columns come from a manual YAML (sources.yml or manual_*.yml).
        if mapping.get('manual', False):
            continue

        # Skip sources that are manually defined in a manual YAML file
        # (sources.yml or manual_*.yml) - they override auto-generation.
        if source_name in manual_source_names:
            print(f"Info: Source '{source_name}' is manually defined, skipping auto-generation...")
            continue
        
        # Note: Source names stay as-is (no 'auto_' prefix in source name)
        # Only the filename gets the 'auto_' prefix to distinguish from manual sources

        # Determine output filename for this source (will have 'auto_' prefix)
        filename = get_source_filename(source_name, database, schema)

        # Initialize file entry if needed
        if filename not in sources_by_file:
            sources_by_file[filename] = []

        tables = []

        # Get all tables for this source from metadata
        for table_name, table_group in group.groupby('TABLE_NAME'):
            if not should_generate_table(table_name, mapping):
                continue

            # Auto-generate from metadata
            sorted_columns = table_group.sort_values('ORDINAL_POSITION')

            table = {
                'name': table_name,
                'identifier': f'"{table_name}"',  # Quote identifier to preserve case
                'columns': [{'name': col, 'data_type': dtype}
                           for col, dtype in zip(sorted_columns['COLUMN_NAME'],
                                               sorted_columns['DATA_TYPE'])]
            }
            tables.append(table)

        source = {
            'name': source_name,  # Keep original source name (no 'auto_' prefix)
            'database': f'"{mapping["database"]}"',  # Quote for case sensitivity
            'schema': f'"{mapping.get("schema", schema)}"',  # Quote schema and use mapped case
            'description': mapping.get('description', ''),
            'tables': tables
        }

        sources_by_file[filename].append(source)
        total_tables += len(tables)
        print(f"Created source '{source_name}' with {len(tables)} tables -> {filename}")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Clean up auto-generated files: remove sources that are now manually defined
    # Find all auto-generated files and remove manual sources from them
    if os.path.exists(OUTPUT_DIR):
        for filename in os.listdir(OUTPUT_DIR):
            if filename.startswith('auto_') and filename.endswith('.yml'):
                file_path = os.path.join(OUTPUT_DIR, filename)
                try:
                    with open(file_path, 'r') as f:
                        file_sources = yaml.safe_load(f)
                        if file_sources and 'sources' in file_sources:
                            # Filter out manual sources
                            original_sources = file_sources['sources']
                            filtered_sources = [
                                source for source in original_sources
                                if source.get('name') not in manual_source_names
                            ]
                            
                            # If any sources were removed, update or delete the file
                            if len(filtered_sources) < len(original_sources):
                                removed_sources = [
                                    source.get('name') for source in original_sources
                                    if source.get('name') in manual_source_names
                                ]
                                
                                if filtered_sources:
                                    # Some sources remain, update the file
                                    file_sources['sources'] = filtered_sources
                                    with open(file_path, 'w') as f:
                                        yaml.dump(file_sources, f, sort_keys=False, default_flow_style=False)
                                    print(f"  Updated {filename} (removed manual sources: {', '.join(removed_sources)})")
                                else:
                                    # All sources were manual, delete the file
                                    os.remove(file_path)
                                    print(f"  Deleted {filename} (all sources [{', '.join(removed_sources)}] are now manually defined in sources.yml)")
                except Exception as e:
                    print(f"  Warning: Could not check {filename}: {e}")

    # Write each source file (excluding sources.yml which contains manual-only sources)
    for filename, sources in sources_by_file.items():
        # Skip sources.yml - this file contains manually defined sources that are NOT in source_mappings.yml
        if filename == 'sources.yml':
            print(f"  Skipping {filename} (manual sources file)")
            continue
            
        sources_yml = {
            'version': 2,
            'sources': sources
        }

        output_path = os.path.join(OUTPUT_DIR, filename)
        with open(output_path, 'w') as f:
            yaml.dump(sources_yml, f, sort_keys=False, default_flow_style=False)

        print(f"  Written: {filename}")
    
    # Note: Manual sources.yml should only contain sources NOT in source_mappings.yml
    # Sources in source_mappings.yml will be auto-generated with all tables (preferring manual table definitions)

    print(f"\nGenerated {len(sources_by_file)} source files in {OUTPUT_DIR}")
    print(f"Total: {sum(len(sources) for sources in sources_by_file.values())} sources with {total_tables} tables")
    print(f"\nNext step: Generate raw layer models:")
    print(f"  python scripts/sources/3_generate_raw_models.py")

if __name__ == '__main__':
    main()
