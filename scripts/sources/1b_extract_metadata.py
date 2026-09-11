
import os
import pathlib
import sys

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


if __name__ == "__main__":
    print(f"SNOWFLAKE_ROLE: {os.getenv('SNOWFLAKE_ROLE')}")

    sql_file_path = pathlib.Path(__file__).parent / "metadata_query.sql"
    output_file = pathlib.Path(__file__).parent / "table_metadata.csv"

    # Read SQL before opening a Snowflake session so a missing file fails fast
    # without triggering browser SSO.
    try:
        sql_query = sql_file_path.read_text()
    except FileNotFoundError:
        print(f"SQL file not found: {sql_file_path}", file=sys.stderr)
        print("Please run script 1a first to generate the metadata query:", file=sys.stderr)
        print("  python scripts/sources/1a_generate_metadata_query.py", file=sys.stderr)
        sys.exit(1)

    def _remove_stale_output():
        if output_file.exists():
            output_file.unlink()
            print(f"Removed stale {output_file}", file=sys.stderr)

    # Auth precedence: key pair (CI), then PAT, then browser SSO.
    if os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'):
        passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', '')
        auth = {
            'private_key_file': os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'),
            'private_key_file_pwd': passphrase.encode() or None,
        }
    elif os.getenv('SNOWFLAKE_PAT') is not None:
        auth = {'password': os.getenv('SNOWFLAKE_PAT')}
    else:
        auth = {'authenticator': 'externalbrowser'}

    conn = None

    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            database="MODELLING",
            schema="DBT_DEV",
            **auth,
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        df = cur.fetch_pandas_all()
        print(f"Shape: {df.shape}")
        print(df.head())
        df.to_csv(output_file, index=False)
        print(f"\nMetadata extracted to {output_file}")
        print(f"\nNext step: Generate sources.yml file:")
        print(f"  python scripts/sources/2_generate_sources.py")
    except Exception as e:
        print(f"Error during metadata extraction: {str(e)}", file=sys.stderr)
        _remove_stale_output()
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()
