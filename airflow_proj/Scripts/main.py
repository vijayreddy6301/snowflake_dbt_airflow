import snowflake.connector
import os
import sys
import env  # Import Snowflake credentials

# ✅ Function to list CSV files in 'src/' directory
def list_local_files(directory):
    """Lists all CSV files in the given directory."""
    try:
        if os.path.exists(directory):
            files = [f for f in os.listdir(directory) if f.endswith(".csv")]
            return files if files else []
        else:
            print(f"❌ Directory '{directory}' not found.")
            return []
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

# ✅ Get directory path and list files
src_dir = "/home/hp/airflow_venv/airflow_proj/src"

files = list_local_files(src_dir)

if not files:
    print("❌ No CSV files found in src/. Exiting...")
    sys.exit(1)

# ✅ Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=env.user,
        password=env.password,
        account=env.account,
        warehouse=env.warehouse,
        database=env.database,
        schema=env.schema
    )
    print("✅ Connected to Snowflake!")
    cur = conn.cursor()

    table_name = "CUSTOMER_TABLE"

    # ✅ Fetch table column names from Snowflake (Keep them uppercase for mapping)
    cur.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';")
    table_columns = {col[0]: col[0] for col in cur.fetchall()}  # Create mapping of column names

    # ✅ Get row count before loading
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    before_count = cur.fetchone()[0]
    print(f"📊 Rows in '{table_name}' before loading: {before_count}")

    # ✅ Create Stage & File Format
    stage_name = "MY_STAGE"
    file_format = "MY_CSV_FORMAT"

    cur.execute(f"CREATE OR REPLACE STAGE {stage_name};")
    cur.execute(f"""
        CREATE OR REPLACE FILE FORMAT {file_format}
        TYPE = 'CSV'
        SKIP_HEADER = 1  -- ✅ Ensure first row is ignored as data
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
        TRIM_SPACE = TRUE;
    """)
    print(f"✅ Stage '{stage_name}' and file format '{file_format}' created.")

    # ✅ Process Each CSV File
    for file_name in files:
        file_to_upload = os.path.join(src_dir, file_name)
        print(f"📂 Uploading file: {file_to_upload}")

        # Upload file to Snowflake stage
        cur.execute(f"PUT file://{file_to_upload} @{stage_name} AUTO_COMPRESS=TRUE;")
        print(f"✅ File '{file_name}' uploaded to stage '{stage_name}'.")

        staged_file = f"@{stage_name}/{file_name}"

        # ✅ Manually Define Expected Column Names (Uppercase with _)
        expected_columns = [
            "INDEX", "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "COMPANY",
            "CITY", "COUNTRY", "PHONE_1", "PHONE_2", "EMAIL",
            "SUBSCRIPTION_DATE", "WEBSITE"
        ]

        # ✅ Transform CSV column names to match table names (Replace spaces with _)
        src_columns = [col.upper().replace(" ", "_") for col in expected_columns]

        print(f"📋 Using manually mapped columns: {src_columns}")

        # ✅ Create explicit column mapping (Ensure columns exist in Snowflake table)
        mapped_columns = [f'"{table_columns[col]}"' for col in src_columns if col in table_columns]
        
        if not mapped_columns:
            print(f"⚠️ No matching columns found in '{file_name}'. Skipping...")
            continue

        column_list = ", ".join(mapped_columns)

        # ✅ Load data with explicit column mapping
        copy_query = f"""
        COPY INTO {table_name} ({column_list})
        FROM {staged_file}
        FILE_FORMAT = (FORMAT_NAME = '{file_format}')
        ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_query)
        print(f"✅ Data from '{file_name}' loaded into table '{table_name}'.")

    # ✅ Get row count after loading data
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    after_count = cur.fetchone()[0]
    print(f"📊 Rows in '{table_name}' after loading: {after_count}")

except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)

finally:
    # ✅ Close connection
    cur.close()
    conn.close()
    print("🔚 Process completed!")
