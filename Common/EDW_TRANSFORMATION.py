import subprocess 
import socket
import sys
import numpy as np
import pandas as pd
import oracledb
import re
import pyarrow as pa
from datetime import datetime
from PASSWORD_DECRYPTOR import decrypt_password
import duckdb
import string
import os
import smtplib
from email.message import EmailMessage

start_up = sys.modules.get("START_UP")

def get_local_server():
    return "PROD" if socket.gethostname() == "svdwh001" else "UAT"

def format_timedelta(td):
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if days > 0:
        return f"{days} days, {hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def x_cmd(command: str):
    """
    x-command similar to SAS

    Parameters:
        command : command to execute in linux
            - eg. x_cmd("ls -lrt")
    """
    cmd_parts = command.strip().split()
    if "psftp" in cmd_parts and cmd_parts[cmd_parts.index("psftp")] == "psftp" and os.getenv("SERVER_ENV") == "UAT":
        print("No SFTP command in UAT")
        return 0  # Return success code
    try:
        result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
        print(result.stdout) 
        return 0  # Success
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return e.returncode  # Return the actual error code

def _get_oracle_output_handler():
    def output_handler(cursor, metadata):
        if metadata.type_code is oracledb.DB_TYPE_TIMESTAMP:
            return cursor.var(oracledb.DB_TYPE_VARCHAR, arraysize=cursor.arraysize)
    return output_handler

def _get_oracle_connection_params(user):
    """Get Oracle connection parameters based on user"""
    host = None
    port = None
    service_name = None
    
    if user in {"pbbdw", "srcdw", "ora_crma", "pivbdw"}:
        host = "svdwh003" if os.getenv("SERVER_ENV") == "PROD" else "svdwh006"
        if user in {"pbbdw", "srcdw"}:
            port = 2281
            service_name = "pbbdw"
        elif user == "pivbdw":
            port = 2282
            service_name = "pivbdw"
        elif user == "ora_crma":
            port = 2283
            service_name = "crma"
    elif user in {"detica_edw", "amltmpbb"}:
        user = "detica_edw"
        host = "svdetdbs001" if os.getenv("SERVER_ENV") == "PROD" else "svdetdbs003"
        port = 2281
        service_name = "amltmpbb"
    
    return user, host, port, service_name

def _create_oracle_connection(user, pwd, host, port, service_name):
    """Create Oracle database connection"""
    if user == 'ora_crma':
        user = 'crma'
    dsn = f'{user}/{pwd}@{host}:{port}/{service_name}'
    try:
        oracledb.init_oracle_client(lib_dir=None)
        oracledb.defaults.arraysize = 100000
        conn = oracledb.connect(dsn)
        conn.outputtypehandler = _get_oracle_output_handler()
        return conn
    except Exception as e:
        print(f"Error connecting to Oracle: {e}")
        return None

def libname(user:str = 'pbbdw', server:str = 'oracle', db:str = None): 
    """
    Libname function and return connection

    Parameters:
        user    : user to connect
        server  : oracle / mysql
        db      : only applicable for mysql

    Returns:
        Connection
    """
    user = user.lower()
    server = server.lower()
    pwd = None
    
    try:
        pwd = decrypt_password(user)
    except Exception:
        print(f"Invalid user {user}")

    if server == 'oracle':
        user, host, port, service_name = _get_oracle_connection_params(user)
        
        if not host:
            print(f"Invalid user {user} in Oracle DB")
            return None

        if not pwd:
            try:
                pwd = decrypt_password(user)
            except Exception:
                print(f"Invalid user {user}")
                return None
        
        return _create_oracle_connection(user, pwd, host, port, service_name)

    print("Error in libname. Please check credentials")
    return None

def _build_csv_mode_table(con, file, delimiter, schema, skiprows):
    """Helper to create data table in CSV mode."""
    columns_sql = ", ".join(f"'{c['name']}':'VARCHAR'" for c in schema)
    con.execute(f"""
        CREATE TEMP TABLE data AS
        SELECT *
        FROM read_csv(
            '{file}',
            delim='{delimiter}',
            quote='"',
            escape='"',
            auto_detect=false,
            skip={skiprows},
            columns={{ {columns_sql} }}
        );
    """)

def _build_fixed_width_columns(schema):
    """Helper to build fixed-width column selection."""
    select_cols = []
    for c in schema:
        start = c["start"] - 1
        length = c["end"] - start if "end" in c else c["length"]
        select_cols.append(f"SUBSTR(line, {start+1}, {length}) AS \"{c['name']}\"")
    return select_cols

def _build_delimited_columns(schema, delimiter):
    """Helper to build delimited column selection."""
    return [f"SPLIT_PART(line, '{delimiter}', {i+1}) AS \"{c['name']}\"" for i, c in enumerate(schema)]

def _build_strict_mode_table(con, file, delimiter, schema, skiprows):
    """Helper to create data table in strict mode."""
    print("Reading file...")
    con.execute("CREATE OR REPLACE TEMP TABLE raw(line VARCHAR);")
    con.execute(f"""
        COPY raw FROM '{file}'
        (
            DELIMITER '\n',
            HEADER FALSE,
            SKIP {skiprows},
            QUOTE '',
            ESCAPE ''
        );
    """)
    
    select_cols = _build_fixed_width_columns(schema) if delimiter is None else _build_delimited_columns(schema, delimiter)
    
    print("Converting columns...")
    con.execute(f"""
        CREATE TEMP TABLE data AS
        SELECT {", ".join(select_cols)}
        FROM raw;
    """)

def _build_type_cast(column):
    """Helper to build type cast for a single column."""
    name = column["name"]
    t = column["type"]
    
    if t in (int, "int"):
        return f"TRY_CAST(REPLACE(TRIM({name}), ',', '') AS BIGINT) AS \"{name}\""
    elif t in (float, "float"):
        return f"TRY_CAST(REPLACE(TRIM({name}), ',', '') AS DOUBLE) AS \"{name}\""
    elif t in ("date",):
        fmt = column.get("format")
        if fmt:
            return f"TRY_STRPTIME(TRIM({name}), '{fmt}') AS \"{name}\""
        else:
            return f"TRY_CAST(TRIM({name}) AS TIMESTAMP) AS \"{name}\""
    else:
        return f"COALESCE(TRIM({name}), '') AS \"{name}\""

def _apply_type_conversions(con, schema):
    """Helper to apply type conversions to data table."""
    casts = [_build_type_cast(c) for c in schema]
    con.execute(f"""
        CREATE TEMP TABLE final AS
        SELECT {", ".join(casts)}
        FROM data;
    """)

def file_reader(
    file: str,
    schema: list,
    delimiter: str = None,
    skiprows: int = 0,
    csv_mode: bool = False
):
    """
    DuckDB-based file reader.

    Modes:
        csv_mode=False (default):
            - Strict parsing
            - Ignores quotes
            - Survives malformed rows, footers

        csv_mode=True:
            - Proper CSV parsing
            - Supports quoted fields
            - Requires clean CSV
    """
    if not os.path.isfile(file):
        raise FileNotFoundError(file)

    con = duckdb.connect()

    if csv_mode:
        if delimiter is None:
            raise ValueError("delimiter must be provided when csv_mode=True")
        _build_csv_mode_table(con, file, delimiter, schema, skiprows)
    else:
        try:
            _build_strict_mode_table(con, file, delimiter, schema, skiprows)
        except Exception as e:
            print(f"Reading failed with error: {e}")
            print("Attempting dos2unix method...")
            fallback_file = file + "_unix"
            x_cmd(f"dos2unix -n {file} {fallback_file}")
            _build_strict_mode_table(con, fallback_file, delimiter, schema, skiprows)
            x_cmd(f"rm -f {fallback_file}")

    _apply_type_conversions(con, schema)
    
    df = con.execute("SELECT * FROM final").df()
    con.close()
    return df

def _smart_round_length(max_len):
    """Round varchar length to standard sizes"""
    if max_len <= 10:
        return 10
    elif max_len <= 25:
        return 25
    elif max_len <= 50:
        return 50
    elif max_len <= 100:
        return 100
    elif max_len <= 255:
        return 255
    elif max_len <= 1000:
        return 1000
    else:
        return min(4000, int(np.ceil(max_len / 100.0) * 100))

def _get_oracle_varchar_length(series):
    """Calculate optimal VARCHAR2 length for a pandas Series"""
    str_vals = series.dropna().astype(str)
    str_vals = str_vals[str_vals.str.strip() != '']
    str_lengths = str_vals.map(len)

    if len(str_lengths) == 0:
        return 1

    unique_lengths = str_lengths.unique()
    if len(unique_lengths) == 1:
        return unique_lengths[0]
    else:
        max_len = str_lengths.max()
        return _smart_round_length(max_len)

def _get_oracle_type_from_pandas(series):
    """Determine Oracle column type from pandas Series"""
    if pd.api.types.is_datetime64_any_dtype(series) or "date" in str(series.dtype) or "timestamp" in str(series.dtype):
        return 'DATE'
    elif pd.api.types.is_numeric_dtype(series):
        return 'NUMBER'
    elif pd.api.types.is_string_dtype(series) or series.apply(lambda x: isinstance(x, str)).any():
        return f'VARCHAR2({_get_oracle_varchar_length(series)})'
    else:
        return 'VARCHAR2(255)'

def _normalize_target_table(target_table):
    """Add default schema if not present"""
    return f"pbbdw.{target_table}" if "." not in target_table else target_table

def _generate_from_dataframe(df, target_table):
    """Generate CREATE TABLE SQL from DataFrame"""
    column_defs = []
    for col in df.columns:
        col_type = _get_oracle_type_from_pandas(df[col])
        column_defs.append(f'"{col.upper()}" {col_type}')

    target_table = _normalize_target_table(target_table)
    return f'CREATE TABLE {target_table.upper()} (\n  ' + ',\n  '.join(column_defs) + '\n)'

def _parse_table_path(input_source):
    """Extract schema and table name from input source"""
    if "." in input_source:
        path, table = input_source.split('.', 1)
        return path, table
    return None, input_source

def _get_oracle_columns(cursor, table_name):
    """Fetch column definitions from Oracle"""
    query = """
        SELECT column_name, data_type, data_length
        FROM all_tab_columns
        WHERE table_name = :input_source
        ORDER BY column_id
    """
    cursor.execute(query, [table_name.upper()])
    return cursor.fetchall()

def _oracle_type_to_sql(name, dtype, length):
    """Convert Oracle data type to CREATE TABLE column definition"""
    if dtype in ('VARCHAR2', 'CHAR'):
        col_type = f'VARCHAR2({length})'
    elif dtype == 'NUMBER':
        col_type = 'NUMBER'
    elif dtype in ('DATE', 'TIMESTAMP'):
        col_type = 'DATE'
    else:
        col_type = f'{dtype}'
    
    return f'"{name}" {col_type}'

def _generate_from_oracle_table(input_source, target_table, conn):
    """Generate CREATE TABLE SQL from existing Oracle table"""
    path, table_name = _parse_table_path(input_source)
    to_close = False
    
    if conn is None:
        to_close = True
        conn = libname(path) if path else libname()
    
    with conn.cursor() as cursor:
        columns = _get_oracle_columns(cursor, table_name)
        column_defs = [_oracle_type_to_sql(name, dtype, length) for name, dtype, length in columns]
    
    if to_close:
        conn.close()

    target_table = _normalize_target_table(target_table)
    return f'CREATE TABLE {target_table.upper()} (\n  ' + ',\n  '.join(column_defs) + '\n)'

def create_oracle_table_sql(input_source, target_table:str, conn=None):
    """
    Generate CREATE TABLE oracle sql string

    Parameters:
        input_source (string or Dataframe):
            - string -> Oracle table name (with or without DB name)
            - Dataframe -> Load from pandas Dataframe
        target_table: Oracle table name (with or without DB name)
        conn: Oracle connection for Oracle input table (leave blank to use input_source credentials)
        
    Assumptions:
        - If no DB name, default to PBBDW

    Example:
        - df = oracle_extract("select * from srcdw.bo_company")
        - df = oracle_extract("select * from ebank_user_daily_activity")

    Returns:
        SQL string
    """
    if isinstance(input_source, pd.DataFrame):
        return _generate_from_dataframe(input_source, target_table)
    elif isinstance(input_source, str):
        return _generate_from_oracle_table(input_source, target_table, conn)
    else:
        raise ValueError("Input must be a DataFrame or table name with a valid Oracle connection.")

def oracle_extract(sql_string:str, conn=None, parameters=None):
    """
    Generate df from Oracle sql

    Parameters:
        sql_string (string):
            sql string
        conn: Oracle connection (leave blank to use pbbdw credentials, else retrieve from sql string)
        parameters: (Optional) Bind variable
        
    Assumptions:
        - If no DB name, default to PBBDW

    Returns:
        pandas.DataFrame
    """

    time_start_exct = datetime.now()

    to_close = False
    if conn is None:
        to_close = True
        match = re.search(r'\bFROM\s+(\w+)\.', sql_string, re.IGNORECASE)
        if not match:
            conn = libname()
        else:
            schema = match.group(1)
            conn = libname(schema)
        
    odf = conn.fetch_df_all(statement=sql_string, parameters=parameters)
    df = pa.table(odf).to_pandas(types_mapper=pd.ArrowDtype)    

    time_end_exct = datetime.now()
    time_diff_exct = time_end_exct - time_start_exct
    print("")
    print(f"Row count: {df.shape[0]}, Column count: {df.shape[1]}", flush=True)
    print("Time elapsed :", format_timedelta(time_diff_exct),flush=True)

    if to_close:
        conn.close()

    return df

DEFAULT_DB = "PBBDW"

# ---------- helpers (TOP LEVEL) ---------- #

def _load_input(input_source):
    if isinstance(input_source, str):
        query = f"SELECT * FROM {input_source}"
        return oracle_extract(query)
    return input_source


def _connect(target_table):
    if "." not in target_table:
        target_table = f"{DEFAULT_DB}.{target_table}"
        target_path = "pbbdw"
    else:
        target_path = target_table.split(".", 1)[0]

    conn = libname(target_path)
    return target_table, conn, conn.cursor()


def _add_batch_columns(df, batch):
    if not batch:
        return

    if "VALID_DTTM" not in df.columns:
        df["VALID_DTTM"] = start_up.batch_dttm[batch]

    if "PROCESSED_DTTM" not in df.columns:
        df["PROCESSED_DTTM"] = datetime.now()


def _prepare_target(cursor, input_source, target_table, df, mode):
    table_name = target_table.split(".")[-1]

    cursor.execute(
        "SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(:1)",
        [table_name],
    )
    exists = cursor.fetchone()[0] > 0

    ddl = create_oracle_table_sql(input_source, target_table)

    if not exists:
        print(f"Creating table {target_table.upper()}...")
        cursor.execute(ddl)
        return

    if mode == "replace":
        print(f"Dropping and recreating table {target_table.upper()}...")
        cursor.execute(f"DROP TABLE {target_table} PURGE")
        cursor.execute(ddl)
        return

    cursor.execute(
        "SELECT column_name FROM user_tab_columns WHERE table_name = UPPER(:1)",
        [table_name],
    )
    target_cols = [r[0] for r in cursor.fetchall()]

    # Drop extra columns
    df.drop(columns=[c for c in df.columns if c not in target_cols], inplace=True)

    # Add missing columns
    for col in set(target_cols) - set(df.columns):
        df[col] = None


def _truncate(cursor, conn, target_table):
    print(f"Truncating table {target_table}...")
    cursor.execute(f"TRUNCATE TABLE {target_table}")
    conn.commit()


def _delete_existing_batch(cursor, conn, target_table, batch):
    if not batch:
        return

    batch_dt = start_up.batch_dttm[batch]
    check_query = f"SELECT COUNT(*) FROM {target_table} WHERE VALID_DTTM = :1"

    print(f"Executing check query: {check_query} with parameter: {batch_dt}")
    cursor.execute(check_query, [batch_dt])
    count = cursor.fetchone()[0]
    print(f"Record count with VALID_DTTM = {batch_dt}: {count}")

    if count == 0:
        return

    print(f"End Date (for delete): {batch_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    delete_query = f"DELETE FROM {target_table} WHERE VALID_DTTM = :1"
    cursor.execute(delete_query, [batch_dt])
    conn.commit()

    print(f"Deleted records with VALID_DTTM = {batch_dt} from {target_table}")


def _bulk_insert(cursor, conn, target_table, df, mode, batch_size=100000):
    print(f"\nData loading into {target_table} using mode: {mode}")

    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    col_str = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{i+1}" for i in range(len(cols)))
    sql = f"INSERT INTO {target_table} ({col_str}) VALUES ({placeholders})"

    data = list(df.itertuples(index=False, name=None))

    total_inserted = 0
    total_failed = 0
    rejected = []

    for i in range(0, len(data), batch_size):
        batch_data = data[i:i + batch_size]

        try:
            cursor.executemany(sql, batch_data, batcherrors=True)
            errors = cursor.getbatcherrors()

            inserted = len(batch_data) - len(errors)
            total_inserted += inserted
            total_failed += len(errors)

            for e in errors:
                rejected.append((i + e.offset, data[e.offset], e.message))

        except Exception as e:
            conn.rollback()
            print(f"Critical error on batch starting at index {i}: {e}")
            total_failed += len(batch_data)
            rejected.extend((i + j, r, str(e)) for j, r in enumerate(batch_data))

    conn.commit()
    return total_inserted, total_failed, rejected


def _report(target_table, df, result):
    inserted, failed, rejected = result

    print(f"\nTable {target_table}:")
    print(f"Total records:      {len(df)}")
    print(f"Inserted:           {inserted}")
    print(f"Rejected:           {failed}")

    if not rejected:
        return

    print("\nRejected rows:")
    for idx, _, msg in rejected:
        print(f"Row {idx + 1}: {msg}")

    df_err = pd.DataFrame(
        rejected, columns=["row_index", "row_data", "error_message"]
    )

    duckdb.register("df_err", df_err)
    fname = f"/sas/oracle_excp/{target_table}_{datetime.now():%Y.%m.%d_%H.%M.%S}.parquet"
    duckdb.sql(f"COPY df_err TO '{fname}' (FORMAT PARQUET)")
    print(f"Errors saved to {fname}")


def _cleanup(cursor, conn):
    cursor.close()
    conn.close()


# ---------- main API ---------- #

def oracle_bulk_loader(input_source, target_table, mode="append", batch="", dna=True):
    """
    Oracle BulkLoader.

    Parameters:
        input_source (string or Dataframe):
            - string -> Oracle table name (with or without DB name)
            - Dataframe -> Load from pandas Dataframe
        target_table: Oracle table name (with or without DB name)
        mode: "append", "truncate", or "replace"
        batch: batch date value for VALID_DTTM column, leave blank if not required
            - eg. "EBANK" / "CIS"
    """

    time_start_load = datetime.now()

    df = _load_input(input_source)
    target_table, conn, cursor = _connect(target_table)

    _add_batch_columns(df, batch)
    _prepare_target(cursor, input_source, target_table, df, mode)

    if mode == "truncate":
        _truncate(cursor, conn, target_table)

    if mode == "append" and dna:
        _delete_existing_batch(cursor, conn, target_table, batch)

    result = _bulk_insert(cursor, conn, target_table, df, mode)

    _cleanup(cursor, conn)
    _report(target_table, df, result)

    print("")
    print("Time elapsed :", format_timedelta(datetime.now() - time_start_load), flush=True)

def get_sftp_info(host_desc:str):
    """
    Get SFTP info from control table
    """

    df = pd.read_sas('/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat', format='sas7bdat', encoding='utf-8')
    
    df = df.loc[df["HOST_DESC"] == host_desc]
    df = df.reset_index()

    sftp_id = df['SFTP_ID'][0]
    sftp_pw = df['SFTP_PW'][0]
    host_ip = df['HOST_IP'][0]
    host_key = df['HOST_KEY'][0]

    return sftp_id, sftp_pw, host_ip, host_key

# ---------- helpers (TOP LEVEL) ---------- #

def _validate_sftp_params(filename, abort_if_empty):
    if not isinstance(abort_if_empty, bool):
        raise ValueError("abort_if_empty must be a boolean (True or False)")
    if not os.path.exists(filename):
        sys.exit(f"Error: File '{filename}' does not exist")
    if os.path.getsize(filename) == 0:
        if abort_if_empty:
            sys.exit(f"Aborting: File '{filename}' is empty")
        else:
            return False
    return True


def _read_sftp_file(filename):
    allowed = set(string.printable)
    with open(filename, "r", errors="ignore") as f:
        lines = f.readlines()
    filtered_lines = ["".join(ch for ch in line if ch in allowed).strip() for line in lines]
    return filtered_lines


def _check_sftp_errors(filtered_lines):
    error_keywords = [
        "ORA-",
        "error while",
        "bad message",
        "permission denied",
        "no such file or directory"
    ]

    has_valid_content = False
    for line in filtered_lines:
        if line:
            has_valid_content = True
            break

    error_found = False

    for line in filtered_lines:
        if any(keyword in line for keyword in error_keywords):
            error_found = True
            print(f"Error line: {line}")

    return has_valid_content, error_found


# ---------- main function ---------- #

def sftp_log_error_check(filename: str, abort_if_empty: bool = True):
    """
    Check file for errors.

    Parameters:
        filename: Filename
        abort_if_empty: Check for empty file (True / False)
    """

    if not _validate_sftp_params(filename, abort_if_empty):
        return  # empty file handled by abort_if_empty=False

    filtered_lines = _read_sftp_file(filename)
    has_valid_content, error_found = _check_sftp_errors(filtered_lines)

    if not has_valid_content:
        if abort_if_empty:
            sys.exit(f"Aborting: File '{filename}' has no valid keyboard characters")
        else:
            return

    if error_found:
        sys.exit(f"Aborting: File '{filename}' contains error. Please check")

def format_column_position(pairs):
    """
    Sample: 
    [
    [1, "TOTAL OUTWARD AMOUNT IN RPP SETTLEMENT (TAR04)"],
    [57, "RM"],
    [74, f'{row["TRANSAMT_PAYNET"]:.2f}', "right"]
    ]
    """
    max_col = 0
    for item in pairs:
        col = int(item[0])
        txt = str(item[1])
        align = item[2] if len(item) > 2 else "left"

        if align == "left":
            end_col = col + len(txt) - 1
        else:
            end_col = col
        max_col = max(max_col, end_col)

    line = [" "] * max_col

    for item in pairs:
        col = int(item[0])
        txt = str(item[1])
        align = item[2] if len(item) > 2 else "left"

        if align == "right":
            start = col - len(txt)
        else:
            start = col - 1
        
        for i, ch in enumerate(txt):
            line[start + i] = ch
    return "".join(line)

def send_mail(from_email, to_email: list, subject='', message='', server_name='mailsmtp.pbb.my'):
    """
    Send email to mailnotes server
    """
    
    def append_edw_if_domain(from_email: str, edw_email: str = "noreply_edw@publicbank.com.my", domain: str = "@publicbank.com.my") -> str:
        if not from_email:
            return f"{edw_email} <{edw_email}>"

        # Append the EDW address only when the domain is missing
        if domain not in from_email:
            return f"{from_email} <{edw_email}>"
        return from_email

    msg = EmailMessage()
    msg['From'] = append_edw_if_domain(from_email)
    if not to_email:
        sys.exit("To email cannot be blank")
    msg['To'] = ', '.join(to_email)
    msg['Subject'] = subject
    msg.set_content(message)

    server = smtplib.SMTP(server_name) # NOSONAR
    try:
        server.send_message(msg)
        print('successfully sent the mail.')
    except Exception as e:
        sys.exit(f"Error: {e}")
    finally:
        server.quit()

def _df_arrow(self):
    table = self.fetch_arrow_table()
    return table.to_pandas(types_mapper=pd.ArrowDtype)

duckdb.DuckDBPyRelation.df_arrow = _df_arrow
duckdb.DuckDBPyConnection.df_arrow = _df_arrow
