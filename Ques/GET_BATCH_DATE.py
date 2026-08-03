import pandas as pd
from sas7bdat import SAS7BDAT
from datetime import datetime, timedelta
import calendar
import os
import requests
from requests.auth import HTTPBasicAuth
import sys

# ============================================================
# Environment
# ============================================================
devops_base_url = os.getenv("AZDO_BASE_URL")
devops_pat = os.getenv("AZDO_PAT")
server_env = os.getenv("SERVER_ENV")

cert = "Data_Warehouse/Common/Cert/Public Bank Group Root CA.pem"
base_dir = sys.prefix
parquet_path = os.path.join(base_dir, cert)

API_VERSION = "7.0-preview.1"

# ============================================================
# Common helpers (reduce duplication)
# ============================================================

def _get_auth():
    return HTTPBasicAuth("PAT_USER", devops_pat)

def _parse_datetime(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def _format_datetime(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _get_variable_groups():
    url = f"{devops_base_url}/_apis/distributedtask/variablegroups?api-version={API_VERSION}"
    resp = requests.get(url, auth=_get_auth(), verify=cert)
    resp.raise_for_status()
    return resp.json().get("value", [])

def _get_variable_group_by_name(group_name):
    groups = _get_variable_groups()
    group = next((g for g in groups if g["name"] == group_name), None)
    if not group:
        raise ValueError(f"Group {group_name} not found")
    return group["id"]

def _get_variable_group_details(group_id):
    url = f"{devops_base_url}/_apis/distributedtask/variablegroups/{group_id}?api-version={API_VERSION}"
    resp = requests.get(url, auth=_get_auth(), verify=cert)
    resp.raise_for_status()
    return resp.json()

# ============================================================
# Batch date access
# ============================================================

def _batch_date_access(ctl_path, source_system_cd, system_name):
    # === LOCAL SAS FILE ===
    if os.path.exists(ctl_path):
        try:
            with SAS7BDAT(ctl_path) as reader:
                df = reader.to_data_frame()
            batch_date = df.loc[df["SOURCE_SYSTEM_CD"] == source_system_cd]
            return _format_datetime(pd.to_datetime(batch_date.iloc[0]["BATCH_DTTM"]))
        except Exception as e:
            print("Error:", e)

    # === AZURE DEVOPS FALLBACK ===
    group_name = f"{system_name}_BATCH_DATE_{server_env}"
    group_id = _get_variable_group_by_name(group_name)
    group_data = _get_variable_group_details(group_id)

    variables = group_data.get("variables", {})
    if source_system_cd not in variables:
        raise ValueError(f"Source system code {source_system_cd} not found")

    return variables[source_system_cd]["value"]

# ============================================================
# Public batch date APIs
# ============================================================

def get_batch_date_dwh(source_system_cd):
    ctl_path = "/sasdata/dwh/control/ctl_dwh_batch_dttm.sas7bdat"
    return _batch_date_access(ctl_path, source_system_cd, "DWH")

def get_batch_date_mart(source_system_cd):
    ctl_path = "/sasdata/dwh/control/ctl_mart_batch_dttm.sas7bdat"
    return _batch_date_access(ctl_path, source_system_cd, "MART")

# ============================================================
# Date utilities
# ============================================================

def first_date_of_month(date):
    dt = _parse_datetime(date)
    return _format_datetime(dt.replace(day=1, hour=0, minute=0, second=0))

def last_date_of_month(date):
    dt = _parse_datetime(date)
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return _format_datetime(dt.replace(day=last_day, hour=23, minute=59, second=59))

def format_date(date, format_str):
    return _parse_datetime(date).strftime(format_str)

def mid15_25_of_month(date):
    dt = _parse_datetime(date)
    dd = dt.day

    if dd == 25:
        start_date = dt.replace(day=15)
        end_date = dt.replace(day=24)
    elif dd == 15:
        start_date = (dt.replace(day=1) - timedelta(days=1)).replace(day=25)
        end_date = dt.replace(day=14)
    else:
        start_date = None
        end_date = None

    return {
        "start_date": _format_datetime(start_date) if start_date else None,
        "end_date": _format_datetime(end_date) if end_date else None
    }

def get_past_n_date(date, n):
    dt = _parse_datetime(date)
    return _format_datetime(dt - timedelta(days=n))

def last_date_of_batch_date(date):
    dt = _parse_datetime(date)
    return _format_datetime(dt.replace(hour=23, minute=59, second=59))

# ============================================================
# Update batch date
# ============================================================

def get_azure_batch_dataframes():
    """
    Retrieve full batch date DataFrames from Azure DevOps.
    Returns (df, df_mart) tuple or (None, None) on failure.
    """
    try:
        groups = _get_variable_groups()

        # DWH date
        group_name = f"DWH_BATCH_DATE_{server_env}"
        group = next((g for g in groups if g["name"] == group_name), None)
        if not group:
            print(f"Invalid library {group_name} in Azure DevOps")
            return None, None

        df = pd.DataFrame.from_dict(group['variables'], orient='index')
        df = df.reset_index()
        df.columns = ['SOURCE_SYSTEM_CD', 'BATCH_DTTM']
        df['BATCH_DTTM'] = pd.to_datetime(df['BATCH_DTTM'], format="%Y-%m-%d %H:%M:%S")

        # DWH mart date
        group_name_mart = f"MART_BATCH_DATE_{server_env}"
        group_mart = next((g for g in groups if g["name"] == group_name_mart), None)
        if not group_mart:
            print(f"Invalid library {group_name_mart} in Azure DevOps")
            return None, None

        df_mart = pd.DataFrame.from_dict(group_mart['variables'], orient='index')
        df_mart = df_mart.reset_index()
        df_mart.columns = ['SOURCE_SYSTEM_CD', 'BATCH_DTTM']
        df_mart['BATCH_DTTM'] = pd.to_datetime(df_mart['BATCH_DTTM'], format="%Y-%m-%d %H:%M:%S")

        return df, df_mart
    except Exception as e:
        print(f"Azure Connection failed: {e}")
        return None, None


def _build_group_name(mart_identifier):
    """Return the correct variable group name based on mart_identifier."""
    if mart_identifier:
        return f"MART_BATCH_DATE_{server_env}"
    return f"DWH_BATCH_DATE_{server_env}"


def _fetch_current_batch_date(source_system_cd, mart_identifier):
    """Fetch the current batch date for the given source system."""
    if mart_identifier:
        return get_batch_date_mart(source_system_cd)
    return get_batch_date_dwh(source_system_cd)


def _apply_batch_date_update(group_name, source_system_cd, new_value):
    """Perform a single PUT update of the batch date in Azure DevOps."""
    group_id = _get_variable_group_by_name(group_name)
    group_data = _get_variable_group_details(group_id)

    variables = group_data.get("variables", {})
    if source_system_cd not in variables:
        raise ValueError(f"Source system code {source_system_cd} not found")

    group_data["variables"][source_system_cd]["value"] = new_value

    url = f"{devops_base_url}/_apis/distributedtask/variablegroups/{group_id}?api-version={API_VERSION}"
    headers = {"Content-Type": "application/json"}

    resp = requests.put(url, json=group_data, auth=_get_auth(), headers=headers, verify=cert)
    resp.raise_for_status()


def _verify_batch_date_updated(source_system_cd, new_value, mart_identifier, max_retries=3):
    """
    Verify the batch date was updated correctly.
    Re-applies the update if the post-update value does not match new_value.
    Raises RuntimeError if the value remains incorrect after max_retries.
    """
    group_name = _build_group_name(mart_identifier)

    for attempt in range(1, max_retries + 1):
        updated_value = _fetch_current_batch_date(source_system_cd, mart_identifier)
        if updated_value == new_value:
            return
        print(
            f"Batch date mismatch on attempt {attempt}/{max_retries}: "
            f"expected '{new_value}', got '{updated_value}'. Retrying update..."
        )
        _apply_batch_date_update(group_name, source_system_cd, new_value)

    final_value = _fetch_current_batch_date(source_system_cd, mart_identifier)
    if final_value != new_value:
        raise RuntimeError(
            f"Failed to update batch date for '{source_system_cd}' after {max_retries} retries. "
            f"Expected '{new_value}', got '{final_value}'."
        )


def update_batch_date(source_system_cd, new_value, mart_identifier=0):
    group_name = _build_group_name(mart_identifier)
    _apply_batch_date_update(group_name, source_system_cd, new_value)
    _verify_batch_date_updated(source_system_cd, new_value, mart_identifier)


def update_variable(group_name, variable_name, new_value):
    group_id = _get_variable_group_by_name(group_name)
    group_data = _get_variable_group_details(group_id)

    group_data["variables"][variable_name]["value"] = new_value

    url = f"{devops_base_url}/_apis/distributedtask/variablegroups/{group_id}?api-version={API_VERSION}"
    headers = {"Content-Type": "application/json"}

    resp = requests.put(url, json=group_data, auth=_get_auth(), headers=headers, verify=cert)
    resp.raise_for_status()
