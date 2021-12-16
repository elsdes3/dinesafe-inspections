#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Workflow Utilities to run End-to-End Data Analysis."""

# pylint: disable=invalid-name,
# pylint: disable=logging-fstring-interpolation
# pylint: disable=too-many-locals
# pylint: disable=no-member
# pylint: disable=too-many-arguments


import configparser
import os
from io import BytesIO
from typing import List
from zipfile import ZipFile

import pandas as pd
import requests
from prefect import flow, task
from prefect.task_runners import DaskTaskRunner
from prefect.utilities.logging import get_logger
from sqlalchemy import create_engine

from src.geopy_helpers import geocode_missing_lat_lon


# Functionality from 1_get_data.ipynb
def get_state_result(state):
    """Get result from Prefect state object."""
    return state.result()


@task
def get_database_uris(config_filepath: str = "../sql.ini") -> List[str]:
    """Get MySQL database URIs."""
    logger = get_logger()
    logger.info("Creating database connection URIs...")
    config = configparser.ConfigParser()
    config.read(config_filepath)
    default_cfg = config["default"]

    DB_TYPE = default_cfg["DB_TYPE"]
    DB_DRIVER = default_cfg["DB_DRIVER"]
    DB_USER = default_cfg["DB_USER"]
    DB_PASS = default_cfg["DB_PASS"]
    DB_HOST = default_cfg["DB_HOST"]
    DB_PORT = default_cfg["DB_PORT"]
    DB_NAME = default_cfg["DB_NAME"]

    # Connect to single database (required to create database)
    URI_NO_DB = (
        f"{DB_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
    )

    # Connect to all databases (required to perform CRUD operations and submit
    # queries)
    URI = (
        f"{DB_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/"
        f"{DB_NAME}"
    )
    logger.info("Done.")
    return [URI_NO_DB, URI, DB_NAME]


@task
def prepare_database(outputs: List[str], dbase_table_name: str) -> None:
    """Perform Database administration tasks."""
    logger = get_logger()
    logger.info("Creating database and table...")
    conn_uri_no_db, conn_uri, db_name = outputs
    # Create database
    engine = create_engine(conn_uri_no_db)
    conn = engine.connect()
    # _ = conn.execute(f"DROP DATABASE IF EXISTS {db_name};")
    _ = conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    conn.close()
    engine.dispose()

    # Create database table
    engine = create_engine(conn_uri)
    conn = engine.connect()
    # _ = conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_table_query = f"""
                         CREATE TABLE IF NOT EXISTS {dbase_table_name} (
                             row_id INT,
                             establishment_id INT,
                             inspection_id INT,
                             establishment_name TEXT,
                             establishmenttype TEXT,
                             establishment_address TEXT,
                             latitude FLOAT,
                             longitude FLOAT,
                             establishment_status TEXT,
                             minimum_inspections_peryear INT,
                             infraction_details TEXT,
                             inspection_date DATE,
                             severity TEXT,
                             action TEXT,
                             court_outcome TEXT,
                             amount_fined FLOAT,
                             filename VARCHAR(20)
                         )
                         """
    _ = conn.execute(create_table_query)
    conn.close()
    engine.dispose()
    logger.info("Done.")


@task(
    name="Retrieve DineSafe infractions data from WayBackMachine",
    retries=2,
    retry_delay_seconds=0,
)
def extract(
    zip_filenames: List[str], outputs: List[str], table_name: str
) -> List[str]:
    """Retrieve dinesafe data snapshot XML files from WayBackMachine."""
    _, uri, _ = outputs
    logger = get_logger()
    available_files = []
    for zip_fname in zip_filenames:
        # Assemble source URL
        url = (
            f"https://web.archive.org/web/{zip_fname}/"
            "http://opendata.toronto.ca/public.health/dinesafe/dinesafe.zip"
        )
        # Create path to target dir, where extracted .XML file will be found
        target_dir = f"data/raw/{zip_fname}"
        fpath = f"data/raw/{zip_fname}/dinesafe.xml"
        if not os.path.exists(fpath):
            logger.info(f"Downloading {zip_fname} locally to {fpath}...")
            # Get zipped file containing .XML file
            r = requests.get(url)
            # Extract to target dir
            with ZipFile(BytesIO(r.content)) as zfile:
                zfile.extractall(target_dir)
            logger.info("Done.")
        else:
            logger.info(f"Found {zip_fname} locally at {fpath}. Did nothing.")
        available_files.append(target_dir)

    # Get list of filenames with data already in database
    engine = create_engine(uri)
    conn = engine.connect()
    existing_filenames = pd.read_sql(
        f"SELECT DISTINCT(filename) AS fnames FROM {table_name}",
        con=conn,
    )
    existing_filenames = existing_filenames["fnames"].astype(int).tolist()
    conn.close()
    engine.dispose()
    return [available_files, existing_filenames]


def read_data(filepath):
    """Load an XML file into a DataFrame."""
    fname = filepath.split("/", 3)[-2]
    return pd.read_xml(filepath).assign(filename=fname)


def process_data(df, cols_order_wanted):
    """Process inspections data."""
    # Datetime formatting
    df["INSPECTION_DATE"] = pd.to_datetime(df["INSPECTION_DATE"])
    # Change datatype 1/2
    df = df.astype({"MINIMUM_INSPECTIONS_PERYEAR": int})
    # Remove commas from column and convert string to float
    cname = "AMOUNT_FINED"
    if df[cname].dtype == "object":
        df[cname] = pd.to_numeric(
            df[cname].astype(str).str.replace(",", ""), errors="coerce"
        )
    # Append latitude and longitude columns, if not found in the data
    for loc_col in ["LATITUDE", "LONGITUDE"]:
        if loc_col not in list(df):
            df[loc_col] = None
    # Change column names to lowercase, Re-order columns and Change datatype
    # of the latitude and longitude columns
    df = df.rename(columns=str.lower)[cols_order_wanted].astype(
        {"latitude": float, "longitude": float, "filename": int}
    )
    return df


@task(name="Process raw infraction data")
def transform(f, existing_filenames, cols_order_wanted, table_name):
    """Transform data in downloaded XML files."""
    logger = get_logger()
    f_int = int(os.path.basename(f))
    if f_int not in existing_filenames:
        fpath = f"{f}/dinesafe.xml"
        logger.info(f"Transforming {fpath}...")
        df = read_data(fpath)
        df = process_data(df, cols_order_wanted)
        logger.info("Done.")
    else:
        logger.info(
            f"Found data from {f_int} in database table {table_name}. "
            "Did nothing."
        )
        df = pd.DataFrame()
    return df


@flow(task_runner=DaskTaskRunner(), name="Process raw infraction data")
def transform_all(files_lists, cols_order_wanted, table_name) -> List:
    """Transform data in downloaded XML files."""
    available_files, existing_filenames = files_lists
    dfs_state = []
    for f in available_files:
        state = transform(f, existing_filenames, cols_order_wanted, table_name)
        dfs_state.append(state)
    return dfs_state


@task(name="Append transformed infractions to database table")
def load(
    dfs: List[pd.DataFrame], outputs: List[str], table_name="inspections"
) -> pd.DataFrame:
    """Vertically concatenate list of DataFrames and Append to database."""
    _, uri, _ = outputs
    logger = get_logger()
    dfs_all = pd.concat(dfs, ignore_index=True).drop_duplicates(
        keep="first", subset=None
    )
    engine = create_engine(uri)
    conn = engine.connect()
    if not dfs_all.empty:
        logger.info(f"Appending data to database table {table_name}...")
        dfs_all.to_sql(
            name=table_name, con=conn, index=False, if_exists="append"
        )
        logger.info("Done.")
    else:
        logger.info(
            f"No new data to append to database table {table_name}. "
            "Did nothing."
        )
    all_existing_filenames = pd.read_sql(
        f"""
        SELECT DISTINCT(filename) AS fnames
        FROM {table_name}
        """,
        con=conn,
    )["fnames"].tolist()
    conn.close()
    engine.dispose()
    return all_existing_filenames


# Functionality from 2_*.ipynb
def get_actions_str(conn, table_name: str) -> List:
    """Get actions str for SQL query."""
    df_actions = pd.read_sql(
        f"""
        SELECT DISTINCT(action)
        FROM {table_name}
        """,
        con=conn,
    )
    action_strs = []
    dtypes_dict = {}
    for _, row in df_actions.iterrows():
        act_str = row["action"] if row["action"] else "NULL"
        action_value = f"= '{act_str}'" if act_str != "NULL" else "IS NULL"
        action_cname = act_str.lower().replace(" ", "_")
        sql_str = (
            f"SUM(CASE WHEN action {action_value} THEN 1 ELSE 0 END) AS "
            f"num_{action_cname}"
        )
        action_strs.append(sql_str)
        dtypes_dict[f"num_{action_cname}"] = int
    action_strs = ",\n".join(action_strs)
    return [action_strs, dtypes_dict]


def get_outcomes_str(conn, table_name: str) -> List:
    """Get actions str for SQL query."""
    df_court_outcomes = pd.read_sql(
        f"""
        SELECT DISTINCT(court_outcome)
        FROM {table_name}
        """,
        con=conn,
    )
    outcome_strs = []
    outcomes_dtypes_dict = {}
    for _, row in df_court_outcomes.iterrows():
        outcome_str = row["court_outcome"] if row["court_outcome"] else "NULL"
        outcome_value = (
            f"= '{outcome_str}'" if outcome_str != "NULL" else "IS NULL"
        )
        outcome_cname = (
            outcome_str.lower()
            .replace(" ", "_")
            .replace("-", "")
            .replace("__", "_")
            .replace("&_", "")
        )
        outcome_sql_str = (
            f"SUM(CASE WHEN court_outcome {outcome_value} THEN 1 ELSE 0 END) "
            f"AS num_{outcome_cname}"
        )
        outcome_strs.append(outcome_sql_str)
        outcomes_dtypes_dict[(f"num_{outcome_cname}")] = int
    outcome_strs = ",\n".join(outcome_strs)
    return [outcome_strs, outcomes_dtypes_dict]


@task
def aggregate_inspections(
    uri: str, table_name: str, establishment_types_wanted: List[str]
) -> pd.DataFrame:
    """Get inspections by aggregating all recorded infractions."""
    logger = get_logger()
    logger.info("Aggregate infractions into inspections...")
    engine = create_engine(uri)
    conn = engine.connect()
    action_strs, dtypes_dict = get_actions_str(conn, table_name)
    outcome_strs, outcomes_dtypes_dict = get_outcomes_str(conn, table_name)
    establishment_types_wanted_str = (
        "('" + "', '".join(establishment_types_wanted) + "')"
    )
    groupby_cols = [
        "establishment_id",
        "establishmenttype",
        "establishment_address",
        "inspection_date",
        "inspection_id",
        "establishment_status",
        "action",
        "court_outcome",
    ]
    groupby_cols_str = ",".join(groupby_cols)
    case_str = "CAST(SUM(CASE WHEN severity LIKE "
    when_str = " THEN 1 ELSE 0 END) AS SIGNED) "
    group_concat_str2 = (
        "GROUP_CONCAT(infractions_summary SEPARATOR '. ') AS "
        "infractions_summary"
    )
    group_concat_str = (
        "GROUP_CONCAT(infraction_details SEPARATOR '. ') AS "
        "infractions_summary"
    )
    df_query = pd.read_sql(
        f"""
        SELECT establishment_id,
            establishmenttype,
            establishment_address,
            inspection_date,
            inspection_id,
            establishment_status,
            {group_concat_str2},
            SUM(num_significant) AS num_significant,
            SUM(num_crucial) AS num_crucial,
            SUM(num_minor) AS num_minor,
            SUM(num_na) AS num_na,
            SUM(num_infractions) AS num_infractions,
            {action_strs},
            {outcome_strs}
        FROM (
            SELECT establishment_id,
                establishmenttype,
                establishment_address,
                inspection_date,
                inspection_id,
                {group_concat_str},
                establishment_status,
                action,
                court_outcome,
                {case_str}"%%S - Significant"{when_str}AS num_significant,
                {case_str}"%%C - Crucial"{when_str}AS num_crucial,
                {case_str}"%%M - Minor"{when_str}AS num_minor,
                {case_str}"%%NA -"{when_str}AS num_na,
                COUNT(infraction_details) AS num_infractions
            FROM {table_name}
            WHERE establishmenttype IN {establishment_types_wanted_str}
            AND severity IS NULL
            OR severity IN ('S - Significant', 'C - Crucial', 'M - Minor')
            GROUP BY {groupby_cols_str}
        ) AS combo
        GROUP BY establishment_id,
                establishmenttype,
                establishment_address,
                inspection_date,
                inspection_id,
                establishment_status
        """,
        con=conn,
    )
    df_query = df_query.astype(dtypes_dict).astype(outcomes_dtypes_dict)
    conn.close()
    engine.dispose()
    logger.info("Done.")
    return df_query


@task
def remove_multi_day_inspections(df: pd.DataFrame) -> pd.DataFrame:
    """Remove inspection IDs taking more than one day to complete."""
    logger = get_logger()
    logger.info("Removing multi-day inspections...")
    merge_cols = [
        "establishment_id",
        "establishmenttype",
        "establishment_address",
        "inspection_id",
    ]
    df_query_no_multi_day_inspections = (
        df.groupby(merge_cols, as_index=False)["inspection_date"]
        .nunique()
        .query("inspection_date == 1")
        .sort_values(by=["inspection_date"], ascending=False)
    )
    df = df_query_no_multi_day_inspections.drop(
        columns=["inspection_date"]
    ).merge(df, on=merge_cols)
    df["inspection_date"] = pd.to_datetime(df["inspection_date"])
    df = df.sort_values(by=merge_cols + ["inspection_date"])
    logger.info("Done.")
    return df


@task
def remove_reinspections(df: pd.DataFrame) -> pd.DataFrame:
    """Remove re-inspections."""
    logger = get_logger()
    logger.info("Remove re-inspections...")
    df["days_to_next"] = (
        df.groupby(
            [
                "establishment_id",
                "establishmenttype",
                "establishment_address",
            ],
        )["inspection_date"]
        .diff(-1)
        .dt.days.abs()
    )
    df = (
        df.query("days_to_next > 2 | days_to_next.isna()")
        .reset_index(drop=True)
        .copy()
    )
    logger.info("Done.")
    return df


@task
def create_class_labels(
    df: pd.DataFrame, label_col_name: str = "is_infraction"
) -> pd.DataFrame:
    """Append column with class labels."""
    logger = get_logger()
    logger.info("Create class labels column...")
    mask = (df["num_significant"] > 0) | (df["num_crucial"] > 0)
    df[label_col_name] = 0
    df.loc[mask, label_col_name] = 1
    logger.info("Done.")
    return df


@flow(name="Converting infractions into inspections")
def convert_infractions_to_inspections(
    establishment_types_wanted: List[str],
    outputs: List[str],
    table_name: str,
    distinct_fnames: List[str],
    label_col_name: str = "is_infraction",
):
    """Aggregate infractions into inspections, filter and create labels."""
    _, uri, _ = outputs
    df = aggregate_inspections(uri, table_name, establishment_types_wanted)
    df = remove_multi_day_inspections(df)
    df = remove_reinspections(df)
    df = create_class_labels(df, label_col_name)
    return df


# Functionality from 3_*.ipynb
@task
def get_missing_lat_lon(
    df: pd.DataFrame, outputs: List[str], table_name: str
) -> List[pd.DataFrame]:
    """Get unique locations that are missing a latitude and longitude."""
    _, uri, _ = outputs
    logger = get_logger()
    logger.info("Getting locations missing a latitude and longitude...")
    engine = create_engine(uri)
    conn = engine.connect()
    df_query = pd.read_sql(
        f"""
        SELECT establishment_id,
               establishmenttype,
               establishment_address,
               MAX(latitude) AS latitude,
               MAX(longitude) AS longitude
        FROM {table_name}
        GROUP BY establishment_id, establishmenttype, establishment_address
        """,
        con=conn,
    )
    conn.close()
    engine.dispose()
    df_with_lat_lon = df.merge(
        df_query,
        on=["establishment_id", "establishmenttype", "establishment_address"],
        how="left",
    )
    df_addr_lat_lon = (
        df_with_lat_lon.query("latitude.isnull() | longitude.isnull()")
        .groupby("establishment_address", as_index=False)[
            ["latitude", "longitude"]
        ]
        .max()
    )
    unique_addresses_missing_lat_lon = (
        df_addr_lat_lon["establishment_address"].str.title()
        + ", Toronto, ON, Canada"
    )
    logger.info("Done.")
    return [df_with_lat_lon, unique_addresses_missing_lat_lon]


@task
def geocode_missing_addr_lat_lon(
    df_outputs: List[pd.DataFrame],
    outputs: List[str],
    geocoded_table_name,
    min_delay: int = 1,
    max_delay: int = 3,
) -> pd.DataFrame:
    """Geocode locations with a missing address."""
    _, uri, _ = outputs
    logger = get_logger()
    logger.info("Geocoding locations a missing co-ordinates...")
    engine = create_engine(uri)
    conn = engine.connect()
    df_with_lat_lon, unique_addresses_missing_lat_lon = df_outputs
    geocode_missing_lat_lon(
        unique_addresses_missing_lat_lon,
        geocoded_table_name,
        uri,
        min_delay,
        max_delay,
    )
    address_uppercase_sql = (
        "UCASE(REPLACE(address, ', Toronto, ON, Canada', ''))"
    )
    df_query = pd.read_sql(
        f"""
        SELECT {address_uppercase_sql} AS establishment_address,
               latitude AS latitude_geo,
               longitude AS longitude_geo
        FROM addressinfo
        """,
        con=conn,
    )
    conn.close()
    engine.dispose()
    df_with_lat_lon_filled = df_with_lat_lon.merge(
        df_query, on=["establishment_address"], how="left"
    )
    logger.info("Done.")
    return df_with_lat_lon_filled


@task
def replace_missing_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values in lat-long columns with geocoded values."""
    logger = get_logger()
    logger.info("Replacing missing co-ordinates with geocoded values...")
    df["latitude"] = df["latitude"].fillna(df["latitude_geo"])
    df["longitude"] = df["longitude"].fillna(df["longitude_geo"])
    df = df.drop(columns=["latitude_geo", "longitude_geo"])
    logger.info("Done.")
    return df


# Flow
@flow(name="Run through end-to-end analysis workflow")
def analyze_infractions(
    zip_filenames: List,
    cols_order_wanted: List,
    establishment_types_wanted: List,
    table_name: str = "inspections",
    geocoded_table_name: str = "addressinfo",
) -> pd.DataFrame:
    """Retrieve data, process and append to database table."""
    # Database Administration
    outputs = get_database_uris()
    prepared_dbase = prepare_database(outputs, table_name)

    # Extract
    files_lists = extract(
        zip_filenames, outputs, table_name, wait_for=[prepared_dbase]
    )

    # Transform
    subflow_state = transform_all(files_lists, cols_order_wanted, table_name)

    # Load
    dfs = list(map(get_state_result, tuple(subflow_state.result())))
    distinct_fnames = load(dfs, outputs, table_name)

    # Filter invalid infractions and Aggregate into inspections
    df = convert_infractions_to_inspections(
        establishment_types_wanted,
        outputs,
        table_name,
        distinct_fnames,
        "is_infraction",
    )

    # Geocode missing latitudes and longitudes
    df_outputs = get_missing_lat_lon(df.result().result(), outputs, table_name)
    df_geocoded = geocode_missing_addr_lat_lon(
        df_outputs, outputs, geocoded_table_name, 1, 3
    )
    df = replace_missing_lat_lon(df_geocoded)
    return df
