#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Workflow Utilities to run End-to-End Geocoding with GeoPy."""


import os
from random import randint
from time import sleep
from typing import Dict, Union

import pandas as pd
import snowflake.connector
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Bing
from snowflake.connector.pandas_tools import write_pandas


def run_bing_geocoder(
    row_number, street_address, verbose: bool = False
) -> Dict[str, Union[str, None]]:
    """Geocode a single street addresses."""
    # Set up the Bing Geocoder
    geolocator = Bing(os.getenv("BING_MAPS_KEY"))

    # Perform geocoding
    try:
        # Geocode a single street address
        location = geolocator.geocode(
            street_address, include_neighborhood=True, exactly_one=True
        )
        # Get the street address key from the .raw attribute of the geocoded
        # output
        address_components = location.raw["address"]
        # Get the neighbourhood (if available)
        neighbourhood = (
            address_components["neighborhood"]
            if "neighborhood" in list(address_components)
            else None
        )
        # Get the locality (if available)
        locality = address_components["locality"]
        # Get the latitude and longitude coordinates
        lat, lon = location.raw["point"]["coordinates"]
        # Store geocoded output in a dictionary
        record = {
            "address": street_address,
            "neighbourhood": neighbourhood,
            "locality": locality,
            "formattedAddress": address_components["formattedAddress"]
            if "formattedAddress" in address_components
            else None,
            "postalCode": address_components["postalCode"]
            if "postalCode" in address_components
            else None,
            "latitude": lat,
            "longitude": lon,
        }
        if verbose:
            print(
                f"{row_number}: Geocode completed for {street_address}", end=""
            )
    except GeocoderTimedOut as e:
        # If geocoding did not work, create dictionary with None for each key
        # in the dictionary where geocoding was successful
        if verbose:
            print(
                "{} - Error: geocode failed on input {} with msg: {}".format(
                    row_number, street_address, e.message
                )
            )
        record = {
            "address": street_address,
            "neighbourhood": None,
            "locality": None,
            "formattedAddress": None,
            "postalCode": None,
            "latitude": None,
            "longitude": None,
        }
    return record


def geocode_missing_lat_lon(
    connector_dict: Dict[str, str],
    unique_addresses_missing_lat_lon,
    db_table_name=None,
    min_delay_seconds=5,
    max_delay_seconds=10,
    verbose: bool = False,
) -> None:
    """Geocode a column with one or more street addresses."""
    conn = snowflake.connector.connect(**connector_dict)
    cur = conn.cursor()
    # Iterate over all street addresses to be geocoded
    for row_num, street_address in unique_addresses_missing_lat_lon.items():
        # Clean the street address
        street_address_clean = street_address.replace("'", "\\'")
        # Query local database for existing record with street address
        df_query = pd.read_sql(
            f"""
            SELECT COUNT(*) AS num_matching_street_addresses
            FROM {db_table_name}
            WHERE address = '{street_address_clean}'
            """,
            con=conn,
        )
        # print(df_query)
        # If geocoded output is not available in local database, then perform
        # geocoding for street address
        if df_query["NUM_MATCHING_STREET_ADDRESSES"].iloc[0] == 0:
            # Geocode
            geocoded_output = run_bing_geocoder(
                row_num, street_address, verbose
            )
            # Pause
            if verbose:
                print("...Pausing...", end="")
            sleep(randint(min_delay_seconds, max_delay_seconds))
            if verbose:
                print("Done.")
            # Convert dictionary of geocoded outputs to DataFrame
            df_geocoded = pd.DataFrame.from_dict(
                geocoded_output, orient="index"
            ).T.astype({"latitude": float, "longitude": float})
            df_geocoded.columns = df_geocoded.columns.str.upper()
            # print(df_geocoded.dtypes)
            # Append DataFrame of geocoded outputs to database
            success, _, nrows, _ = write_pandas(
                conn, df_geocoded, db_table_name
            )
            assert success
            assert nrows == 1
        else:
            # If geocoded output is available in local database, then do not
            # geocode the same street address
            if verbose:
                print(
                    f"{row_num}: Found existing record for {street_address}. "
                    "Did nothing."
                )
    cur.close()
    conn.close()
