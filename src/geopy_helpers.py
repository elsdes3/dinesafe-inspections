#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Workflow Utilities to run End-to-End Geocoding with GeoPy."""


import os
from random import randint
from time import sleep

import pandas as pd
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Bing
from sqlalchemy import create_engine


def run_bing_geocoder(row_number, street_address, verbose: bool = False):
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
    unique_addresses_missing_lat_lon,
    db_table_name=None,
    uri=None,
    min_delay_seconds=5,
    max_delay_seconds=10,
    verbose: bool = False,
):
    """Geocode a column with one or more street addresses."""
    engine = create_engine(uri)
    conn = engine.connect()
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
        # If geocoded output is not available in local database, then preform
        # geocoding for street address
        if df_query["num_matching_street_addresses"].iloc[0] == 0:
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
            # Append DataFrame of geocoded outputs to database
            df_geocoded.to_sql(
                name=db_table_name, con=conn, index=False, if_exists="append"
            )
        else:
            # If geocoded output is available in local database, then do not
            # geocode the same street address
            if verbose:
                print(
                    f"{row_num}: Found existing record for {street_address}. "
                    "Did nothing."
                )
    conn.close()
    engine.dispose()
