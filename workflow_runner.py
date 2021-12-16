#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Programmatic execution of notebooks."""

# pylint: disable=invalid-name


from src.workflow.workflow_utils import analyze_infractions

if __name__ == "__main__":
    # Name of database table
    table_name = "inspections2"

    # Data file names to download (these are timestamps at which data
    # snapshot was captured by WayBackMachine)
    zip_filenames = [
        "20130723222156",
        "20150603085055",
        "20151012004454",
        "20160129205023",
        "20160317045436",
        "20160915001010",
        "20170303162206",
        "20170330001043",
        "20170726115444",
        "20190116215713",
        "20190126084933",
        "20190614092848",
        "20210626163552",
    ]

    # Order of DataFrame columns (to re-order raw data) in order to match
    # column order in database table
    cols_order_wanted = [
        "row_id",
        "establishment_id",
        "inspection_id",
        "establishment_name",
        "establishmenttype",
        "establishment_address",
        "latitude",
        "longitude",
        "establishment_status",
        "minimum_inspections_peryear",
        "infraction_details",
        "inspection_date",
        "severity",
        "action",
        "court_outcome",
        "amount_fined",
        "filename",
    ]

    establishment_types_wanted = [
        "Restaurant",
        "Food Take Out",
        "Food Store (Convenience / Variety)",  # equivalent to grocery store
        "Food Court Vendor",
        "Supermarket",  # equivalent to grocery store
        "Bakery",  # equivalent to grocery store
        "Butcher Shop",  # equivalent to grocery store
        "Cafeteria - Public Access",
        "Cocktail Bar / Beverage Room",
        "Fish Shop",  # equivalent to grocery store
        "Bake Shop",  # equivalent to grocery store
        "Flea Market",  # equivalent to grocery store
        "Farmer\\'s Market",  # equivalent to grocery store
    ]

    state = analyze_infractions(
        zip_filenames,
        cols_order_wanted,
        establishment_types_wanted,
        table_name,
        "addressinfo",
    )
    df = state.result().result()
    print(
        df["is_infraction"]
        .value_counts(normalize=True)
        .rename("fraction")
        .to_frame()
        .merge(
            df["is_infraction"]
            .value_counts()
            .rename("num_inspections")
            .to_frame(),
            left_index=True,
            right_index=True,
            how="inner",
        )
    )
