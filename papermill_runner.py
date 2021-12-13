#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Programmatic execution of notebooks."""

# pylint: disable=invalid-name

import os
from datetime import datetime
from typing import Dict, List

import papermill as pm

PROJ_ROOT_DIR = os.getcwd()
data_dir = os.path.join(PROJ_ROOT_DIR, "data")
output_notebook_dir = os.path.join(PROJ_ROOT_DIR, "executed_notebooks")

raw_data_path = os.path.join(data_dir, "raw")

zero_dict_nb_name = "0_ci_get_data.ipynb"
one_dict_nb_name = "1_get_data.ipynb"
two_dict_nb_name = "2_sql_filter_transform.ipynb"
three_dict_nb_name = "3_geocode_missing_locations.ipynb"
four_dict_nb_name = "4_get_stats_by_neighbourhood.ipynb"
seven_dict_nb_name = "7_feat_engineering.ipynb"
eight_dict_nb_name = "8_ml.ipynb"

zero_dict = dict(
    dload_fpath=(
        "data/processed/filtered_transformed_filledmissing_data__"
        "20211209_120056.zip"
    ),
    action="download",
)
one_dict = dict(
    table_name="inspections",
    zip_filenames=[
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
    ],
    cols_order_wanted=[
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
    ],
)
two_dict = dict(transformed_fname_prefix="filtered_transformed_data")
three_dict = dict(
    geocoded_fname_prefix="filtered_transformed_filledmissing_data"
)
four_dict = dict(
    url=(
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show"
    ),
    neigh_params={"id": "4def3f65-2a65-4a4f-83c4-b2a4aed72d46"},
    neigh_profile_params={"id": "6e19a90f-971c-46b3-852c-0c48c436d1fc"},
    mci_params={"id": "247788f6-ca20-42e8-b00f-894ac43053e5"},
    processed_data_fname_prefix="processed",
)
seven_dict = dict(proc_data_fname_prefix="processed_with_features")
eight_dict = dict(trained_model_fname="trained_model")


def papermill_run_notebook(
    nb_dict: Dict, output_notebook_directory: str = "executed_notebooks"
) -> None:
    """Execute notebook with papermill"""
    for notebook, nb_params in nb_dict.items():
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_nb = os.path.basename(notebook).replace(
            ".ipynb", f"-{now}.ipynb"
        )
        print(
            f"\nInput notebook path: {notebook}",
            f"Output notebook path: {output_notebook_directory}/{output_nb} ",
            sep="\n",
        )
        for key, val in nb_params.items():
            print(key, val, sep=": ")
        pm.execute_notebook(
            input_path=notebook,
            output_path=f"{output_notebook_directory}/{output_nb}",
            parameters=nb_params,
        )


def run_notebooks(
    notebooks_list: List, output_notebook_directory: str = "executed_notebooks"
) -> None:
    """Execute notebooks from CLI.
    Parameters
    ----------
    nb_dict : List
        list of notebooks to be executed
    Usage
    -----
    > import os
    > PROJ_ROOT_DIR = os.path.abspath(os.getcwd())
    > one_dict_nb_name = "a.ipynb
    > one_dict = {"a": 1}
    > run_notebook(
          notebook_list=[
              {os.path.join(PROJ_ROOT_DIR, one_dict_nb_name): one_dict}
          ]
      )
    """
    for nb in notebooks_list:
        papermill_run_notebook(
            nb_dict=nb, output_notebook_directory=output_notebook_directory
        )


if __name__ == "__main__":
    nb_dict_list = [
        zero_dict,
        # one_dict,
        # two_dict,
        # three_dict,
        four_dict,
        seven_dict,
        eight_dict,
    ]
    nb_name_list = [
        zero_dict_nb_name,
        # one_dict_nb_name,
        # two_dict_nb_name,
        # three_dict_nb_name,
        four_dict_nb_name,
        seven_dict_nb_name,
        eight_dict_nb_name,
    ]
    notebook_list = [
        {os.path.join(PROJ_ROOT_DIR, nb_name): nb_dict}
        for nb_dict, nb_name in zip(nb_dict_list, nb_name_list)
    ]
    run_notebooks(
        notebooks_list=notebook_list,
        output_notebook_directory=output_notebook_dir,
    )
