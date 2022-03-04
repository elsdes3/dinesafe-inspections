#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""Programmatic conversion of notebooks."""

# pylint: disable=invalid-name


import argparse
import os
import shlex
import subprocess
from datetime import datetime
from glob import glob
from typing import List


def convert_notebooks_to_html(
    notebooks_list: List, output_notebook_directory: str = "executed_notebooks"
):
    """Convert list of notebooks to HTML files."""
    for nb in notebooks_list:
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        fname = os.path.splitext(nb)[0]
        fname_out = fname.split("/")[-1] if "/" in fname else fname
        run_cmd(
            "jupyter nbconvert "
            f"--output-dir='{output_notebook_directory}' "
            f"--output='{fname_out}__{now}.ipynb' "
            f"{fname}.ipynb --to html"
        )


def run_cmd(cmd: str) -> None:
    """Run a shell command using Python."""
    print(cmd)
    process = subprocess.Popen(
        shlex.split(cmd), shell=False, stdout=subprocess.PIPE
    )
    while True:
        output = process.stdout.readline()
        if process.poll() is not None:
            break
        if output:
            print(str(output.strip(), "utf-8"))
    _ = process.poll()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nbdir",
        type=str,
        dest="nbdir",
        default="executed_notebooks",
        help="directory containing notebooks to be converted",
    )
    args = parser.parse_args()

    PROJ_ROOT_DIR = os.getcwd()
    data_dir = os.path.join(PROJ_ROOT_DIR, "data")
    output_notebook_dir = os.path.join(PROJ_ROOT_DIR, "executed_notebooks")

    raw_data_path = os.path.join(data_dir, "raw")

    notebook_list = glob(f"{args.nbdir}/*.ipynb")

    convert_notebooks_to_html(notebook_list, output_notebook_dir)
