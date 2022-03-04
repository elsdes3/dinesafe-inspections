# Predicting Infractions during Dinesafe Food Inspections

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/elsdes3/dinesafe-inspections)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/elsdes3/dinesafe-inspections/master/0_get_data.ipynb)
![CI](https://github.com/elsdes3/dinesafe-inspections/actions/workflows/main.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/mit)
![OpenSource](https://badgen.net/badge/Open%20Source%20%3F/Yes%21/blue?icon=github)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
![prs-welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)
![pyup](https://pyup.io/repos/github/elsdes3/dinesafe-inspections/shield.svg)

## [Table of Contents](#table-of-contents)
1. [About](#about)
   * [Overview](#overview)
   * [Background](#background)
   * [Present Operations](#present-operations)
   * [Objectives](#objectives)
2. [Analysis](#analysis)
3. [Notes](#notes)
4. [Project Organization](#project-organization)

## [About](#about)

### [Overview](#overview)
This project aims to explore the feasibility of using [predictive modeling](https://www.netsuite.com/portal/resource/articles/financial-management/predictive-modeling.shtml) to predict [significant or crucial infractions during food inspections](https://www.toronto.ca/community-people/health-wellness-care/health-programs-advice/food-safety/dinesafe/dinesafe-infractions/) that would be detected during a physical inspection conducted by the DineSafe ([1](https://www.toronto.ca/community-people/health-wellness-care/health-programs-advice/food-safety/dinesafe/), [2](https://www.toronto.ca/community-people/health-wellness-care/health-programs-advice/food-safety/dinesafe/about-dinesafe/)) program at restaurants, grocery stores and similar businesses in the City of Toronto.

This is work in progress.

### [Background](#background)
Of the nearly 11,000 annual inspections performed by DineSafe inspectors at establishments in the City of Toronto, a small fraction will discover infractions that present a potential or immediate health hazard. These are called significant or crucial infractions respectively. The result of an inspection revealing infractions can be the closing of the business in order to protect the health of customers that dine at the establishment.

### [Present Operations](#present-operations)
Currently, determining the presence of an infraction that is associated with a health hazard requires a physical inspection of the establishment. For obvious reasons, the sooner such infractions are caught (and can be fixed) the better. The minimum number of annual inspections of an establishment depends on the level of risk assigned to that establishment, with the higher rish establishments being inspected more often. However, there are a large number of establishments assigned a higher annual inspection frequency meaning that these, and all other, inspections need to be prioritized. Additionally, the majority of inspepctions (including establishments inspected multiple times a year) do not always reveal an infraction presenting a health hazard. Being able to predict the presence of such infractions ahead of time and prioritizing inspections at affected establishments can help eliminate such hazards sooner and reduce the risk of customers being exposed to these hazards.

### [Objectives](#objectives)
The first objective of this project is to use quantitative modeling techniques to predict the presence of a significant or crucial infraction ahead of a planned (physical) inspection of an establishment. Inspections predicted to reveal such an infraction must be physically inspected by an inspector in order to verify the output of the predictive modeling approach.

Not all predicted infractions are equally likely. So, a second objective is to develop an inspection piority with the inspections most likely to reveal such an infraction given an earlier recommended physical inspection date than others. The inspections with a likelihood of an infraction above a certain threshold will be recommended for an immediate physical inspection. Inspections falling in the next highest likelihood block of likelihoods of an infraction will be assigned a physical inspection a day later and so on.

This project could be useful to organizers of the DineSafe program (the client).

## [Analysis](#analysis)
1. `1_get_data.ipynb` ([view](https://nbviewer.org/github/elsdes3/dinesafe-inspections/blob/main/1_get_data.ipynb))
   - download historical DineSafe infractions data from inspections performed at establishments
2. `2_sql_filter_transform.ipynb` ([view](https://nbviewer.org/github/elsdes3/dinesafe-inspections/blob/main/2_sql_filter_transform.ipynb))
   - transform infractions into inspections
   - remove unusable inspections
3. `3_geocode_missing_locations.ipynb` ([view](https://github.com/elsdes3/dinesafe-inspections/blob/main/3_geocode_missing_locations.ipynb))
   - for establihments missing a latitude and longitude, use [geocoding](https://desktop.arcgis.com/en/arcmap/latest/manage-data/geocoding/what-is-geocoding.htm) to get these co-ordinates from the [Bing Maps](https://www.bing.com/maps/) [API](https://www.microsoft.com/en-us/maps/choose-your-bing-maps-api)
4. `4_get_stats_by_neighbourhood.ipynb` ([view](https://nbviewer.org/github/elsdes3/dinesafe-inspections/blob/main/4_get_stats_by_neighbourhood.ipynb))
   - get the city of Toronto neighbourhood containing each inspected establishment and neighbourhood-based statistics (population, crimes committed, etc.)
5. `6_eda.ipynb` ([view](https://nbviewer.org/github/elsdes3/dinesafe-inspections/blob/main/6_eda.ipynb))
   - exploratory data analysis
6. `7_feat_engineering.ipynb` ([view](https://nbviewer.org/github/elsdes3/dinesafe-inspections/blob/main/7_feat_engineering.ipynb))
   - engineering features to be used in ML experiments
7. `8_ml.ipynb` ([view](https://nbviewer.org/github/elsdes3/dinesafe-inspections/blob/main/8_ml.ipynb))
   - ML experiments

## [Notes](#notes)
1. A notebook with a filename ending in `_v2.ipynb` contains analysis that is in progress.

## [Project Organization](#project-organization)

    ├── LICENSE
    ├── .gitignore                    <- files and folders to be ignored by version control system
    ├── .pre-commit-config.yaml       <- configuration file for pre-commit hooks
    ├── .github
    │   ├── workflows
    │       └── main.yml              <- configuration file for CI build on Github Actions
    ├── Makefile                      <- Makefile with commands like `make lint` or `make build`
    ├── README.md                     <- The top-level README for developers using this project.
    ├── environment.yml               <- configuration file to create environment to run project on Binder
    ├── executed_notebooks
    |   └── *.ipynb                   <- executed notebooks, with output and execution datetime suffix in filename
    ├── reports                       <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   ├── figures                   <- Generated graphics and figures to be used in reporting
    ├── data
    │   ├── raw                       <- The original, immutable data dump.
    |   └── processed                 <- Intermediate (transformed) data and final, canonical data sets for modeling.
    ├── *.ipynb                       <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                                    and a short `-` delimited description, e.g. `1.0-jqp-initial-data-exploration`.
    ├── requirements.txt              <- base packages required to execute all Jupyter notebooks (incl. jupyter)
    ├── src                           <- Source code for use in this project.
    │   ├── __init__.py               <- Makes src a Python module
    |   └── workflows                 <- Scripts to run workflow of essential analysis steps.
    │   └── *.py                      <- Scripts to use in development of analysis for processing, viz., training, etc.
    ├── papermill_runner.py           <- Python functions to programmatically run notebooks.
    └── tox.ini                       <- tox file with settings for running tox; see https://tox.readthedocs.io/en/latest/

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
