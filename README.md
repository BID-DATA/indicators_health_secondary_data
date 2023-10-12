# Health indicators from secondary data

## Index: 
- [Description](#description)
- [Structure](#structure)
- [Data source](#data-source)
- [Author(s)](#authors)
- [Citation](#citation)

## Description 
This repository contains a collection of code for preprocessing secondary data for health in Latin America and the Caribben (LAC). The repository aims to facilite data analysis and exploration by providing modules (.py) for preprocessing various datasets related to the health indicators. This repository's **main objetive** is to create a centralized and continuosly updated resource for preprocessing and analyzing health indicators in the region. By fostering collaboration and knowledge sharing, the repository aims to support research, analysts, and policymakers in conducting in-depth geospatial analysis and generating valuable insights for the region. 

This repository includes scripts and instructions for obtaining the following datasets: 

1. Life expectancy and healthy life expectancy from [IHME GBD Results](https://vizhub.healthdata.org/gbd-results/)

2. Healthcare Access and Quality Index 1990-2019 from [IHME GHDx Results](https://ghdx.healthdata.org/record/ihme-data/gbd-2019-healthcare-access-and-quality-1990-2019)

3. Global Health Expenditure Database from [WHO GHED](https://apps.who.int/nha/database)

4. Global Health Indicators from [WHO GHO](https://www.who.int/data/gho/data/indicators)

## Structure
The repository offers a range of code examples and modules to assist users in conducting data analysis on health indicators from secondary sources. These examples demonstrate retrieving and preprocessing each required dataset, ensuring a seamless workflow for researchers. This repository workflow is (so far) divided into 4 modules:

- [ihme-le.py](https://github.com/BID-DATA/indicators_health_secondary_data/blob/main/source/ihme-le.py): step-by-step to create a dataset with life expectancy and healthy life expectancy by country, sex, age group, and year. 

- [ihme-haq.py](https://github.com/BID-DATA/indicators_health_secondary_data/blob/main/source/ihme-haq.py): step-by-step to create a dataset with the Healthcare Access and Quality Index by country, condition category, and year. 

- [who-ghed.py](https://github.com/BID-DATA/indicators_health_secondary_data/blob/main/source/who-ghed.py): step-by-step to create a data with health expenditure indicators by country, unit of measure, and year. 

- [who-gho.py](https://github.com/BID-DATA/indicators_health_secondary_data/blob/main/source/who-gho.py): step-by-step to fetch indicators from the package GHOclient by country, category (i.e., sex, income group), and year. 

The previous .py show the step-by-step to preprocess the analysis described. In addition, we included [SCL-indicators.py](https://github.com/BID-DATA/indicators_health_secondary_data/blob/main/source/scl-indicators.py), a file that generates the health indicators used in the SCL Country Profiles. We also include a [notebook](https://github.com/BID-DATA/indicators_health_secondary_data/blob/main/examples/dashboard-figures.ipynb) that show examples of visualizations with the health indicators. Users can leverage this example to perform a variety of analysis and dashboard. 

## Data source
> Life expectancy: [International Organizations/Institute for Health Metrics and Evaluation (IHME)](https://scldata.iadb.org/app/folder/6A7ABB29-6A5D-4EDC-BFBD-BF8DF8C0DAAA)

> HAQ: [International Organizations/Institute for Health Metrics and Evaluation (IHME)](https://scldata.iadb.org/app/folder/1C07C56B-4717-4970-B016-9F812276B28C)

> Health expenditure: [International Organizations/World Health Organization (WHO)](https://scldata.iadb.org/app/folder/AAA992DC-495D-4E14-A732-7CA4AF9934A6)

## Author(s)
[Laura Goyeneche](https://github.com/lgoyenec), Social Data Consultant, Inter-American Development Bank

## Citation
> "Source: Inter-American Development Bank (year of consultation), Health indicator from secondary data". We suggest to reference the date on which the databases were consulted, as the information contained in them may change. Likewise, we appreciate a copy of the publications or reports that use the information contained in this database for our records.

> All rights concerning the public datasets used from IHME and WHO belong to its owners.

## Limitation of responsibilities
The IDB is not responsible, under any circumstance, for damage or compensation, moral or patrimonial; direct or indirect; accessory or special; or by way of consequence, foreseen or unforeseen, that could arise: (i) under any concept of intellectual property, negligence or detriment of another part theory; (ii) following the use of the digital tool, including, but not limited to defects in the Digital Tool, or the loss or inaccuracy of data of any kind. The foregoing includes expenses or damages associated with communication failures and / or malfunctions of computers, linked to the use of the digital tool.
