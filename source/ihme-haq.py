'''
Program   : Preprocessing IHME data 
            Life expectancy and healthy life expectancy
Source    : IHME GHDx Results
            GBD Study 2019 Healthcare Access and Quality Index 1990-2019
            https://ghdx.healthdata.org/record/ihme-data/gbd-2019-healthcare-access-and-quality-1990-2019
Author    : Laura Goyeneche 
            lauragoy@iadb.org 
            lgoyenechec5@gmail.com
Dependency: SCL-SPH
Repository: indicators_health_secondary_data
            https://github.com/BID-DATA/indicators_health_secondary_data
Objective : Proprocess indicators of interest
Notes     : Download data manually
                Only upload the most recent year
                Check for recent data at least once a year
                Update code to read all available datasets in the collection
                Update code to merge all available datasets per source
            Upload it to the Social Data Lake 
                https://scldata.iadb.org/app?locale=es
                International Organizations/Institute for Health Metrics and Evaluation (IHME)
            Rerun notebook
'''

# Libraries
#------------------------------------------------------------------------------
import numpy as np
import pandas as pd

# Working directory
# TODO: Change to DataLake connection
#------------------------------------------------------------------------------
path  = "/Users/lauragoyeneche/Google Drive/My Drive/02-Work/10-IDB Consultant/1-Social Protection & Health"
path += "/0-Health data/health-public"

# Country keys 
#------------------------------------------------------------------------------
# IADB 26-LAC countries
iadb       = pd.read_csv(f"{path[:-14]}/iadb-keys.csv")
codes_iadb = iadb.isoalpha3.unique().tolist()

# OECD countries
oecd       = pd.read_csv(f"{path[:-14]}/oecd-keys.csv") 
codes_oecd = oecd.isoalpha3.unique().tolist()

# World countries
world = pd.read_csv(f"{path[:-14]}/world-keys.csv") 
world = world.rename(columns = {"isoalpha3":"code","country_name_en":"location_name"})
world = world.drop(columns = "income_group")

# Import data and dictionary
# TODO: Change to DataLake connection
#------------------------------------------------------------------------------
ihme_haq = pd.read_csv(f"{path}/IHME/haq/raw/haq_1990_2016_scaled.csv")

# Preprocessing
#------------------------------------------------------------------------------
# Keep only country-level data
ihme_haq = ihme_haq[np.where(ihme_haq.ihme_loc_id.isin(world.code),1,0) == 1]

# Keep variables of interest
vars_    = ["location_id","indicator_id","measure","lower","upper"]
vars_   += ["parent_location","sdi_quintile","super_region_name","region_name"]
ihme_haq = ihme_haq.drop(columns = vars_)

# Rename variables
ihme_haq = ihme_haq.rename(columns = {"ihme_loc_id":"code","year_id":"year"})

# Define regions 
ihme_haq["IADB"]   = np.where(ihme_haq.code.isin(codes_iadb),1,0)
ihme_haq["OECD"]   = np.where(ihme_haq.code.isin(codes_oecd),1,0)
ihme_haq["Global"] = 1

# Export data 
ihme_haq.to_csv(f"{path}/IHME/haq/raw/haq.csv", index = False)
#------------------------------------------------------------------------------