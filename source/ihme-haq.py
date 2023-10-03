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
import os
import boto3
import dotenv
import numpy as np
import pandas as pd

# Working environments
#------------------------------------------------------------------------------
dotenv.load_dotenv("/home/ec2-user/SageMaker/.env")
sclbucket   = os.environ.get("sclbucket")
scldatalake = os.environ.get("scldatalake")

# Resources and buckets
#------------------------------------------------------------------------------
s3        = boto3.client('s3')
s3_       = boto3.resource("s3")
s3_bucket = s3_.Bucket(sclbucket)

# Country keys 
#------------------------------------------------------------------------------
# Path 
path = "Geospatial Basemaps/Cartographic Boundary Files/keys"

# IADB 26-LAC countries
iadb       = pd.read_csv(f"s3://{sclbucket}/{path}/iadb-keys.csv")
codes_iadb = iadb.isoalpha3.unique().tolist()

# OECD countries
oecd       = pd.read_csv(f"s3://{sclbucket}/{path}/oecd-keys.csv") 
codes_oecd = oecd.isoalpha3.unique().tolist()

# World countries
world = pd.read_csv(f"s3://{sclbucket}/{path}/world-keys.csv") 
world = world.rename(columns = {"isoalpha3":"code","country_name_en":"location_name"})
world = world.drop(columns = "income_group")

# Import data and dictionary
#------------------------------------------------------------------------------
path     = "International Organizations/Institute for Health Metrics and Evaluation (IHME)"
path    +="/Healthcare Access and Quality (HAQ) index/raw"
ihme_haq = pd.read_csv(f"s3://{sclbucket}/{path}/haq_1990_2016_scaled.csv")

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
path     = "International Organizations/Institute for Health Metrics and Evaluation (IHME)"
path    +="/Healthcare Access and Quality (HAQ) index/processed"
ihme_haq.to_csv(f"s3://{sclbucket}/{path}/haq.csv", index = False)
#------------------------------------------------------------------------------