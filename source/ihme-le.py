'''
Program   : Preprocessing IHME data 
            Life expectancy and healthy life expectancy
Source    : IHME GBD Results
            https://vizhub.healthdata.org/gbd-results/
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
path      = "International Organizations/Institute for Health Metrics and Evaluation (IHME)/"
path     += "Global Burden of Disease (GBD)"

ihme_le   = pd.concat([pd.read_csv(f"s3://{sclbucket}/{path}/raw/ihme-gbd-le-{i}.csv")     for i in [1,2,3]])
ihme_hale = pd.concat([pd.read_csv(f"s3://{sclbucket}/{path}/raw/ihme-gbd-hale-{i}.csv") for i in [1,2,3]])
ihme_le   = pd.concat([ihme_le, ihme_hale])
ihme_dict = pd.read_csv(f"s3://{sclbucket}/{path}/codebook/ihme-location-2019.csv")

# Preprocessing
#------------------------------------------------------------------------------
# Keep only country-level data
ihme_le = ihme_le.merge(ihme_dict[["location_id","level"]], on = "location_id", how = "left")
ihme_le = ihme_le[ihme_le.level == 3]

# Drop columns
vars_   = ["measure_id","location_id","sex_id","age_id","metric_id","metric_name","upper","lower","level"]
ihme_le = ihme_le.drop(columns = vars_)

# Add world codes
ihme_le = ihme_le.merge(world, on = "location_name", how = "left")

# Rename measure
label_ = {"Life expectancy":"Life expectancy at birth","HALE (Healthy life expectancy)":"Healthy Life Expectancy at birth"}
ihme_le.measure_name = ihme_le.measure_name.replace(label_)

# Define regions 
ihme_le["IADB"]   = np.where(ihme_le.code.isin(codes_iadb),1,0)
ihme_le["OECD"]   = np.where(ihme_le.code.isin(codes_oecd),1,0)
ihme_le["Global"] = 1

# Create group values
group_ = []
for group in ["IADB","OECD","Global"]:
    
    # Collapse data
    temp_ = ihme_le.copy()
    temp_ = temp_.groupby(["measure_name","sex_name","age_name","year",group]).mean().reset_index()
    temp_ = temp_[temp_[group] == 1]
    temp_ = temp_.drop(columns = ["IADB","OECD","Global"])

    # Add lables
    temp_["location_name"] = group
    temp_["code"]          = group
    
    # Append to list of groups
    group_.append(temp_)

# Append to dataset
ihme_le = pd.concat([ihme_le] + group_)

# Drop columns 
ihme_le = ihme_le.drop(columns = ["IADB","OECD","Global"])

# Export data 
ihme_le.to_csv(f"s3://{sclbucket}/{path}/processed/ihme-gbd-le-hale.csv", index = False)
#------------------------------------------------------------------------------













