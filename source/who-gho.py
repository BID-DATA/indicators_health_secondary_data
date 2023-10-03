'''
Program   : Preprocessing WHO data 
Source    : WHO Global Health Indicatirs
            https://www.who.int/data/gho/data/indicators
Author    : Laura Goyeneche 
            lauragoy@iadb.org 
            lgoyenechec5@gmail.com
Dependency: SCL-SPH
Repository: indicators_health_secondary_data
            https://github.com/BID-DATA/indicators_health_secondary_data
Objective : Proprocess indicators of interest
Notes     : This dataset can be updated manually 
            This code uses the API 
                https://www.who.int/data/gho/info/athena-api
            We use the package GHOclient to access their data programmaticaly
            Some considerations:
               For additional indicators, search codes by name and add them in the list of code of interest
               Run section Fetch data from selected codes only for the new codes, i.e. replace list indicators
               Append the new indicators to the already available dataset
               Update code to read al available datasets in the collection
               Update code to merge all available datasets from the API     
'''

# Libraries
#------------------------------------------------------------------------------
import os
import boto3
import dotenv
import numpy as np
import pandas as pd
from ghoclient import GHOSession, index

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

# Connect to API
#------------------------------------------------------------------------------
# Connection
gc = GHOSession()

# Get available codes / indicators
codes = gc.get_data_codes(format = "dataframe")
index.build_index(codes)

# Search indicators by word
# Note: example for `UHC`
search_ = index.search("UHC")
search_ = pd.DataFrame(search_)

# Preprocessing
#------------------------------------------------------------------------------
# Define list/dict of codes of interest
indicators  = ["WHOSIS_000003","nmr"]                                  # neonatal mortality
indicators += ["MDG_0000000007","u5mr","MEDS1_02_04"]                  # under-5 mortality
indicators += ["MDG_0000000001","imr","MEDS1_02_03"]                   # infant mortality
indicators += ["WHS6_102"]                                             # hospital beds
indicators += ["HWF_0001"]                                             # number of doctors
indicators += ["FINPROTECTION_CATA_TOT_10_POP"]                        # catastrophic out-of-pocket spending
indicators += ["WSH_SANITATION_BASIC","WSH_SANITATION_SAFELY_MANAGED"] # sanitation services
indicators += ["NCD_BMI_25C","NCD_BMI_25A"]                            # overweight prevalence
indicators += ["NCD_BMI_18C","NCD_BMI_18A"]                            # underweight prevalence
indicators += ["LBW_PREVALENCE"]                                       # low birth weight prevalence
indicators += ["NUTUNDERWEIGHTPREV"]                                   # underweight prevalence among children
indicators += ["NUTOVERWEIGHTPREV","NUTRITION_ANT_WHZ_NE2"]            # overweight prevalence among children
indicators += ["vmsl","WHS8_110","MCV2"]                               # measles vaccination
indicators += ["WHS4_543","vbcg"]                                      # BCG vaccination
indicators += ["UHC_INDEX_REPORTED"]                                   # UHC index
indicators += ["UHC_SCI_RMNCH"]                                        # UHC sub-index maternal
indicators += ["WHS6_102"]                                             # hospital beds
indicators += ["HWF_0001"]                                             # doctors

# Fetch data from selected codes
who_gho = []
for name in indicators:
    try:
        temp = gc.fetch_data_from_codes(code = name)
        if temp.shape[0] > 0:
            temp["display"] = codes[codes["@Label"] == name].Display.tolist()[0]
            who_gho.append(temp)
    except:
        print(f"An exception ocurred for code {name}")

# Create master dataframe
who_gho = pd.concat(who_gho)

# Keep country-level data
who_gho = who_gho[~who_gho.COUNTRY.isna()]
who_gho = who_gho[~who_gho.REGION.isna()]

# Drop variables 
vars_   = ["PUBLISHSTATE","StdErr","StdDev","Comments","Low","High"]
vars_  += ["UNREGION","UNSDGREGION","WORLDBANKREGION","UNICEFREGION","WORLDBANKINCOMEGROUP","UNICEFREGION","DHSMICSGEOREGION"] 
vars_   = [name for name in vars_ if name in who_gho.columns]

# Drop rows without numeric values
who_gho = who_gho[~who_gho.Numeric.isna()]

# Create category variable
    # Select categories
cats_   = ["SEX","AGEGROUP","EDUCATIONLEVEL","RESIDENCEAREATYPE","WEALTHQUINTILE","WEALTHDECILE"]
cats_  += ["POP_TYPE","HOUSEHOLD_COMP_BY_AGE"]
cats_   = [name for name in cats_ if name in who_gho.columns]

    # Merge categories
who_gho["CATEGORY"] = who_gho[cats_].astype(str).agg('-'.join, axis = 1)
who_gho["CATEGORY"] = who_gho.CATEGORY.str.replace("nan-","")
who_gho["CATEGORY"] = who_gho.CATEGORY.str.replace("-nan","")
who_gho["CATEGORY"] = who_gho.CATEGORY.str.replace("nan" ,"")
who_gho             = who_gho.drop(columns = cats_)

# Find latest observation
vars_ = ["GHO","REGION","COUNTRY","CATEGORY"]
who_gho["YEAR_MAX"] = who_gho.groupby(vars_).YEAR.transform("max")
who_gho["LATEST"]   = (who_gho.YEAR == who_gho.YEAR_MAX).astype(int)

# Export data 
#------------------------------------------------------------------------------
path  = "International Organizations/World Health Organization (WHO)/"
path += "Global Health Observatory (GHO)"
who_gho.to_csv(f"s3://{sclbucket}/{path}/who-gho-api.csv", index = False)
#------------------------------------------------------------------------------