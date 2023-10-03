'''
Program   : Preprocessing GHED database
Source    : WHO GHED
            https://apps.who.int/nha/database
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
                International Organizations/World Health Organization (WHO)
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

# Import data and dictionary
# TODO: Change to DataLake connection
#------------------------------------------------------------------------------
who_ghed      = pd.read_excel(f"{path}/WHO/GHE/GHED_data_raw.xlsx", sheet_name = "Data")
who_ghed_dict = pd.read_excel(f"{path}/WHO/GHE/GHED_data_raw.xlsx", sheet_name = "Codebook")
who_ghed_dict = who_ghed_dict.rename(columns = {"variable code":"var_code","variable name":"var_name"})

# Preprocessing
#------------------------------------------------------------------------------
# Define variables of interest
vars_   = ["country","code","year","che_usd","gdp_usd","gdp_ppp","pop"]
govment = ["gghed","gghed_usd","gghed_ppp2020","gghed_usd2020_pc","gghed_ppp2020_pc","gghed_gdp"]
oop     = ["hf3"  ,"hf3_usd"  ,"hf3_ppp2020"  ,"hf3_usd2020_pc"  ,"hf3_ppp2020_pc"  ,"hf3_gdp"  ]
private = ["hf2"  ,"hf2_usd"  ,"hf2_ppp2020"  ,"hf2_usd2020_pc"  ,"hf2_ppp2020_pc"  ,"hf2_gdp"  ]
vars_  += govment + oop + private

# Select variables of interest
who_ghed = who_ghed[vars_]

# Drop NAs rows
who_ghed = who_ghed[~who_ghed.gdp_usd.isna()]

# Define regions 
who_ghed["IADB"]   = np.where(who_ghed.code.isin(codes_iadb),1,0)
who_ghed["OECD"]   = np.where(who_ghed.code.isin(codes_oecd),1,0)
who_ghed["Global"] = 1

# Create group values
group_ = []
for group in ["IADB","OECD","Global"]:
    
    # Collapse data
    temp_ = who_ghed.copy()
    temp_ = temp_.groupby(["year",group]).sum().reset_index()
    temp_ = temp_[temp_[group] == 1]
    temp_ = temp_.drop(columns = ["IADB","OECD","Global"])

    # Create new variables
    temp_["pop"]              = temp_["pop"] * 1000
    temp_["gghed_usd2020_pc"] = temp_["pop"] / temp_.gghed_usd
    temp_["hf3_usd2020_pc"]   = temp_["pop"] / temp_.hf3_usd
    temp_["hf2_usd2020_pc"]   = temp_["pop"] / temp_.hf2_usd
    temp_["gghed_ppp2020_pc"] = temp_["pop"] / temp_.gghed_ppp2020
    temp_["hf3_ppp2020_pc"]   = temp_["pop"] / temp_.hf3_ppp2020
    temp_["hf2_ppp2020_pc"]   = temp_["pop"] / temp_.hf2_ppp2020
    temp_["gghed_gdp"]        = temp_.gghed_usd * 100 / temp_.gdp_usd
    temp_["hf3_gdp"]          = temp_.gghed_usd * 100 / temp_.gdp_usd
    temp_["hf2_gdp"]          = temp_.gghed_usd * 100 / temp_.gdp_usd

    # Add lables
    temp_["country"] = group
    temp_["code"]    = group
    
    # Append to list of groups
    group_.append(temp_)
    
# Append to dataset
who_ghed = pd.concat([who_ghed] + group_)

# Drop columns 
who_ghed = who_ghed.drop(columns = ["IADB","OECD","Global"])

# Add variables as % of CHE
who_ghed["gghed_che"] = who_ghed.gghed_usd * 100 / (who_ghed.gghed_usd + who_ghed.hf3_usd + who_ghed.hf2_usd)
who_ghed["oop_che"]   = who_ghed.hf3_usd   * 100 / (who_ghed.gghed_usd + who_ghed.hf3_usd + who_ghed.hf2_usd)
who_ghed["pri_che"]   = who_ghed.hf2_usd   * 100 / (who_ghed.gghed_usd + who_ghed.hf3_usd + who_ghed.hf2_usd)
who_ghed["che_gdp"]   = who_ghed.che_usd   * 100 / who_ghed.gdp_usd

# Reshape dataset
id_vars_ = ["country","code","year"]
val_vars = who_ghed.columns[3:]
who_ghed = who_ghed.melt(id_vars = id_vars_, value_vars = val_vars, var_name = "var_code")

# Remove year 2021
# Notes: Not complete for all countries
who_ghed = who_ghed[who_ghed.year < 2021]

# Add description 
who_ghed = who_ghed.merge(who_ghed_dict[["var_code","var_name"]], on = "var_code", how = "left")

# Complete descriptions
who_ghed.var_name = np.where(who_ghed.var_code == "gghed_che", "Governemnt as % CHE", who_ghed.var_name)
who_ghed.var_name = np.where(who_ghed.var_code == "oop_che"  , "OOP as % CHE"       , who_ghed.var_name)
who_ghed.var_name = np.where(who_ghed.var_code == "pri_che"  , "Private as % CHE"   , who_ghed.var_name)
who_ghed.var_name = np.where(who_ghed.var_code == "che_gdp"  , "CHE as % GDP"       , who_ghed.var_name)

# Export data
who_ghed.to_csv(f"{path}/WHO/GHE/GHED_data_processed.csv", index = False)
#------------------------------------------------------------------------------