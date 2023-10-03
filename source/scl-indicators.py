'''
Program   : Health data for country profile
Source    : Multiple
Author    : Laura Goyeneche 
            lauragoy@iadb.org 
            lgoyenechec5@gmail.com
Dependency: SCL-SPH
Repository: indicators_health_secondary_data
            https://github.com/BID-DATA/indicators_health_secondary_data
Objective : Create dataset with indicators for SCL country profile
Notes     : Edit dictionary manually 
            First commit of dictionary used code, for further updates, manually
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

# IADB 26-LAC countries keys 
#------------------------------------------------------------------------------
iadb       = pd.read_csv(f"{path[:-14]}/iadb-keys.csv")
codes_iadb = iadb.isoalpha3.unique().tolist()

# Import data 
#------------------------------------------------------------------------------
who_ghed = pd.read_csv(f"{path}/WHO/GHE/GHED_data_processed.csv")
ihme_le  = pd.read_csv(f"{path}/IHME/gbd/expectancy/ihme-gbd-le-hale.csv")
ihme_haq = pd.read_csv(f"{path}/IHME/haq/raw/haq.csv")
who_gho  = pd.read_csv(f"{path}/WHO/GHO/who-gho-api.csv")

# Preprocessing
#------------------------------------------------------------------------------
# WHO GHED
who_ghed_ = who_ghed.copy()
who_ghed_ = who_ghed_.drop(columns = ["country","var_name"])
who_ghed_ = who_ghed_[who_ghed_.code.isin(codes_iadb)]
who_ghed_ = who_ghed_.rename(columns = {"code":"isoalpha3","var_code":"indicator"})
who_ghed_["iddate"] = "year"
who_ghed_["source"] = "WHO GHED"
who_ghed_["idgeo"]  = "country"

# IHME LE-HALE
ihme_le = ihme_le.drop(columns = ["location_name"])
ihme_le = ihme_le[ihme_le.code.isin(codes_iadb)]
ihme_le = ihme_le.rename(columns = {"measure_name":"indicator","sex_name":"sex","age_name":"age","val":"value","code":"isoalpha3"})
ihme_le["iddate"] = "year"
ihme_le["source"] = "IHME GBD"
ihme_le["idgeo"]  = "country"
ihme_le.indicator = ihme_le.indicator.replace({'Life expectancy at birth':"lexp",'Healthy Life Expectancy at birth':'hale'})

# IHME HAQ 
ihme_haq = ihme_haq.drop(columns = ["location_name"])
ihme_haq = ihme_haq[ihme_haq.code.isin(codes_iadb)]
ihme_haq = ihme_haq.rename(columns = {"code":"isoalpha3","indicator_name":"indicator","val":"value"})
ihme_haq = ihme_haq[ihme_haq.indicator == "Healthcare Access and Quality Index"]
ihme_haq["iddate"] = "year"
ihme_haq["source"] = "IHME HAQ"
ihme_haq["idgeo"]  = "country"
ihme_haq.indicator = "HAQ" 

# WHO GHO
who_gho_ = who_gho.copy()
who_gho_.GHO = who_gho_.GHO.str.lower()
vars_    = ['mdg_0000000007','whs6_102','hwf_0001','wsh_sanitation_basic','ncd_bmi_25a','lbw_prevalence']
vars_   += ['whs4_543','uhc_index_reported','uhc_sci_rmnch','finprotection_cata_tot_10_pop']
who_gho_ = who_gho_[who_gho_.GHO.isin(vars_)]
who_gho_.CATEGORY = who_gho_.CATEGORY.fillna("TOTL")
who_gho_ = who_gho_[who_gho_.CATEGORY.isin(["BTSX","TOTL","BTSX-YEARS18-PLUS","MLE","FMLE"])]
who_gho_.CATEGORY = who_gho_.CATEGORY.str.replace("BTSX-YEARS18-PLUS","BTSX")
who_gho_["sex"] = np.where(who_gho_.CATEGORY == "FMLE","Female","")
who_gho_["sex"] = np.where(who_gho_.CATEGORY == "MLE" ,"Male"  ,who_gho_["sex"])
who_gho_["sex"] = np.where(who_gho_.CATEGORY == "TOTL","Both"  ,who_gho_["sex"])
who_gho_["sex"] = np.where(who_gho_.CATEGORY == "BTSX","Both"  ,who_gho_["sex"])
who_gho_ = who_gho_[["GHO","YEAR","COUNTRY","sex","Numeric"]]
who_gho_ = who_gho_.rename(columns = {"GHO":"indicator","YEAR":"year","COUNTRY":"isoalpha3","Numeric":"value"})
who_gho_["iddate"] = "year"
who_gho_["source"] = "WHO GHO"
who_gho_["idgeo"]  = "country"
who_gho_.indicator = who_gho_.indicator.str.lower()
who_gho_ = who_gho_.drop_duplicates()

# Append and export master data
#------------------------------------------------------------------------------
# Append
health = pd.concat([who_ghed_, ihme_le, ihme_haq, who_gho_])
health = health[["iddate","year","idgeo","isoalpha3","source","sex","age","indicator","value"]]

# Export dataset
health.to_csv(f"{path}/indicators_health.csv", index = False)
#------------------------------------------------------------------------------

'''
# Temporary dataframe with labels
#------------------------------------------------------------------------------
ghed    = who_ghed[["var_code","var_name"]]
ghed    = ghed.rename(columns = {"var_code":"indicator","var_name":"label_en"})
gho     = who_gho[["GHO","display"]]
gho     = gho.rename(columns = {"GHO":"indicator","display":"label_en"})
labels_ = pd.concat([ghed, gho])
labels_ = labels_.drop_duplicates()
labels_ = labels_.reset_index(drop = True)
labels_.indicator = labels_.indicator.str.lower()

# Create dictionary 
health_dict = health.copy()
health_dict = health_dict.drop(columns = ["iddate","year","idgeo","isoalpha3","sex","age","value"])
health_dict = health_dict.drop_duplicates()
health_dict = health_dict.reset_index(drop = True)
health_dict["collection"] = "International Organizations"
health_dict["resource"]   = "International Organizations Indicators"
health_dict["theme_en"]   = "health"
health_dict["theme_es"]   = "salud"
health_dict               = health_dict.merge(labels_, on = "indicator", how = "left")
health_dict["label_en"]   = np.where(health_dict.indicator == "lexp","Life expectancy at birth"        , health_dict.label_en)
health_dict["label_en"]   = np.where(health_dict.indicator == "hale","Healthy Life Expectancy at birth", health_dict.label_en)
health_dict["label_en"]   = np.where(health_dict.indicator == "HAQ" ,"Health Access and Quality index" , health_dict.label_en)
health_dict                   = health_dict[["collection","resource","source","theme_en","theme_es","indicator","label_en"]]
health_dict["label_es"]       = ""
health_dict["description_en"] = ""
health_dict["description_es"] = ""
health_dict["value_type"]     = ""
health_dict["categories"]     = ""

# Export dictionary
# TODO: Add label_es and description_en/_es manually 
health_dict.to_csv(f"{path}/dictionary_health.csv", index = False)
'''