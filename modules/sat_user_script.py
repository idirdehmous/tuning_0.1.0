import matplotlib.pyplot as plt
import matplotlib as mpl 
import pandas as pd
import multiprocessing as mp  
import os 

import sys 
sys.path.insert(0, "/home/idehmous/Desktop/rmib_dev/tuning/tuning_0.1.0/modules")
from  obstool_sat    import  Setting  
from  obstool_sat    import  ExtractData
from  tuning_stats   import  Diags 




# ODB PATH (Should contain CCMA as     ODB_PATH/YYYYMMDDHH/CCMA   )
odb_path="/mnt/HDS_ALD_TEAM/ALD_TEAM/idehmous/depot_tampon/METCOOP_ODB"

obs_category="conv"

# SAT OBS types list 
#'amsub'  ,
#'atms'   ,
#'iasi'   ,
#'mwhs'   ,
#'mwhs2'  ,
#'msh'    ,
# obstype =["seviri" ] 


# CONV OBS types list 
# gpssol
# synop  
# dribu  
# airep  
# airepl 
# radar  
# temp   
# templ  

obstype =["airep", "dribu", "radar" ]



# Init Objects 
st  =Setting ()
ext =ExtractData(odb_path , obs_kind =obs_category ) 
diag=Diags ()


# PERIOD 
bdate="2024010500"
edate="2024010521"

# PERIOD DATES LIST 
period=st.set_period(  bdate, edate  )
#period=["2024010506"  , "2024010606"] #,"2024010512","2024010521" ]

# List of concerned obs and varobs ( varobs = obsname_varno , or obsname_sensor )
varobs , obs_list = st.set_obs_list(obstype, obs_kind = obs_category   )

# THE BEST PERFORMANCE  :
# chunk_size= 16 
# nprocs    = 196
# Max smaphore processes = 16 
# Max processes children = 100 


#ext.get_odb_rows (period, obs_list, obs_kind=obs_category ,  reprocess_odb=True , chunk_size=16 , nprocs=196 , verbosity=1)

#ext.dist_matrix  ( period , varobs )


# Proceed to Diagnostics and plots 
frames = diag.get_frames( period  , 'conv',   obs_list  ,odb_path )

stat   = diag.dhl_stats ( frames , new_max_dist = 100, new_bin_dist= 10)
for st in stat:
    print( st )
