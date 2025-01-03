#-*- coding : utf-8 -*- 
import os 
import pandas as pd 
from   glob import glob 
import gc 

class DataIO: 
    def __init__(self):
        return None 

    def CheckWrite (self, file_path):
        if os.access(file_path, os.W_OK):           
           return True 
        else:
           return False 

    def CheckRead (self, file_path):
        if os.access(file_path, os.R_OK):
           return True
        else:
           return False




    def FlushFrame (self,   df ,dbpath ,  subdir , cdtg , var, p_id , ext ):
        """
        Since the program is relatively long, it becomes sometimes 
        slow to free memory and cosumes a lot of resources 
        Flushing data between some steps can helps to avoid such a behavior 
        """
        if p_id !=None:
           pkpath= dbpath+"/"+subdir+"/"+cdtg
           os.system( "mkdir -p "+ pkpath   )
           filepath="/".join(  (  pkpath , var+"_"+cdtg+"_xz"+str(p_id)+ext ) )
           try:
             df.to_pickle( filepath     , compression ="xz"  )
           except:
             Exception 
             print("Error while writing data to",filepath  )
        else:
           pkpath= dbpath+"/"+cdtg+"/"+subdir+"/"+cdtg
           os.system( "mkdir -p "+ pkpath   )
           filepath="/".join(  (  pkpath , var+"_"+cdtg+"_xz"+ext ) )
           try:
             df.to_pickle( filepath     , compression ="xz"  )
           except:
             print("Error while writing data to",filepath  )
        del df  
        gc.collect()
        return None 



    def LoadFrame (self,  dbpath , subdir,  cdtg , var, ext ):
        dfs=[]
        pkpath= dbpath+"/"+cdtg+"/"+subdir+"/"+cdtg
        filepath="/".join((  pkpath , var+"_"+cdtg+"_xz*"+ext ) )
        files=glob( filepath  )

        for file in files:
            if os.path.isfile( file ):
               try:
                 data=pd.read_pickle(file, compression='xz', storage_options=None) 
               except:
                 print("WARNING : Error while reading file", filepath )

               dfs.append( data  )
        if len(dfs)  != 0:
           all_df=pd.concat( dfs )
           return all_df  
        else:
           return None 

