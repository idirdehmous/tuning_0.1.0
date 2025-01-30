import  os,sys
from    ctypes       import cdll 
from    pandas       import DataFrame , cut 
import  numpy as np
from    datetime     import datetime ,timedelta
from    collections  import defaultdict 
import  matplotlib.pyplot as plt
import  pandas as pd 
import  multiprocessing as mp 
import  time
import  gc  


# pyodb_extra  MODULES 
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.parser       import  StringParser
from pyodb_extra.odb_ob       import  OdbObject


# pyodb ENV 
odb_install_dir=os.getenv( "ODB_INSTALL_DIR" ) 
env= OdbEnv(odb_install_dir, "libodb.so") 
env.InitEnv () 

# pyodb MODULES (Pure C )
from pyodb  import   odbFetch
from pyodb  import   odbDca



# OBSTOOL MODULES 
from dca             import DCAFiles
from build_sql       import SqlHandler 
from cma_rows        import OdbCCMA , GatherRows
from obstype_info    import ObsType
from conv_stats      import DHLStat 
from handle_df       import SplitDf ,ConcatDf  ,GroupDf ,Binning  
from flush_data      import DataIO



class Setting:
    def __init__ (self ):
        

        self.sat      = Satem() 
        self.conv     = Conv ()

        self.conv_obs = self.conv.conv_obs
        self.sat_obs  = self.sat.sat_obs 
        return None 

    def set_period(self, bdate , edate , cycle_inc=3 ):
        # CREATE DATE/TIME LIST
        if not isinstance (bdate, str) or not isinstance ( edate , str):
           btype, etype =   type(bdate ) , type( edate)
           print("Start and end dates period argument must be strings. Got {}  {} ".format( btype, etype ) )
           sys.exit(0)

        if len( bdate) != 10: print("Malformatted start date") ; sys.exit(1)
        if len( edate) != 10: print("Malformatted end   date") ; sys.exit(2)
        period=[]
        bdate =datetime.strptime( bdate , "%Y%m%d%H")
        edate =datetime.strptime( edate , "%Y%m%d%H")
        delta =timedelta(hours=int(cycle_inc))        
        while bdate <= edate:
              strdate=bdate.strftime("%Y%m%d%H")
              period.append( strdate )
              bdate += delta
        return period 




    def set_obs_list(self, obstypes , obs_kind ):      
        if len(obstypes ) == 0 :
          print("Can not process empty observation list." )
          sys.exit(1)

        if not isinstance ( obstypes  , list):
          print("Observation types variable expects list." )
          sys.exit(2)

        
        for obs in obstypes:
            if obs.strip() not in self.conv_obs and obs_kind == 'conv':
               print("Can t process the given obstype {}. Not in predefined list of conventional obstypes".format( obs ))
               sys.exit(3)
            if obs.strip() not in self.sat_obs  and obs_kind == 'satem': 
               print("Can t process the given obstype {}. Not in predefined list of Satem obstypes".format( obs ))  
               sys.exit(3)

        types             = ObsType ()
        if  obs_kind == 'conv':
            obs_dict ,  _     = types.ConvDict()
            obs_list =[]
            for obs in obs_dict:
                if obs["obs_name"] in obstypes:
                   
                   obs_list.append( obs )
            
            # Obs + codetype or varno ( used as keys  for data tracking  )
            varobs , _     = types.SelectConv (obs_list )
            return varobs, obs_list 


        elif obs_kind == 'satem':            
            sat_obs, sat_name, sat_name_dict, sens_name_dict ,sensor_dict    = types.SatDict()
            # Build varobs list  
            # FOR SATEM : obs_name_sensor  ( e.g  amsua_3)
            obs_list=[]
            for  obs in  sat_obs:                
                if obs["obs_name"] in obstypes:
                   obs_list.append( obs  )
            varobs , _       = types.SelectSat (obs_list )
            return varobs , obs_list 







# DATA CETEGORIES (CONV , SATEM )

# What we need as obstype   
#         SATEM 
# NAME         : sensor 
# AMSUA        : 3
# AMSUB        : 4
# MHS          :15 
# IASI         :16  
# ATMS         :19  
# MWHS         :73  
# SEVIRI       :29 
class Satem:
    def __init__(self):
        
        # --------------------------------------------------#
        # ---> CAUTION !: NEVER CHANGE THE ORDER OF COLUMNS 
        #      TO ADD NEW ONEs , ONE CAN ONLY DO AN APPEND !
        #---------------------------------------------------#
        self.cols      =[ "",
                          "obstype"                 ,  # 1
                          "codetype"                ,  # 2
                          "statid"                  ,  # 3
                          "varno"                   ,  # 4
                          "degrees(lat)"            ,  # 5
                          "degrees(lon)"            ,  # 6 
                          "vertco_type"             ,  # 7
                          "vertco_reference_1"      ,  # 8
                          "sensor"                  ,  # 9
                          "date"                    ,  # 10
                          "time"                    ,  # 11
                          "an_depar"                ,  # 12
                          "fg_depar"                ,  # 13
                          "obsvalue"                ,  # 14
                         ]

        # Considered tables & additional sql statement 
        # ODB_CONSIDER_TABLES="/hdr/desc/body"  
        self.tables        = ["hdr","desc","body"]
        self.tbl_env       = "/".join( self.tables  )
        self.other_sql     = "(an_depar is not NULL) AND (fg_depar is not  NULL)"

        # Sat obstype list 
        self.sat_obs = [ 'amsua'  ,
                         'amsub'  ,
                         'atms'   ,
                         'iasi'   ,
                         'mwhs'   ,
                         'mwhs2'  ,
                         'msh'    ,
                         'seviri' ]
        return None





# CONV OBSTYPE LIST
#  obs        varno  
# gpssol :    128
# synop  :    1,29,58,41,42,7,39
# dribu  :    
# airep  :    2,3,4
# airepl :    2,3,4
# radar  :    29,195
# temp   :    2,3,4,39,58,41,42
# templ  :    2,3,4,39,58,41,42 
class Conv:
    def __init__(self ):
         
        # ---> CAUTION !: NEVER CHANGE THE ORDER OF COLUMNS 
        #      TO ADD NEW ONEs , ONE CAN DO ONLY AN APPEND !
        self.cols      =[ "",
                          "obstype"                 ,  # 1
                          "codetype"                ,  # 2
                          "statid"                  ,  # 3
                          "varno"                   ,  # 4
                          "degrees(lat)"            ,  # 5
                          "degrees(lon)"            ,  # 6 
                          "vertco_reference_1"      ,  # 7
                          "date"                    ,  # 8
                          "time"                    ,  # 9
                          "an_depar"                ,  # 10
                          "fg_depar"                ,  # 11
                          "obsvalue"                ,  # 12
                         ]

        # Considered tables & additional sql statement 
        # ODB_CONSIDER_TABLES="/hdr/desc/body"  
        self.tables        = ["hdr","desc","body"]
        self.tbl_env       = "/".join( self.tables  )
        self.other_sql     = "(an_depar is not NULL) AND (fg_depar is not  NULL)"

        # ObsType list 
        self.conv_obs=[ 'gpssol' ,
                        'synop'  ,
                        'dribu'  ,
                        'airep'  ,
                        'airepl' ,
                        'radar'  ,
                        'temp'   ,
                        'templ'  ]

        return None






class ExtractData:
    def __init__(self , dbpath , obs_kind  ):

       self.odb_path=dbpath                                                                
       # Path to the directory containing the ODB(s)
       self.odb_path = dbpath    
       if  not os.path.isdir (self.odb_path): print("ODB(s) directory '{}' not found.".format( self.odb_path )) ; sys.exit(0)

       self.obs_kind=obs_kind 
       self.st      = Setting  ()
       self.conv_obs= None 
       self.sat_obs = None 
       self.cols    = None 
       self.tables  = None 

       if obs_kind == 'satem':
          self.sat      = Satem() 
          self.sat_obs  = self.sat.sat_obs 
          self.cols     = self.sat.cols  
          self.tables   = self.sat.tables 
          self.other_sql= self.sat.other_sql

       elif  obs_kind == 'conv':
          self.conv      = Conv()
          self.conv_obs  = self.conv.conv_obs
          self.cols      = self.conv.cols 
          self.tables    = self.conv.tables
          self.other_sql = self.conv.other_sql
       else:
          print("Missing or unknown bservation category. Possible values :  'conv'  or 'satem'")
          sys.exit()

            
       # SQL      
       self.ccma  =OdbCCMA  ()
       self.sql   =SqlHandler()


    def _OdbRows  (self,  cdtg , dobs ,chunk_size , nprocs , vrb , obs_kind , sema  ):
        # Obstool needs only CCMA 
        ccma_path ="/".join( [self.odb_path , cdtg  , "CCMA"] ) 

        # Check DCA directory  (if not there they will be created )
        dca_f=DCAFiles()
        dca_f.CheckDca ( ccma_path  )


        # If level range requiered or not 
        llev     =False 
        lev_range=dobs["level_range"]
        if lev_range != None :  
           llev=True 
           if vrb ==1 :
              print("Observation type {} has level selection {}".format( dobs["obs_name"],  lev_range  ))
        
        # BUILD & CHECK sql query 
        query=self.sql.BuildQuery(        columns       =self.cols  ,
                                          tables        =self.tables,
                                          obs_dict      =dobs       ,
                                          has_levels    =llev       ,
                                          vertco_type   ="height"   ,   
                                          remaining_sql =self.other_sql )

        
        if vrb in [1, 2 , 3 ]: 
           print ("ODB date         :" ,   cdtg  ) 
           print ("Getting rows for :" ,   dobs["obs_name"] ,"   ...")

        varobs=[]                
        if self.sat_obs !=None:
           if dobs["obs_name"]  in self.sat_obs:
              if isinstance( dobs["sensor"], list) and len(dobs["sensor"]) != 0 :
                 for v in dobs["sensor"]:
                     varobs.append( dobs["obs_name"]+"_"+str(v) )
              elif isinstance ( dobs["sensor"],   int ):
                  varobs.append( dobs["obs_name"]+"_"+str(dobs["sensor"])  )
              else:
                  print("Sensor value must be an integer or list of integers")              
                  sys.exit(1)


        elif self.conv_obs !=None:
           if dobs["obs_name"]  in self.conv_obs:
              if isinstance( dobs["varno"], list) and len(dobs["varno"]) != 0 :
                 for v in dobs["varno"]:
                     varobs.append( dobs["obs_name"]+"_"+str(v) )
           elif isinstance ( dobs["varno"],   int ):
                varobs.append( dobs["obs_name"]+"_"+str(dobs["varno"])  )
           else:
                print("Varno value must be an integer or list of integers")              
                sys.exit(1)


        # UPDATE ODB_SRCPATH & ODB_DATAPATH 
        os.environ["IOASSIGN"]=ccma_path+"/IOASSIGN"
        os.environ["ODB_SRCPATH_CCMA"] =ccma_path
        os.environ["ODB_DATAPATH_CCMA"]=ccma_path
        os.environ["ODB_IDXPATH_CCMA" ]=ccma_path
        if vrb == [ 2 , 3]:
           print("ODB PATHS set to  :")
           print("IOASSIGN          :",ccma_path+"IOASSIGN" )
           print("ODB_SRCPATH_CCMA  :",ccma_path )
           print("ODB_DATAPATH_CCMA :",ccma_path )
           print("ODB_IDXPATH_CCMA  :",ccma_path )

        self.ccma.FetchByObstype (                   obs_kind    = obs_kind , 
                                                     varobs      = varobs  , 
                                                     dbpath      =ccma_path , 
                                                     sql_query   =query     ,
                                                     sqlfile     =None      ,
                                                     pools       =None      ,
                                                     progress_bar=True     , 
                                                     get_header  =False     ,                                 
                                                     return_rows =True      ,
                                                     nchunk      =chunk_size, 
                                                     nproc       =nprocs    ,
                                                     datetime    =cdtg , 
                                                     verbosity   =vrb  
                                                      )
        sema.release()





    def get_odb_rows  (self,     period,      obs_list,
                                              obs_kind     = None , 
                                              cycle_inc    =3 , 
                                              reprocess_odb=True, 
                                              chunk_size   =None , 
                                              nprocs       =None , 
                                              verbosity    =0):


        #for obs in obs_list:
        #    if   obs["obs_name"]  in self.sat_obs  and obs_kind =='conv':
        #         print("The argument 'obs_kind' should be  'satem'. The observation list must contain either convential or Satem obs kind not the both ! ")
        #         sys.exit(0)
        #    elif obs["obs_name"]  in self.conv_obs  and obs_kind =='satem':
        #         print("The argument 'obs_kind' should be  'conv'. The observation list must contain either convential or Satem obs kind not the both ! ")
        #         sys.exit(0)


        vrb=verbosity
        if vrb not in [0,1,2,3]:
           print("Minimum and maximum verbosity levels: 0 ->  3.  Got :  ", vrb )
           sys.exit(0)
        if chunk_size==None: 
           chunk_size=4
           if vrb in [1,2]:
              print( "The chunk size= None ,  reset it to 4")
        if nprocs    ==None: 
           nprocs    = 4      # Means processes not cpus !!
           if vrb in [1 ,2]:
              print( "The number of processes is set to None.  Reset it to {} ".format( nprocs   ))

        types  = ObsType ()
        sat_list, _ , _ ,sens_name_dict, sensor_dict = types.SatDict()

        if reprocess_odb   ==True:              # Extract rows again if new obstype  is added for example  !
           max_concurrency = 8                  # --> 1 Days ( if cycle inc == 3 protected by semaphore Lock !)
           procs=[]
           sema = mp.Semaphore(max_concurrency)
           ncpu =mp.cpu_count() 
           for dobs in obs_list:
               for cdtg in period:
                   obsname=dobs["obs_name"]
                   bin_files=self.odb_path+"/"+cdtg+"/bin/"+cdtg+"/"+obsname+"_*.bin"
                   os.system("rm -rf "+bin_files )
                   if vrb in [ 2, 3]:
                      print( "Process observation type {} ODB date {} ".format( dobs["obs_name"], cdtg   ))
                   sema.acquire()
                   p=mp.Process (target =self._OdbRows  , args=(cdtg  , dobs, chunk_size, nprocs , verbosity , obs_kind , sema) )
                   p.start()
                   procs.append(p)
               for p in procs:
                   p.join()
               procs=[]



    def dist_matrix  (self , period , varobs ):
        # Gather all small chunks in  *.tmp files 
        gt=GatherRows(self.obs_kind )  
        procs=[]
        for var in varobs:
            for cdtg in period: 
                print( "Compute ditances matrix.   Date: {} ,   parameter: {}".format(   cdtg,   var   ))
                p=mp.Process (target =gt.Rows2Array  , args=(self.odb_path , cdtg, self.obs_kind,  var,) )
                p.start()
                procs.append(p)
            for p in procs:
                p.join()



