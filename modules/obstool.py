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
#from dist_matrix     import gcDistance
from flush_data      import DataIO


# What we need as obstype   
#          CONV 
# GNSS         
# SYNOP  TYPE  : 11=surface,14=surface+automatic, 21=ships,24=ships+automatic,  varno : 39=t2m, 58=rh2m,41=u10m, 42=v10m, 1=z
# RADAR  varno :  q=29 , DOW=195
# AIREP   ALL  : t=2  , u=3 , v=4 
#     + LEVELS : vertco_reference_1:  >=25000   <=35000 (T, U , V )
# DRIBU varno  : z=1 , 42=v10m  , 41=u10m   , 39=t2m
# TEMP varno   : t=2 , u=3 , v=4 , q=7
# LEVELS       : vertco_reference_1:  >=40000     <=60000

# TO BE ADDED MAYBE IN SEPARATE class 
#         SATEM 
# NAME         : sensor 
# AMSUA        : 3
# AMSUB        : 4
# MHS          :15 
# IASI         :16  
# ATMS         :19  
# MWHS         :73  
# SEVIRI       :29 



class ExtractData:
    def __init__(self, dbpath):
        self.odb_path=dbpath 
        
        # Path to the directory containing the ODB(s)
        self.odb_path = dbpath 

        #self.obs_type = obs_type   # conv or sat   

        if  not os.path.isdir (self.odb_path): print("ODB(s) directory '{}' not found.".format( self.odb_path )) ; sys.exit(0)
        #if self.obs_type ==None: 
        #   print("Can not initialize data extraction without obs type arg. Possible values 'conv' or 'sat'") 
        #   sys.exit(0)
        
        # ONLY FOR CONV OBS 
        # Input for sql query  (specific to obstool diagnostics !)
         
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


        self.ccma=OdbCCMA   ()
        self.sql =SqlHandler() 
        return None



    def set_period(self, bdate , edate , cycle_inc=3 ):
        # CREATE DATE/TIME LIST
        if not isinstance (bdate, str) or not isinstance ( edate , str):
           btype, etype =   type(bdate ) , type( edate)
           print("Start and end dates period argument must be strings. Got {}  {} ".format( btype, etype ) )
           sys.exit(1)

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



    def set_obs_list(self, obstypes ):      
       if len(obstypes ) == 0 :
          print("Can not process empty observation list." )
          sys.exit(0)

       if not isinstance ( obstypes  , list):
          print("Observation types variable expects list." )
          sys.exit(0)

       types             = ObsType ()
       obs_dict ,  _     = types.ConvDict()
       names             = []
       for _ in obs_dict:
           for k , v in  _.items():
               if k == "obs_name":
                  names.append(v)
       
       for obs in obstypes:
          if obs not in names:
             print("Observation type '{}' not in predefined observation list".format( obs )  )
             sys.exit (3)

       # List of obsnames with short_name as suffix (  e.g  synop_z --> synop geopotential ) 
       self.selected_dicts=[]
       for obs in obs_dict:
           if  obs["obs_name"] in obstypes:
               self.selected_dicts.append( obs )

       # Obs + codetype or varno ( used as keys  for data tracking  )
       # They will figure in the final Dataframe stats 
       self.varobs = types.SelectConv ( self.selected_dicts )
       return self.selected_dicts , self.varobs



    def update_obs_list(self, list_):
        types         = ObsType ()
        obs_dict,  _  = types.ConvDict()
        keys  = []
        values= []
        names = []
        for _ in obs_dict:
           for k , v in  _.items():
               keys.append  (  k )
               values.append(  v )
               if k == "obs_name":
                  names.append(v)
        
        for ll in list_:
            if not isinstance (ll , dict ):
               print("Tne new inserted observation object must be a dictionnary")
               sys.exit (1)
            elif ll["obs_name"] not in names:
               print("The new inserted observation type not in the original obs list")
               sys.exit (2)
           
        for i, sd in enumerate(self.selected_dicts):
            for ll in list_:
                keys=list(sd.keys() )
                vals=list(sd.values()) 
                if ll["obs_name"] in vals:
                   try:
                      del self.selected_dicts[i]
                      new_dict=ll 
                      new_dict["obstype" ]=ll["obstype" ]
                      new_dict["codetype"]=ll["codetype"]
                      new_dict["varno"   ]=ll["varno"   ]
                      new_dict['sensor'  ]=ll["sensor" ]
                      new_dict['vertco_reference_1']=ll["vertco_reference_1"]
                      self.selected_dicts.append( new_dict  )
                   except:
                      KeyError
                      print("New observation dictionnary doesn't match the predefined ones in obs_dict.py module")
                      sys.exit(1)
        return self.selected_dicts 


    def _OdbRows  (self,  cdtg , dobs ,chunk_size , nprocs , vrb , sema ):
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
   
        
        if vrb in [1, 2 ]: 
           print ("ODB date         :" ,   cdtg  ) 
           print ("Getting rows for :" ,   dobs["obs_name"] ,"   ...")

        varobs=[]
        # Conv  
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
        if vrb == 2:
           print("ODB PATHS set to  :")
           print("IOASSIGN          :",ccma_path+"IOASSIGN" )
           print("ODB_SRCPATH_CCMA  :",ccma_path )
           print("ODB_DATAPATH_CCMA :",ccma_path )
           print("ODB_IDXPATH_CCMA  :",ccma_path )

        self.ccma.FetchByObstype (                  varobs      = varobs  , 
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









    def get_odb_rows  (self,  bdate , edate , obs_list,
                                            cycle_inc=3 , 
                                            reprocess_odb=True, 
                                            chunk_size=None , 
                                            nprocs    =None , 
                                            verbosity =0):
        vrb=verbosity
        if vrb not in [0,1,2,3]:
           print("Minimum and maximum verbosity levels: 0 ->  3.  Got :  ", vrb )
           sys.exit(0)
        if chunk_size==None: 
           chunk_size=4
           if vrb in [1,2]:
              print( "The chunk size= None ,  reset it to 4")
        if nprocs    ==None: 
           nprocs    = 4  # Means processes not cpus !!
           if vrb in [1,2]:
              print( "The number of processes is set to None.  Reset it to {} ".format( nprocs   ))

        period = self.set_period( bdate , edate  )
        types   = ObsType ()
        conv_list   , varno_dict = types.ConvDict()

        if reprocess_odb==True:
           max_concurrency = 8                  # --> 1 Days (  if cycle inc == 3 )
           procs=[]
           sema = mp.Semaphore(max_concurrency)
           ncpu =mp.cpu_count() 
           for dobs in obs_list:
               for cdtg in period:
                   obsname=dobs["obs_name"]
                   bin_files=self.odb_path+"/"+cdtg+"/bin/"+cdtg+"/"+obsname+"_*.bin"
#                   print( bin_files ) 
                   os.system("rm -rf "+bin_files )
                   if vrb in [1, 2]:
                      print( "Process observation type {} ODB date {} ".format( dobs["obs_name"], cdtg   ))
                   sema.acquire()
                   p=mp.Process (target =self._OdbRows  , args=(cdtg  , dobs, chunk_size, nprocs , verbosity , sema  ) )
                   p.start()
                   procs.append(p)
               for p in procs:
                   p.join()
               procs=[]

    def obs_distances (self, bdate , edate ):
        # Gather all small chunks in  *.tmp files 
        period = self.set_period( bdate , edate  )
        gt=GatherRows()  
        procs=[]
        for var in self.varobs:
            for cdtg in period: 
                p=mp.Process (target =gt.Rows2Array  , args=(self.odb_path , cdtg, var, ) )
                p.start()
                procs.append(p)
            for p in procs:
                p.join()




class Diags:
      def __init__(self):

          # DataFrame's "swiss penknife" 
          self.bin =Binning ()
          self.con =ConcatDf()
          self.spt =SplitDf ()
          return None 


      def set_period(self, bdate , edate , cycle_inc=3):
          # Replace with inherited from ExtractData    class 
          if not isinstance (bdate, str) or not isinstance ( edate , str):
             btype, etype =   type(bdate ) , type( edate)
             print("Start and end dates period argument must be strings. Got {}  {} ".format( btype, etype ) )
             sys.exit(1)

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


      def _ReadPickle (self, path , cdtg , var  ):
          filepath="/".join( (path,  var+"_"+cdtg+"_xz.pkl"    ) )
          if os.path.isfile( filepath  ):
             pickled_df   = pd.read_pickle(filepath  , compression ="xz" )  
             return pickled_df
          else:
              print("WARNING : No data found. ODB Date:{},  parameter:{}".format( cdtg , var  ) )



      def get_frames (self , bdate , edate   ,  obs_list, read_path ):
          all_df=defaultdict(list)
          types           = ObsType ()
          _  , varno_dict = types.ConvDict()
          period= self.set_period( bdate ,edate  )

          for cdtg in period:
             for dobs in obs_list:
                 if isinstance ( dobs["varno"] , list):
                    varnos = dobs["varno"]
                    for vr in varnos:
                        varname= dobs["obs_name"]+"_"+ varno_dict[ int( vr) ]                         
                        all_df[varname].append(  self._ReadPickle ( read_path+"/"+cdtg+"/pkl/"+cdtg , cdtg ,  varname))
          return all_df  




      def dhl_stats  (self, all_df , new_max_dist=100 , new_int_dist=10, delta_t=60):
          figs    =[]
          stats   =[]
          dfs     =[]    # Returned by Split 
          stat_list=[]
          for k , v   in all_df.items():
              var    = k
              
              dflist =[ d.query("dist <=  "+str(new_max_dist) )  for d in v if d is not  None  ]

              ddf    =self.con.ConcatByDate (dflist)
              cdf    =self.bin.CutByBins(ddf )
              sdf    =self.spt.SubsetDf (cdf )
              sdf["varname"] =  var 
              dfs.append([var ,sdf] )  
          

          for dl in dfs:
              var = dl[0]
              df  = dl[1]
              stat   =DHLStat (  df , var  , new_max_dist , new_int_dist , delta_t )
              st_df  =stat.getStatFrame()  
              stat_list.append({var:st_df}) 
          return    stat_list  
