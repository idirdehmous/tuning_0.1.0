# -*- coding :utf-8 -*- 
import os ,sys  
from   ctypes        import cdll , CDLL
from   pandas        import DataFrame , cut 
from   collections   import defaultdict  
import pandas as pd 
import numpy  as np
from   datetime      import datetime
from   itertools     import  *
import re 
import gc  



# Pyodb modules 
from pyodb_extra.environment  import OdbEnv  
from pyodb_extra.parser       import StringParser 


odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()

from pyodb  import   odbFetch
from pyodb  import   odbDca


# obstool modules
from build_sql             import SqlHandler  
from multi_proc            import MpRequest 
from obstype_info          import ObsType 
from flush_data            import DataIO 
from dist_matrix           import gcDistance


class OdbCCMA:
    __slots__ = ['types'    , 'varno_dict',
                 'd_io'     , 'varobs',    'path'   ,'query' ,
                 'queryfile', 'pool'  ,    'verbose','header',
                 'pbar'     , 'header',    'rrows'  ,'nchunk',
                 'nproc'    , 'cdtg'  ,  'fmt_float' , 'verb']

    def __init__(self):
        
        self.d_io = DataIO()
        return None 

    def FetchByObstype(self, **kwarg):
        # ARGUMENT TO SEND, GET ROWS 
        args=["varobs" ,
              "dbpath"    , 
              "sql_query" , 
              "sqlfile"   ,
              "pools"     , 
              "fmt_float" , 
              "get_header", 
              "progress_bar" , 
              "return_rows",
              "nchunk"    ,
              "nproc"     ,
              "datetime"  ,
              "verbosity"   ]
        
        kargs=[] ; kvals=[]
        for k , v in   kwarg.items(): 
            if k in args:
               self.varobs   =kwarg["varobs"]
               self.path     =kwarg["dbpath"]
               self.query    =kwarg["sql_query" ]
               self.queryfile=kwarg["sqlfile"   ]    
               self.pool     =kwarg["pools"     ]
               self.header   =kwarg["get_header"]
               self.pbar     =kwarg["progress_bar"]
               self.rrows    =kwarg["return_rows"]
               self.nchunk   =kwarg["nchunk"    ]
               self.nproc    =kwarg["nproc"     ]
               self.cdtg     =kwarg["datetime"]
               self.verb     =kwarg["verbosity"]
               self.fmt_float=None 
        
            else:
              print("Unexpected argument :" , k)
        
        self.types           = ObsType ()
        _ , self.varno_dict  = self.types.ConvDict() 

        sql=SqlHandler()
        nfunc , sql_query = sql.CheckQuery(self.query)  

    
        mq=MpRequest (self.path ,  sql_query, self.varobs , self.cdtg , self.verb )       
        nchunks=self.nchunk
        nproc  =self.nproc 
        mq.DispatchQuery( cdtg=self.cdtg ,  nchunk=nchunks, nproc=nproc )


        # Gather all small chunks in  *.tmp files 
        #for var in self.varobs:
        #    arrs   = gt.Rows2Array( self.path , self.cdtg , var  )


    
class GatherRows:
      __slots__ = ['gcd', 'tp', 'varno', 'novar', 'obs_dict']
      def __init__(self  ):


          self.gcd =gcDistance ()
          self.tp =ObsType ()
          self.obs_dict ,self.varno =self.tp.ConvDict()

          # Reverse the dict 
          self.novar = { v:k for k,v in self.varno.items() }
          d_io  =DataIO ()
          return None 

   
      def Np2Df (self , cdtg ,  data_arr_gen  ) :
          df_list=[]
          colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
          for d in data_arr_gen  : 
              for var  , values in d.items():
                  df_data = pd.DataFrame( values  , columns = colnames )
                  var_col =[]
                  dte_col =[]
                  var_col.append(   [var      for i   in range(len(df_data)) ] )
                  dte_col.append(   [cdtg     for i   in range(len(df_data)) ] )
                  df_data ["var" ] =var_col[0] 
                  df_data ["date"] =dte_col[0]
          return {var:df_data }


      def Rows2Array(self , db_path , cdtg , var , sm  ):
          # LOAD save data from .tmp files 
          d_io=DataIO()
          ext=".bin"
          df_data =  d_io.LoadFrame ( db_path ,"bin", cdtg , var , ext  )                    
          if df_data  is not None:
             lats= [ row  for row in df_data["lats"]  ]
             lons= [ row  for row in df_data["lons"]  ]
             an_d= [ row  for row in df_data["an_d"]  ]
             fg_d= [ row  for row in df_data["fg_d"]  ]
         
             # Build a numpy array 
             idx=[]
             [  idx.append(i)   for i in  product(range(len(lats)) , repeat=2) ]
             d1=[ i[0] for i in idx ]
             d2=[ i[1] for i in idx ]

             an1=[i[0] for i in product(an_d , repeat=2)    ]
             an2=[i[1] for i in product(an_d , repeat=2)    ]
             fg1=[i[0] for i in product(fg_d , repeat=2)    ]
             fg2=[i[1] for i in product(fg_d , repeat=2)    ]

             # Matrix distances  (Great circle distances )
             latlon=np.array( [lats,lons] ).T
             #name      =var.split("_")[0]
             #vr        =int(var.split("_")[1] )
             #vname     =name + "_"+ self.varno[vr]

             dist=self.gcd.ComputeDistances ( latlon  , var  , chunk_size=10 )
             gcdist=list(dist.reshape(len(lats)**2 ) )
             data=[d1,d2, gcdist,  an2 , an1 , fg2, fg1  ]
             data_arr = np.array( data  ).T

             var_col =[]
             dte_col =[]
             var_col.append(   [var       for i   in range(len(d1)) ] )
             dte_col.append(   [cdtg      for i   in range(len(d1)) ] )

             colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
             df_data = pd.DataFrame( data_arr  , columns = colnames )
             df_data ["var" ] =var_col[0] 
             df_data ["date"] =dte_col[0]

             # Serialize  to pickle files 
             ext   =".pkl"
             subdir= "pkl"
             #print("Flush dataframe into pickle file.")
             #print("ODB: {}, parameter: {}, Path: {}".format(cdtg , var ,  db_path+"/"+cdtg+"/"+subdir )  )
             
             d_io.FlushFrame (df_data ,db_path  ,subdir ,  cdtg , var , None  ,  ext  )
             sm.release()
             # CLEAN Memory !
             del d1 ; del d2 ; del an1 ; del an2 ; del fg1 ; del fg2 
             del data ; del gcdist ; del dist ; del data_arr 
             gc.collect()

