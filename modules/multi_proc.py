# -*- coding: utf-8 -*-
import  os
import  sys
import  re 
import  numpy        as np 
from    itertools    import *
import  multiprocessing  as mp 
import  resource  
from    collections import defaultdict , Counter 
import pandas as pd  
import  gc   

# ODB  MODULES 
from pyodb_extra.environment  import  OdbEnv


odb_install_dir=os.getenv("ODB_INSTALL_DIR")

# INIT ENV 
env= OdbEnv(odb_install_dir , "libodb.so")
env.InitEnv ()

# --> NOW pyodb could be imported  !
from pyodb   import  odbFetch


# obstool MODULES 
from build_sql    import  SqlHandler
from obstype_info import  ObsType 
from flush_data   import  DataIO 



class MpRequest:
    __slots__ = ['dbpath', 
                 'query' , 
                 'varobs', 
                 'cdtg'  ,
                 'types' ,
                 'varno_dict',
                 'obs_type'  ,
                 'verb'  ,
                 'd_io']

    def __init__(self, dbpath , sql_query , varobs , dte , verb ):
        self.dbpath = dbpath 
        self.query  = sql_query
        self.varobs = varobs 
        self.cdtg   = dte 
        self.verb   = verb 
        # This can also be changed by env variable
        # export ODB_MAXHANDLE=N 
        os.environ["ODB_MAXHANDLE"]= "1000"
        self.types   = ObsType ()
        _    , self.varno_dict  = self.types.ConvDict()
        self.d_io=DataIO ()
        return None 



    def ParallelEnv(self):
        ncpu      = mp.cpu_count()  
        th_info   = sys.thread_info[1]
        if th_info == "semaphore":
           pass 
        if resource.getrlimit(resource.RLIMIT_CPU) == -1:
           pass 
        else:
           resource.setrlimit(resource.RLIMIT_CPU, ( -1, -1) )
        return ncpu  , th_info  




    def AlterQuery ( self, sql_string ):
        """
        Remove orginal select statement and replece 
        with seqno and entryno and ORDER them by seqno  
        """
        rr=sql_string.lower().split()                # BE SURE THAT THE QUERY IS IN LOWER CASE
        rj=" ".join(rr).partition('from')            # JOIN EVERYTHING AND USE "from" KEYWORD AS SEPARATOR
        from_token = rj[1:]                          # THE SELECT STETEMENT IS AT INDEX 0

        rwhere =" ".join(from_token).split('and')
        seqno_query  =[]
        data_query   =[]
        seqno_select ="SELECT seqno,entryno "
        sql_order    ="ORDER BY seqno"
        pattern="varno"
        for i , item in enumerate ( rwhere):
           var_  , no  =re.findall ( pattern+'\s*==\s*' ,item  ),   re.findall(   pattern+'\s*==\s*(\d+)' , from_token[1] )    
           if len( var_ ) !=0  and len(no) !=0 and len(var_)  == len(no) :
              del rwhere[i]
              for st, v in zip ( var_ , no  ):
                  r= " and ".join( rwhere    )
                  seqno_query.append(  seqno_select+ " "+r +" and "+ st +v + " "+sql_order)     
                  data_query.append(   rj[0]       + " "+r +" and "+ st +v  )

        if len(seqno_query ) ==0:
           seqno_query.append( sql_string )
           data_query.append( sql_string )
        return seqno_query, data_query 



    def Flatten(self, list_):
        "Flatten one level of nesting."
        return chain.from_iterable(list_)



    def Chunk( self, lst , chunk_size   ):
        if len(lst)    > chunk_size  and chunk_size >=2 :   # nchunks should be 2 at least
           return [lst[i:i+chunk_size] for i in range(0, len(lst) , int(chunk_size ) )]
        else:
           return lst

  
    def Seqno (self  , sql_seqno ):
        # Get the rows sequence number in ODB 
        query_file=None 
        nfunc     =0 
        progress  =True
        pool      =None 
        fmt_float =None
        verbose   =False 
        header    =False 
        if self.verb == 2: 
           progress =True 
        try:
           seq_rows  =odbFetch( self.dbpath ,sql_seqno  , nfunc  ,query_file ,pool ,fmt_float,progress, verbose  , header)
           seqno     =[item [0]   for item in seq_rows ]
           entryno   =[item [1]   for item in seq_rows ]
           if self.verb in [2, 3 ]:
              print( "Fetch sequence numbers . Done !" )
           return seqno, entryno 
        except:
           Exception 
           print("Failed to fetch seqno number for the query: ",sql_seqno )
           sys.exit (1)


    def SelectBySeqno   (self ,  query,  sq1, sq2, cdtg , varname ):
        pp=os.getpid() 
        #if self.verb == 2:
        print("Get ODB rows by seqno. {} to {}       process ID : {}".format( sq1, sq2, pp  ) )
        nfunc    = 2
        query_file=None 
        pool     = None           
        float_fmt= None 
        progress = False 
        verbose  = False   
        header   = False 
        seqno_cond =" AND seqno >="+str(sq1) +" AND "+" seqno <= "+str(sq2)
        query=query+" "+seqno_cond
        
        rows=odbFetch(self.dbpath, 
                            query, 
                            nfunc,
                       query_file, 
                           pool  , 
                       float_fmt ,
                        progress , 
                         verbose , 
                          header     )

        #if len(rows) != 0:
        #   W_OK=self.d_io.CheckWrite( self.dbpath    )
           # It can happen to get 'NULL' even th condition in the SQL statement 
        #   lats= ( row[4]   for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )
        #   lons= ( row[5]   for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )
        #   an_d= ( row[9 ]  for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )
        #   fg_d= ( row[10]  for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )

        #   df=self.Gen2Df ( lats , lons , an_d ,fg_d  )
        #   if W_OK ==True :
        #      db_path=os.path.dirname(self.dbpath )
        #      self.d_io.FlushFrame (  df , db_path , "bin", self.cdtg ,  varname  ,pp  , ".bin"  )
        #else:
        #   print("Request returned no data for chunk.{}->{}  Date :{},   Parameter:{}  ".format(sq1, sq2,cdtg , varname ) )

        #del rows
        return rows 


    def Gen2Df  (self ,  lats , lons , an , fg   ):
        """
        Write only needed data into files 
        
        Since the program is relatively long and there is sometimes 
        huge data amount, it is better flushing the data variables.
        """
        df_depar =pd.DataFrame({ "lats" : lats   , 
                                 "lons" : lons  , 
                                 "an_d" : an    , 
                                 "fg_d" : fg   
                                })
        df_depar["lats"]= df_depar["lats"].astype("float32")
        df_depar["lons"]= df_depar["lons"].astype("float32")
        df_depar["an_d"]= df_depar["an_d"].astype("float32")
        df_depar["fg_d"]= df_depar["fg_d"].astype("float32")
        return df_depar


    def DispatchQuery(self ,  cdtg ,  nchunk , nproc ):     
       
       # N proc pools 
       # Set default parameters 
       ncpu     =self.ParallelEnv()[0]
       pool_size=nproc 


       # Split into chunks
       seqno_qr , data_qr =self.AlterQuery (self.query )

    
       for sq , dq , vr in zip ( seqno_qr, data_qr ,self.varobs ):
           seq , entry  = self.Seqno (sq)
           seqno_chunks=self.Chunk(  seq, nchunk    )
           
           # Reset ncpu to 1 if nchunk =1 
           if nchunk ==1 :   nproc = 1 

           sq_sets    =[]
           is_sublist =False 
           for sq in seqno_chunks:
               if isinstance ( sq , list ):
                  sq_sets.append( [sq[0], sq[-1]]   )
                  is_sublist =True 

           if is_sublist==False:
               if len( sq_sets )==0 or nchunk == 1:    # Simple list no sublist
                  if len(seqno_chunks ) !=0 :
                     sq_sets.append(min(seqno_chunks) )
                     sq_sets.append(max(seqno_chunks) )                  
           elif seqno_chunks ==None:
               is_sublist =False 
               print("Failed to split the seqno list into chunks " )
               sys.exit (0)

           # Run in prallel pools (    processes not ODB pools !!!)
           if nchunk >1    and  nproc  > 1 :   # Parallel
              varname =  self.types.RenameVarno (vr    )
              print( dq )
              if self.verb in [ 1,2,3]:
                 print("Process rows by chunks.  ODB date: {},  Number of chunks: {}, Parameter: {}".format( cdtg , len(sq_sets)  , varname  ))
  
              with mp.Pool(processes=pool_size,  maxtasksperchild=100 ) as _pool:
                  out= _pool.starmap(self.SelectBySeqno  ,[ (dq, seq[0], seq[1],cdtg,  varname ) for  seq in sq_sets]    )
              _pool.close()
              _pool.join() 
    
    def ClosePool (self, pool ):
        pool.close()
        pool.join()
        del pool 
        return None 




"""class GatherRows:
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


      def Rows2Array(self , db_path , cdtg , var  ):
          # LOAD save data from .tmp files 
          d_io=DataIO()
          ext=".tmp"
          varname =  self.tp.RenameVarno ( var )
          df_data =  d_io.LoadFrame ( db_path , cdtg , varname , ext  )                    

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
          name      =var.split("_")[0]
          vr        =int(var.split("_")[1] )
          vname     =name + "_"+ self.varno[vr]

          dist=self.gcd.ComputeDistances ( latlon  , vname  , chunk_size=10 )
          gcdist=list(dist.reshape(len(lats)**2 ) )
          data=[d1,d2, gcdist,  an2 , an1 , fg2, fg1  ]
          data_arr = np.array( data  ).T

          var_col =[]
          dte_col =[]
          var_col.append(   [vname     for i   in range(len(d1)) ] )
          dte_col.append(   [cdtg      for i   in range(len(d1)) ] )

          colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
          df_data = pd.DataFrame( data_arr  , columns = colnames )
          df_data ["var" ] =var_col[0] 
          df_data ["date"] =dte_col[0]

          # Serializer  to pickle files 
          ext=".pkl"
          pkpath=os.path.dirname (db_path)
          #if self.verb in [1,2]:
          #   pkpath=os.path.dirname(self.path )+"/pkl/"+self.cdtg
          #   print("Flush dataframe into pickle file. ODB date: {} , parameter :{}".format( self.cdtg, varname ) )
          #   print("Path : {}".format( pkpath )  )

          d_io.FlushFrame (df_data , pkpath  , cdtg , vname , None  ,  ext  )
#         yield {vname:data_arr }
          # CLEAN Memory !
          del d1 ; del d2 ; del an1 ; del an2 ; del fg1 ; del fg2 
          del data ; del gcdist ; del dist ; del data_arr 
          gc.collect()"""






