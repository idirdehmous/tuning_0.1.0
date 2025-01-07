# -*- coding: utf-8 -*-
import  os
import  sys
import  re 
import  random  
from    datetime  import datetime 
import  numpy as np 
from    itertools import *
import  multiprocessing  as mp 
import  resource  
from    collections import defaultdict , Counter 
import  pandas      as pd  
import  gc   


# pyodb  MODULES 
from pyodb_extra.environment  import  OdbEnv

odb_install_dir=os.getenv("ODB_INSTALL_DIR")

# INIT ENV 
env= OdbEnv(odb_install_dir , "libodb.so")
env.InitEnv ()

# pyodb modules 
from pyodb   import  odbFetch


# obstool MODULES 
from build_sql    import  SqlHandler
from obstype_info import  ObsType 
from flush_data   import  DataIO 





class MpRequest:
    __slots__ = ['dbpath', 
                 'query' , 
                 'varobs', 
                 'obs_kind', 
                 'cdtg'  ,
                 'types' ,
                 'varno_dict',
                 'obs_type'  ,
                 'verb'  ,
                 'd_io']



    def __init__(self, dbpath , sql_query , obs_kind, varobs , dte , verb ):
        self.dbpath  = dbpath 
        self.query   = sql_query
        self.varobs  = varobs 
        self.obs_kind= obs_kind  
        self.cdtg    = dte 
        self.verb    = verb 

        # This can also be changed by env variable
        # export ODB_MAXHANDLE=N 
        os.environ["ODB_MAXHANDLE"]= "1000"
        self.types   = ObsType ()
        if self.obs_kind ==  'conv':
            _    , self.varno_dict  = self.types.ConvDict()
        elif self.obs_kind=='satem':
            sat_list, _ , _ ,sens_name_dict, sensor_dict = self.types.SatDict()

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



    def AlterQuery ( self, sql_string , obs_kind ):
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

        if obs_kind == 'conv' :  pattern      ="varno"
        if obs_kind == 'satem':  pattern      ="sensor"

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
        progress  =False 
        pool      =None 
        fmt_float =None
        verbose   =False 
        header    =False 
        if self.verb == 2: 
           progress =False  
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
        if self.verb == 2:
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
       
        if len(rows) != 0:
           W_OK=self.d_io.CheckWrite( self.dbpath    )
           # It can happen to get 'NULL' even th condition in the SQL statement 
           lats= ( row[4]   for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )
           lons= ( row[5]   for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )
           an_d= ( row[9 ]  for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )
           fg_d= ( row[10]  for row in rows if row[9] != 'NULL' and row[10] != 'NULL' )

           df=self.Gen2Df ( lats , lons , an_d ,fg_d  )

           # Set a unique file id  ( combine random and time.microseconds  )
           # The probability to get two identical ids is ----->>>0 
           rand= "{:08}".format(random.randint (  1,  9999999 ) )
           dt  = "{:08}".format(datetime.now().microsecond )

           fid = rand+"_"+dt

           if W_OK ==True :
              db_path=os.path.dirname(self.dbpath )
              self.d_io.FlushFrame (  df , db_path , "bin", self.cdtg ,  varname, fid, ".bin"  )
        else:
           print("Request returned no data for chunk.{}->{}  Date :{},   Parameter:{}  ".format(sq1, sq2,cdtg , varname ) )
        del rows


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
       seqno_qr , data_qr =self.AlterQuery (self.query, self.obs_kind  )

    
       for sq , dq , vr in zip ( seqno_qr, data_qr ,self.varobs ):
           seq , entry  = self.Seqno (sq)
           seqno_chunks=self.Chunk(  seq, nchunk    )
           
           # Reset ncpu to 1 if nchunk =1 
           if nchunk ==1 :   nproc = 1 

           sq_sets    =[]
           for sq in seqno_chunks:
               if isinstance ( sq , list ):
                  sq_sets.append( [sq[0], sq[-1]]   )

               if len( sq_sets )==0 or nchunk == 1: 
                  if len(seqno_chunks ) !=0 :
                     sq_sets.append(min(seqno_chunks) )
                     sq_sets.append(max(seqno_chunks) )                 
  
           # Create processes pool     ( processes not ODB pools !!!)
           if nchunk >1    and  nproc  > 1 :   # Parallel
              varname =  self.types.RenameVarno (vr  , self.obs_kind   )              
              if self.verb in [1,2,3]:
                 print("Process rows by chunks. ODB date: {}, Parameter: {},  Nrows: {}, Nchunks: {}".format( cdtg ,varname,len(seq),len(sq_sets) ))
              with mp.Pool(processes=pool_size , maxtasksperchild=200 ) as pl:
                   pl.starmap(self.SelectBySeqno  ,[ (dq, seq[0], seq[1],cdtg,  varname ) for  seq in sq_sets]    )
              self.ClosePool(pl )
   

    def ClosePool (self, pool ):
        pool.close()
        pool.terminate()
        pool.join()
        del pool 
        return None 

