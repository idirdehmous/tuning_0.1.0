#-*-coding:utf-8 -*- 
import os 
from multiprocessing import  cpu_count

import pandas as pd 
import numpy as np 
from   collections import defaultdict 

from   handle_df  import  SplitDf , GroupDf ,ConcatDf 

        
class DHLStat:
    """
    Class :  Compute the diffrent statistics
             covariance, correlation and standard deviations 
             from FG1, FG2 , OA1 , OA2 in observation space 
             After the method of 
             Desroziers , Hollingsworth,Lonneberg ( DHL )
    """

    def __init__(self, df  , var ,new_max_dist =100, new_bin_dist =10 , delta_t =60 ):
        
        
        # STATS
        # GET THE STATS IN THE MIDDLE OF THE BIN SQUARE  (the maximum distance and bin interval could be changed !!)
        # DEFAULT VALUES IN class 
        self.dist_max=100 #Km
        self.bin_int =10  #Km

        # OVERWRITE IF DIFFERENT 
        print("**********    DESROZIERS , HOLLINGSWORT-LÖNNBERG  Statistics.    Parameter : {}  **********".format( var   ))
        if new_max_dist != 100:
           self.dist_max =  new_max_dist
           print("New value for maximum distance has been set. Maximum value for distance =" , self.dist_max )
        else:
            print("Default max distance     :{} Km".format( self.dist_max ))

        if new_bin_dist != 10 : 
           self.bin_int =  new_bin_dist
           print("New value for maximum distance has been set. bin_interval=" , self.bin_int)
        else:
            print("Default binning interval     :{} Km".format( self.dist_max ) )

        self.gp       =GroupDf ()
        self.merged_df=df 
        return None 


    def getCov(self , inplace=None  ):
        d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list  =self.gp.DataByBins (self.merged_df ,  self.dist_max  ,self.bin_int )

        # Nobs 
        nobs=np.asarray(dobs )

        # Varname 
        # HL (Holingsworth-LÖnnberg )
        t1 =  np.divide  (d5 ,dobs) 
        t2 =  np.divide  ( np.multiply(d2 ,d3  ) , np.power( dobs, 2 ))
        cov_hl=  np.subtract(t1, t2  )

        # Desroziers B 
        tb1 = np.divide( d4, dobs )
        tb2 = np.divide( np.multiply(d1, d3   ) , np.power(dobs, 2 ) )
        drb_ =np.subtract( tb1, tb2 )
        cov_drB =np.subtract(cov_hl , drb_  )

        # Desroziers R 
        tr1 = np.divide  ( d4 , dobs  )
        tr2 = np.divide  (np.multiply( d1 , d3 ) , np.power(dobs , 2 ) )
        cov_drR = np.subtract(tr1 , tr2 )
        


        df_cov =  pd.DataFrame ({ 
                                  "nobs"    : nobs     , 
                                  "COV_HL"  : cov_hl   ,
                                  "COV_DR-B": cov_drB  ,
                                  "COV_DR-R": cov_drR  } )

        if inplace ==True :
           return cov_hl , cov_drB, cov_drR  # inside class 
        else: 
           return df_cov  
        

    def getSigma ( self ,  inplace =None ):

        d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list  =self.gp.DataByBins (self.merged_df ,  self.dist_max  ,self.bin_int )

        # Nobs 
        nobs = np.asarray( dobs )
        # SIGMA FG1
        st1=np.divide( d6 , dobs)
        st2=np.divide( d2 , dobs)**2
        sigma_fg1=np.sqrt(  np.subtract(st1, st2 ))
        del st1 , st2 

        # SIGMA FG2 
        st1=np.divide( d7 ,dobs )
        st2=np.divide( d3 ,dobs )**2
        sigma_fg2=np.sqrt( np.subtract(st1, st2 ))
        del st1, st2

        # SIGMA A1
        st1=np.divide( d8 ,dobs )
        st2=np.divide( d1 ,dobs )**2
        sigma_a1=np.sqrt( np.subtract(st1, st2  )) 
    
        df_sig =pd.DataFrame({ 
                               "nobs"     : nobs       ,
                               "dist"     : dist_list[1:]  ,
                               "sigma_fg1": sigma_fg1  ,
                               "sigma_fg2": sigma_fg2  ,
                               "sigma_a1" : sigma_a1
                                })
        if inplace ==True:
           return sigma_fg1 , sigma_fg2, sigma_a1    # inside class 
        else:
           return df_sig


    def getCor (self  ,  inplace=None ):
        _,_, _, _, _, _, _, _  , dobs , dist_list  =self.gp.DataByBins (self.merged_df , self.dist_max  ,self.bin_int )
        cov_hl, cov_drB, cov_drR =self.getCov  (  inplace =True )
        sfg1 , sfg2 , sa1        =self.getSigma(  inplace =True )

        nobs=np.asarray( dobs )

        # CORRELATIONS
        cor_hl =np.divide( cov_hl , np.multiply( sfg1 , sfg2  ) )
        cor_drB=np.divide( cov_drB, np.multiply( sfg1 , sfg2  ) )
        cor_drR=np.divide( cov_drR, np.multiply( sa1  , sfg2  ) )


        df_cor=pd.DataFrame({ 
                "nobs"     : nobs       ,
                "dist"     : dist_list[1:]  ,
                "COR_HL"   : cor_hl     ,
                "COR_DR-B" : cor_drB    ,
                "COR_DR-R" : cor_drR
                } )


        if inplace ==True:
           return cor_hl , cor_drB , cor_drR
        else:
           return df_cor 


    def getStatFrame (self):
        _,_, _, _, _, _, _, _  , dobs , dist_list  =self.gp.DataByBins (self.merged_df, self.dist_max  ,self.bin_int )
        nobs=np.asarray(dobs )
        cov_hl, cov_drB, cov_drR     =self.getCov  ( inplace =True )
        cor_hl, cor_drB, cor_drR     =self.getCor  ( inplace =True )
        sigma_fg1,sigma_fg2,sigma_a1 =self.getSigma( inplace =True )
       
        drhl_frame={ "nobs"     :nobs , 
                     "dist"     :dist_list[1:],
                     "COV_HL"   :cov_hl       , 
                     "COV_DR-B" :cov_drB      ,
                     "COV_DR-R" :cov_drR      ,
                     "sigma_FG1":sigma_fg1    ,
                     "sigma_FG2":sigma_fg2    ,
                     "sigma_OA1":sigma_a1     ,
                     "COR_HL"   :cor_hl       , 
                     "COR_DR-B" :cor_drB      , 
                     "COR_DR-R" :cor_drR
                  }

        stat_frame =pd.DataFrame (   drhl_frame  )    # To Be Plotted   !!
        return stat_frame 
