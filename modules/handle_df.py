#-*-coding:utf-8 -*- 
import pandas as pd 
import numpy  as np 



class Binning:
    def __init__(self):
        return None 

    def CutByBins  (self, ndf   ,  bin_max_dist=100 , bin_int=10 ):
        lDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int ))
        cDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int))
                                              
        # Partitions over bins inplace !
        # Binning by  bin_int  Km 
        dbin   =[0,1]+lDint
        dlabel =[0  ]+cDint           
        dbin_serie   =pd.cut(  ndf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
        nndf = ndf.copy()  # Create a copy explicitly               
        nndf["dbin"] = dbin_serie[0]                               
        return nndf  



class SplitDf:
    """
        Class : Contains methods to split and group 
                the rows from distance dataframe 


    """
    def __init__ (self):
        return None 

    def _mulDf(sefl,  df1 ,  df2 ):
        df=  np.multiply(df1, df2)
        return df

     
    def DeparturesDf ( self , ndist_df, max_dist=100 ):

        self.ndist_df =ndist_df 
        self.max_dist =max_dist 

        ndf    =self.ndist_df.query("dist <= "+str(  self.max_dist  )  )
        a1f2   =self._mulDf ( ndf.OA1  ,ndf.FG2    ) 
        f1f2   =self._mulDf ( ndf.FG1  ,ndf.FG2    ) 
        f1f1   =self._mulDf ( ndf.FG1  ,ndf.FG1    )
        f2f2   =self._mulDf ( ndf.FG2  ,ndf.FG2    )
        a1a1   =self._mulDf ( ndf.OA1  ,ndf.OA1    )

        # AFGsqr  FGsqr num    FGsqr1  FGsqr2    Asqr
        df_frame = {
                      "d1"     :ndf.d1   ,
                      "d2"     :ndf.d2   ,
                      "dist"   :ndf.dist ,
                      "dbin"   :ndf.dbin ,
                      "OA1"    :ndf.OA1  ,
                      "OA2"    :ndf.OA2  ,
                      "FG1"    :ndf.FG1  ,
                      "FG2"    :ndf.FG2  ,
                      "AFGsqr" :a1f2, 
                      "FGsqr"  :f1f2,
                      "FGsqr1" :f1f1, 
                      "FGsqr2" :f2f2,
                      "Asqr1"  :a1a1  
                   }

        stat_df=pd.DataFrame( df_frame   )
        return stat_df  


    def SubsetDf (self ,df ,  max_dist =100 , bin_int=10):
        self.max_dist  = max_dist
        self.bin_int   = bin_int

        stat_df=self.DeparturesDf(df , self.max_dist )

        dbin   = [0,1]+list(np.arange(self.bin_int, self.max_dist + self.bin_int , self.bin_int ))
        dlabel = [0  ]+list(np.arange(self.bin_int, self.max_dist + self.bin_int , self.bin_int ))
        # DIVIDE BY DIST INTERVALS 
        pd.cut( stat_df['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
   

        # NOBS  & DISTS 
        nobs  = stat_df.groupby     (["dbin"], observed=True )["dist"].count()
        ldist = list(stat_df.groupby(["dbin"], observed=True) ["dbin"].groups.keys())
   
 
        # sum(AO1)  sum(FG1)    sum(FG2)
        # AFGsqr  ->  OA1*FG2
        # FGsqr   ->  FG1*FG2
        # FGsqr1  ->  FG1*FG1
        # FGsqr2  ->  FG2*FG2
        # Asqr1   ->  OA1*OA1

        oa1_sum  = stat_df.groupby( ["dbin"],observed=True  ) ["OA1"   ].sum().reset_index()
        fg1_sum  = stat_df.groupby( ["dbin"],observed=True  ) ["FG1"   ].sum().reset_index()
        fg2_sum  = stat_df.groupby( ["dbin"],observed=True  ) ["FG2"   ].sum().reset_index()
        a1f2_sqrt= stat_df.groupby( ["dbin"],observed=True  ) ["AFGsqr"].sum().reset_index()
        f1f2_sqrt= stat_df.groupby( ["dbin"],observed=True  ) ["FGsqr" ].sum().reset_index()
        f1f1_sqrt= stat_df.groupby( ["dbin"],observed=True  ) ["FGsqr1"].sum().reset_index()
        f2f2_sqrt= stat_df.groupby( ["dbin"],observed=True  ) ["FGsqr2"].sum().reset_index()
        a1a1_sqrt= stat_df.groupby( ["dbin"],observed=True  ) ["Asqr1" ].sum().reset_index()



        frame_={"nobs"  :list(nobs)   ,
                "dist"  :list(ldist ) ,
                "Asum1" :oa1_sum.OA1  ,
                "Fsum1" :fg1_sum.FG1  ,
                "Fsum2" :fg2_sum.FG2  ,
                "AFGsqr":a1f2_sqrt.AFGsqr ,
                "FGsqr" :f1f2_sqrt.FGsqr  ,
                "FGsqr1":f1f1_sqrt.FGsqr1 ,
                "FGsqr2":f2f2_sqrt.FGsqr2 ,
                "Asqr1" :a1a1_sqrt.Asqr1
                     }
        spdf=pd.DataFrame ( frame_)
        return spdf 


class ConcatDf:
    def __init__(self ):
        return None 

    def ConcatByDate (self, dfl  ):
        if isinstance (dfl , list ):
           merged_df =pd.DataFrame()
           print( dfl  ) 
           merged_df =pd.concat(dfl)
           return merged_df
        else:
           return dfl 



class GroupDf:
    def __init__(self  ):
        return None 

    def DataByBins (self, merged_df  , new_max_dist=100  , new_bin_int=10 ):
        self.max_ndist    =100
        self.bin_ninterval=10 

        if new_max_dist != 100:
           self.max_ndist =  new_max_dist
        if new_bin_int != 10 :
           self.bin_ninterval =  new_bin_dist

        d_bins    = [ int(i) for i in   np.arange(0,self.max_ndist     + self.bin_ninterval    ,self.bin_ninterval )  ]
        d_label   = [ int(i) for i in   np.arange(self.bin_ninterval/2 , self.max_ndist+self.bin_ninterval  -(self.bin_ninterval/2) , self.bin_ninterval)  ]

     
        # DIVIDE BY DIST INTERVALS 
        sd=pd.cut( merged_df ['dist'], bins=d_bins , labels=d_label , right=True, include_lowest=False)
        merged_df ["dist"]=sd
        
        # Distances 
        dist_list =sd.values 
        
        # Asum1      Fsum1      Fsum2     AFGsqr      FGsqr      FGsqr1      FGsqr2       Asqr
        d1  =merged_df.groupby(["dist"],observed=True)["Asum1" ].sum()
        d2  =merged_df.groupby(["dist"],observed=True)["Fsum1" ].sum()
        d3  =merged_df.groupby(["dist"],observed=True)["Fsum2" ].sum()
        d4  =merged_df.groupby(["dist"],observed=True)["AFGsqr"].sum()
        d5  =merged_df.groupby(["dist"],observed=True)["FGsqr" ].sum()
        d6  =merged_df.groupby(["dist"],observed=True)["FGsqr1"].sum()
        d7  =merged_df.groupby(["dist"],observed=True)["FGsqr2"].sum()
        d8  =merged_df.groupby(["dist"],observed=True)["Asqr1" ].sum()
        dobs=merged_df.groupby(["dist"],observed=True)["nobs"  ].sum()
        return d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list 



