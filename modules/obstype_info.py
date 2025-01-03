import sys , os 




class ObsType:
 """
  Class: Contains the  necessary obs list to run 
         obstool , Jarvinen and Desroziers diags 
         NOT FINISHED !
         MAYBE IT HAS TO BE SPLITTED INTO (Conv and Sat) !
 """

 def _init__(self):
     # ObsType lists 
     self.conv_obs=[    'gpssol' ,
                        'synop'  ,
                        'dribu'  ,
                        'airep'  ,
                        'airepl' ,
                        'radar'  ,
                        'temp'   ,
                        'templ'  ]

     self.sat_obs = [ 'amsua'  ,
                         'amsub'  ,    
                         'atms'   ,   
                         'iasi'   ,
                         'mwhs'   ,
                         'msh'    ,
                         'seviri' ]

     return None 

 def ConvDict(self  , obs_type ="conv" ) :
    self.obs_type=obs_type.strip()
       
    _obs=['conv', 'sat']
    if self.obs_type not in _obs :
       print( "Observaton type is None or not satallite neither conventional. Possible argument 'conv', 'sat'")
       sys.exit(1)

    # CONVENTIONAL  
    self.obs_conv=[

         {"obs_name"            : "gpssol",
           "obstype"           : 1  ,
           "codetype"          : 110 , 
           "varno"             : 128  ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None         } ,
 
 
         { "obs_name"          :  "synop",
           "obstype"           : 1 ,
           "codetype"          : [11, 14, 170, 182] ,
           "varno"             : [1, 42, 41, 58, 39],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None          },
 
         { "obs_name"          : "dribu",
           "obstype"           : 4 ,
           "codetype"          : None,
           "varno"             : [1, 39, 41, 42],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None          },
 
         { "obs_name"          : "ascat",
           "obstype"           : 9 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None           } ,
 
        {  "obs_name"          : "radar",
           "obstype"           : 13,
           "codetype"          : None,
            "varno"             : [29, 195],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None            },
 
        {  "obs_name"          : "airep",
           "obstype"           :  2 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4] ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None           },
 
        {  "obs_name"          : "airepl" ,
           "obstype"           : 2 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : [25000, 35000] },
 
         { "obs_name"          : "temp",
           "obstype"           : 5,
           "codetype"          : None,
           "varno"             : [2, 3, 4, 7],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None           },
 
         { "obs_name"          : "templ" ,
           "obstype"           : 5 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4, 7] ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : [40000, 60000]} ]
     
    self.varno_dict ={ 128 : "ztd"  ,  # gpssol Zenith total delay 
                       1   :  "z"   ,  # geopotential 
                       42  :  "u"   ,  # 10m wind speed u
                       41  :  "v"   ,  # 10m wind speed v
                       58  :  "h"   ,  # 2 relative humidity
                       39  :  "t"   ,  # 2 meter temperature 
                       2   :  "t"   ,  # T temperature       (upper air)
                       3   :  "u"   ,  # U component of wind (upper air)
                       4   :  "v"   ,  # V component of wind (upper air)
                       7   :  "q"   ,  # specific humidity 
                       29  :  "rh"  ,  # upper rh (radar)
                      195  :  "dw"     # Dopp radial wind 
                      }

    return self.obs_conv  , self.varno_dict
 



 def SatDict(self ):   
    # SATELLITE      
    self.obs_sat  = [ 
         { "obs_name"          : "amsua",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 3,
           "level_range"       : None     },
 
         { "obs_name"          : "amsub",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 4 ,
           "level_range"       : None     },
 
         { "obs_name"          : "msh",
           "obstype"           :  7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : "15 ",
           "level_range"       : None     },
 
         { "obs_name"          : "iasi",
           "obstype"           : 7,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 16,
           "level_range"       : None     },
 
         { "obs_name"          : "atms",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 19 ,
           "level_range"       : None     },
 
          {"obs_name"          : "mwhs" ,
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 73 ,
           "level_range"       : None      },
 
         { "obs_name"          : "seviri",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 29,
           "level_range"       : None      }   ]






    # Satellite names and attributes 
    # The satellite will  be "tracked" by sat_id !
    # Names  
    self.sat_name    =["NOAA-19","NOAA-18","MetOp-B",
                       "MetOp-A","METOP-C","MET-11" ,
                       "S-NPP"  ,"NOAA-20","NOAA-21",
                       "FY-3D"  ,"Meteosat-10"]
    # id --> names 
    self.sat_name_dict ={ 
                     223:"NOAA-19", 
                     209:"NOAA-18", 
                       3:"MetOp-B", 
                       4:"MetOp-A", 
                       5:"METOP-C", 
                      70:"MET-11", 
                     224:"S-NPP", 
                     225:"NOAA-20", 
                     226:"NOAA-21", 
                     523:"FY-3D", 
                     57 :"Meteosat-10" }

    # sensor --> sensor names
    self.sens_name_dict  = { 3:"amsua",
                             4:"amsub",
                            15:"mhs"  ,
                            16:"iasi" ,
                            19:"atms" ,
                            73:"mwhs" ,
                            29:"seviri" }


    # We use the sensor as short name                   
    self.sensor_dict={ 3   :  "3"  ,# AMSUA        : 3
                       4   :  "4"  ,# AMSUB        : 4
                      15   :  "15" ,# MHS          :15
                      16   :  "16" ,# IASI         :16
                      19   :  "19" ,# ATMS         :19
                      73   :  "73" ,# MWHS         :73 
                      29   :  "29"  # SEVIRI       :29
                      }


    if   self.obs_type== "conv":
       return self.obs_conv  , self.varno_dict

    elif self.obs_type =="sat":
       return self.sat_name,  self.sat_name_dict,  self.sens_name_dict ,  self.sensor_dict 


 def RenameVarno (self, string_var ):
     dict_={}
     obslist , dict_   = self.ConvDict()
     obsname=[  dobs["obs_name"] for dobs in obslist  ]
     name      =    string_var.split("_")[0]
     vr        =int(string_var.split("_")[1] )
     vname     =name + "_"+ dict_[vr]
     return vname 



 def SelectObs(self, list_   ):
     varobs=[]
     self.list          = list_
     obs_list, var_dict = self.ConvDict()
     for lst in self.list  :
         for k, v in lst.items():
             code =lst["codetype"]
             varno=lst["varno"]
             if  code == None and varno == None:
                 if lst["obs_name"] not in varobs:
                    varobs.append( lst["obs_name"] )
                 else:
                    continue 
             elif isinstance( code ,list) and isinstance( varno  ,list):  #  ( list, list)
                  for c in code:
                      for v in varno :
                          #obsId = lst["obs_name"]+"_c"+str(c)+"_v"+str(v) 
                          obsId = lst["obs_name"]+"_"+var_dict[v]
                          if obsId  not in varobs:
                             varobs.append( obsId   )
             elif isinstance( code , list ) and isinstance( varno  ,int):  # (list,  int
                  for c  in code:
                          #obsId = lst["obs_name"]+"_c"+str(c)+"_v"+str(lst["varno"] ) 
                          obsId = lst["obs_name"]+"_"+var_dict[varno] 
                          if obsId  not in varobs:
                             varobs.append( obsId  )
             elif isinstance( code , int  ) and isinstance( varno  , list ): # (int , list)
                  for v   in varno:
                        #obsId =lst["obs_name"]+"_c"+str(lst["codetype"])+"_v"+str(v)
                        obsId =lst["obs_name"]+"_"+var_dict[v]
                        if obsId not in varobs:
                           varobs.append( obsId )
             elif isinstance ( code, int )  and isinstance ( varno , int  ):  # (int , int ) 
                  #obsId = lst["obs_name"]+"_c"+str(code)+"_v"+str(varno)
                  obsId = lst["obs_name"]+"_"+var_dict[varno]
                  if obsId not in varobs:
                     varobs.append( obsId )
             elif isinstance( code ,list) and varno == None:                 #  (list , None )
                  for c in code:
                      obsId = lst["obs_name"]+"_c"+str(c) 
                      if obsId not in varobs:
                         varobs.append(obsId) 
             elif code ==None and  isinstance( varno  , list ):      # ( None , list ) 
                  for v in varno:
                      obsId =lst["obs_name"]+"_"+var_dict[v]
                      if obsId not in varobs:
                         varobs.append(obsId) 
             elif code ==None and  isinstance( varno  , int ):      # (None , int )
                  obsId = lst["obs_name"]+"_"+var_dict[lst["varno"] ]
                  if obsId not in varobs:
                     varobs.append(  obsId )
             #elif isinstance( code ,int ) and varno == None:        # (int , None ) 
             #     obsId =lst["obs_name"]+"_c"+str(lst["codetype"] ) 
             #     varobs.append ( obsId  )
     #for obs in self.attrib:
     #    obs_dict= { k:v  for k , v in  zip ( self.keys, obs  )  }
     #    dict_list.append( obs_dict  )


     return  varobs 





