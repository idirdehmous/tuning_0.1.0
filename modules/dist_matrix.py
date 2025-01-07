#-*- coding :utf-8 -*- 
import os  , sys 
import multiprocessing as mp 
#import multiprocessing.dummy  as mp
import numpy           as np 
import haversine  as hav 
import gc   

import logging

logging.basicConfig(level=logging.DEBUG)


class gcDistance:
      def __init__(self  ):
          self.R = 6378.137
          return None 

      def Haversine(self, lat1, lon1, lat2, lon2):
          R = self.R   # Radius of Earth in kilometers
          lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])  # Convert to radians
          dlat = lat2 - lat1
          dlon = lon2 - lon1
          a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
          c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
          return R * c


      def Distances_in_chunk(self, chunk_data, full_data, start_idx, chunk_size):
          """
          Compute great circle distances between all points in a chunk with all other points.
          Arguments:
          - chunk_data: Data for the current chunk (latitudes and longitudes).
          - full_data: Full dataset for computing distances across chunks.
          - start_idx: Starting index of the chunk in the full data.
          - chunk_size: The number of points in the chunk.
          Returns:
          - A distance matrix chunk for the given chunk.
          
          """
          pp=os.getpid() 
          distances_chunk = np.zeros((chunk_size, len(full_data)))

          llat2 = full_data [:, 0]
          llon2 = full_data [:, 1]
          
          lat1=[ item[0] for item   in chunk_data ]
          lon1=[ item[1] for item   in chunk_data ]
        
          npoint=len(llat2)

          try:
             # Assuming hav.gcdist is the correct method to compute the distance between points
             dist = [
                 list(hav.gcdist(lat1, lon1, llat2, llon2, npoint ))
                 for lat1, lon1, lat2, lon2 in zip(lat1, lon1, llat2, llon2)
                        ]
             return start_idx ,  np.array(dist )
          except Exception as e:
         #    # Catch the exception and return an error message or a None value
              logging.error(f"Error in worker function Distances_in_chunk  : {e}")
              return None  # Or some error flag like "error"

      def ComputeDistances(self, cdtg , var ,  latlon , chunk_size=200   ):
          """
          Compute the full distance matrix for all points in the data using chunks and multiprocessing.
          Arguments:
          - data: Latitudes and longitudes of points (n_samples, 2).
          - chunk_size: Number of samples to process in each chunk.
          Returns:

          - A distance matrix of shape (n_samples, n_samples).
          """
          nodata=False 
          if len(latlon) ==0:
             print("WARNING: No data found to compute distances matrix. Date: {}  parameter: {}".format( cdtg ,  var))
             nodata=True 

          if not nodata:
              npoint = latlon.shape[0]
              print( "Compute ditances matrix.   Date : {},  parameter: {},   {} x {} points".format(  cdtg ,  var , npoint,npoint )) 
          n_samples = len(latlon)
          distance_matrix = np.zeros((n_samples, n_samples))

          # Split the data into chunks for parallel computation
          p2=mp.Pool( processes= chunk_size  )
          results=[]

          procs=[]
          for start_idx in range(0, n_samples, chunk_size):
              chunk_end_idx = min(start_idx + chunk_size, n_samples)
              chunk_data    = latlon[start_idx:chunk_end_idx]

              #out=p2.apply_async( self.Distances_in_chunk,(chunk_data, latlon , start_idx, chunk_end_idx - start_idx)  )    
              out=p2.starmap( self.Distances_in_chunk,  [(chunk_data, latlon , start_idx, chunk_end_idx - start_idx, )]  )[0]
              start_idx , distances_chunk= out[0], out[1]
              distance_matrix[start_idx:start_idx+distances_chunk.shape[0], :] = distances_chunk
          p2.close()
          p2.terminate ()
          p2.join() 
          return distance_matrix
          

