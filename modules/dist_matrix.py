#-*- coding :utf-8 -*- 
import os  , sys 
import multiprocessing as mp 
import numpy           as np 
from   tqdm    import tqdm 
import gc   


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
          #print( "Compute matrix distances,          process ID : {}".format(pp))
          distances_chunk = np.zeros((chunk_size, len(full_data)))

#          for i in tqdm( range(chunk_size), desc="Nchunks ", ncols=100, file=sys.stdout):
          for i in range(chunk_size) :
             lat1, lon1 = chunk_data[i]
             lat2, lon2 = full_data [:, 0] , full_data[:, 1]         # Full data lat/lon

             # The distance is computed with haversine formula 
             # In the R the great circle is computed with Vicenty's formula 
             # Could be implemented later
             # The diffrence in accuracy is just 0.01 %
             distances_chunk[i, :] = self.Haversine(lat1, lon1, lat2, lon2 ) 
    
             #distances_chunk[i, :] = gcDist ( lat1, lon1, lon2, lat2 , len(lon2)  )

             # Could be changed by geopy.geodesic 
             # distances_chunk[i, :] = geodesic(point1, point2).kilometers.all()
          return start_idx, distances_chunk



      def ComputeDistances(self,  latlon , var , chunk_size=20 ):
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
             print("WARNING: No data found to compute distances matrix.  parameter: {}".format(  var))
             nodata=True 

          if not nodata:
             print( "Compute ditances matrix for parameter :" ,  var  ) 
          n_samples = len(latlon)
          distance_matrix = np.zeros((n_samples, n_samples))

          # Split the data into chunks for parallel computation
          p2=mp.Pool( processes= 32  )
          results=[]

          for start_idx in range(0, n_samples, chunk_size):
              chunk_end_idx = min(start_idx + chunk_size, n_samples)
              chunk_data    = latlon[start_idx:chunk_end_idx]

              # Submit task to the pool
              results.append(p2.apply_async(
              self.Distances_in_chunk,  (chunk_data, latlon , start_idx, chunk_end_idx - start_idx) ))


          p2.close()
          p2.join() 

          # Collect all results
          for result in results:
              start_idx, distances_chunk = result.get()
              distance_matrix[start_idx:start_idx+distances_chunk.shape[0], :] = distances_chunk
          return  distance_matrix

