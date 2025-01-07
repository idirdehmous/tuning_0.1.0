    SUBROUTINE gcDist  (  rlat, rlon, lats, lons   , n , dist  )

    real(8), parameter :: R = 6371.0   ! Radius of Earth in kilometers
    
    integer(4) , intent (in)  :: n 
    real(8)    ,dimension(n)  ,  intent (in) :: lats, lons 
    real(8)    ,dimension(n ),  intent (out):: dist 

    real(8) :: rlat1 , rlon1, lon2, lat2 , delta_phi , delta_lambda , a , c ,rad 
    integer(4)  ::  i  

    rad = 3.141592653589793 / 180.0
    
    ! Convert degrees to radians
    do  i =1,n 

     rlon1 = rlon *  rad 
     rlat1 = rlat *  rad 

     lon2 = lons(i)  * rad 
     lat2 = lats(i)  * rad 

     delta_phi    =rlat1 - lat2 
     delta_lambda =lon2 - rlon1  

    ! Haversine formula
    a = sin(delta_phi / 2.0d0)**2 + cos(rlat1) * cos(lat2) * sin(delta_lambda / 2.0d0)**2
    c = 2.0d0 * atan2(sqrt(a), sqrt(1.0d0 - a))

    ! Calculate distance
    dist( i ) = R * c

     enddo  
     END SUBROUTINE  gcDist 

