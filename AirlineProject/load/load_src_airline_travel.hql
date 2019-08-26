USE airline_DB
load data local inpath "/home/tsipl0514/AirlineProject/sourceData/travel" into table airline_DB.src_airline_travel
load data local inpath "/home/tsipl0514/AirlineProject/sourceData/planeData" into table airline_DB.src_airline_planedata
load data local inpath "/home/tsipl0514/AirlineProject/sourceData/carriers" into table airline_DB.src_airline_carriers
load data local inpath "/home/tsipl0514/AirlineProject/sourceData/airports" into table airline_DB.src_airline_airports