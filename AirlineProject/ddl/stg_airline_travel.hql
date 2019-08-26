DROP TABLE IF EXISTS airline_DB.stg_airline_travel
CREATE TABLE IF NOT EXISTS airline_DB.stg_airline_travel(month Int ,dayofmonth Int ,dayofweek Int ,deptime Int ,crsdeptime Int ,arrtime Int ,crsarrtime Int ,uniquecarrier String ,flightnum String ,tailnum String ,actualelapsedtime String ,crselapsedtime String ,airtime String ,arrdelay Int ,depdelay Int ,origin String ,dest String ,distance String ,taxiin String ,taxiout String ,cancelled String ,cancellationcode String ,diverted String ,carrierdelay String ,weatherdelay String ,nasdelay String ,securitydelay String ,lateaircraftdelay String )PARTITIONED BY (year Int) STORED AS ORC
DROP TABLE IF EXISTS airline_DB.stg_airline_carriers
CREATE TABLE IF NOT EXISTS airline_DB.stg_airline_carriers(Code String, Describtion String) STORED AS ORC
DROP TABLE IF EXISTS airline_DB.stg_airline_planedata
CREATE TABLE IF NOT EXISTS airline_DB.stg_airline_planedata(tailnum String,type String, manufacturer String, issue_date String, model String, status String, aircraft_type String, engine_type String,year String)STORED AS ORC
DROP TABLE IF EXISTS airline_DB.stg_airline_airports
CREATE TABLE IF NOT EXISTS airline_DB.stg_airline_airports(iata String, airport String, city String, state String, country String, lat String, long String) STORED AS ORC