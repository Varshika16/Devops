# Airline-Dataset-Analysis

Using the airline on-time performance dataset to demonstrate techniques in this spark-scala project and we will proceed to answer the questions related to the flight delays and prepare the excel reports using the Python programming.

# Requirements
    
    Spark 2.1+                                                                                                               
    Scala 2.11+                                                                                                                      
    Hadoop 2.6+                                                                                                          
    Hive 1.2+                                                                                                           
    Python 2.7+
                                                                                                                                 
# Features
 
    =>  Application will receive the airline on-time performance dataset CSV files as source data and will load it into the Hive source tables.
    =>  Cleansing will be performed on Hive tables required columns and will load it into the stage by applying the Hive Dynamic partition.
    =>  Report queries will be executed and target tables will be loaded.
    =>  Excel Charts will be generated on target tables data by using Python.

# Using with Spark Submit

    spark-submit --class com.tetra.airline.airlineETL --jars /home/<username>/jars/typesafe-config-2.10.1.jar airline-0.0.1.jar /home/<username>/config/configfile.conf
    
# Reports
    
    =>  What is the best time of day of week to fly to minimize delays?

![DayOfWeekDelay](/uploads/4f1854708abeae03ec0229017a33a979/DayOfWeekDelay.JPG)    
   
    =>  What is the best time of year to fly to minimize delays?
    
![MonthlyDelay](/uploads/d37af233caa8c9b556f73ca82e42e780/MonthlyDelay.JPG)
    
    =>  What is the best time of day to fly to minimize delays?
    
![HourOfDayDelay](/uploads/c07d40f81bf3367f84eb2d06063ce3b0/HourOfDayDelay.JPG)    
    
    =>  Do older planes suffer more delays?
    
![OldvsNew](/uploads/5ddcf6625df307cc835355849b9de9f1/OldvsNew.JPG)    
