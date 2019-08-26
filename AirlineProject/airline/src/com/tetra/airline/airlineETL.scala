package com.tetra.airline

import com.tetra.airline.appdata.AppConfig
import com.tetra.airline.appdata.SparkSessionProvider
import org.apache.spark.sql.{ Dataset, Row };
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession;
import com.tetra.airline.ddl.createTable
import com.tetra.airline.dataload.LoadTable
import com.tetra.airline.airlineReports
import scala.collection.mutable.ListBuffer

object airlineETL {
  def main(args: Array[String]) {
   try{
    val conf = new AppConfig()
    val config = conf.getConfigInfo(args(0).toString())
    val spark = new SparkSessionProvider().getInstance(config)
    import spark.implicits._
    
    var appName = spark.sparkContext.appName
    println(appName)
    
    var tblSrcList = ListBuffer[String]() 
    if (config.getLoadIntoSrc()){
	createTable.getCreateSrcTables(config,spark,tblSrcList)
	LoadTable.srcTableLoad(config,spark)
    }
    
    if (config.getLoadIntoStg()){	
        createTable.getCreateStgTables(config,spark)
        LoadTable.stgTableLoad(config,spark,tblSrcList)
    }  

    var tblTgtList = ListBuffer[String]()    	
    tblTgtList = LoadTable.tgtTableLoad(config,spark,tblTgtList)
    airlineReports.getReports(config,spark,tblTgtList)
	
    }
    catch{
      case th: Throwable =>
        println("Execption : " + th)
        throw th
    } finally {
      //spark.stop()
    }
}
}
