package com.tetra.airline

import com.tetra.airline.appdata.AppConfig
import com.tetra.airline.appdata.SparkSessionProvider
import org.apache.spark.sql.{ Dataset, Row };
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession;
import com.tetra.airline.ddl.createTable
import com.tetra.airline.dataload.LoadTable
import scala.collection.mutable.ListBuffer

object airlineReports {
  def getReports(config: AppConfig,spark: SparkSession,tableName:ListBuffer[String]){
	  tableName.foreach(println)
	  var ipData = spark.sparkContext.parallelize(tableName)
	  var pyPath = config.getPythonFilePath()
	  var opData = ipData.pipe(pyPath)
	  opData.foreach(println)
      }
}
