package com.tetra.airline.dataload

import com.tetra.airline.appdata.AppConfig
import com.tetra.airline.appdata.SparkSessionProvider
import org.apache.spark.sql.{ Dataset, Row };
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession;
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

object LoadTable {

def srcTableLoad(config: AppConfig,spark: SparkSession){

   for (fid <- scala.io.Source.fromFile(config.getSrcloadPath()).getLines){
		    	spark.sql(fid)    	
		    }
    }

def stgTableLoad(config: AppConfig,spark: SparkSession,tblSrcNameList: ListBuffer[String]){
	import spark.implicits._
	for (tbl <- tblSrcNameList) {
		    var tblTmp = tbl.replace("src","stg")
		    var df=spark.table(tbl)
		    var tblColumns = df.columns.toSeq
		    var sample_Clmn = tblColumns(0)
		    var clmnCount = tblColumns.length
		    var partDetails=config.getPrtionClmn()
		    var partTables= partDetails.split('|')		    
		    var partionFound = false
		    var partClmns = List[String]()
		    var partClmnsStr =""
		    println(partTables(0).toUpperCase())
		    println(tblTmp.toUpperCase())
		    for (id <- 0 until partTables.length){
			var parTbl = partTables(id).split(',').head
			println(tblTmp.toUpperCase() +"#####"+ parTbl.toUpperCase())
			println(partionFound+"#####"+parTbl)
			 if ((tblTmp.toUpperCase() == parTbl.toUpperCase()) && partionFound == false ) {
				partionFound = true	
				println(partionFound)
				if (partTables(id).split(',').length > 2){
					println("Greater"+partTables(id).split(',').toList.tail)
					partClmns = partClmns ++ partTables(id).split(',').tail.toList
					partClmnsStr = partClmns.mkString(",")
					}
				else{
					partClmns = partClmns ++ partTables(id).split(',').tail.toList
					println("Less"+partClmns)
					partClmnsStr = partClmns.mkString
				}
				
			 }			 
		    }		    	    
		    var clDf= df.na.replace(tblColumns,Map("NA"->"0",""->"0"))		    
		    var tmpclfDF=clDf.filter($"$sample_Clmn" !== sample_Clmn)    
	    
		    if (partionFound){	
			//val lastPart=tblColumns.filter(!partClmns.contains(_))
			var lastPart= tblColumns.map(x=>x.toUpperCase).filter(!partClmns.map(y=>y.toUpperCase).contains(_))
			var finalClmns = lastPart ++ partClmns
			tmpclfDF.select(finalClmns.head,finalClmns.tail: _*).createTempView(tblTmp.substring(tblTmp.indexOf(".")+1) + "_tmp")

			println("Stage Load####lastPart#####"+lastPart)
			println("Stage Load#####partClmns####"+partClmns(0))
		        println("Stage Load####finalClmns#####"+finalClmns)

			spark.sql("Insert into " + tblTmp + " PARTITION("+partClmnsStr+") SELECT * from " + tblTmp.substring(tblTmp.indexOf(".")+1) + "_tmp")
			}
		    else{
			tmpclfDF.createTempView(tblTmp.substring(tblTmp.indexOf(".")+1) + "_tmp")
			spark.sql("Insert into " + tblTmp + " select * from " + tblTmp.substring(tblTmp.indexOf(".")+1) + "_tmp")
		    }

		}
    }

def tgtTableLoad(config: AppConfig,spark: SparkSession,tableNameList: ListBuffer[String]): ListBuffer[String]={
        import spark.implicits._
	var tableName =""
	var repQuery = ""
	for (fid <- scala.io.Source.fromFile(config.getRepSqlPath()).getLines){
		    	var tableNameStr = fid.split("&&")
			tableName = tableNameStr(0)
			repQuery = tableNameStr(1)
			val resultDf = spark.sql(repQuery)			
			//resultDf.write.format("parquet").mode("overwrite").saveAsTable(tableName)			
			resultDf.coalesce(2).write.format("orc").mode("overwrite").saveAsTable(tableName)
			//resultDf.write.format("text").mode("overwrite").saveAsTable(tableName)
			if (tableName.trim.length > 0 ){
				tableNameList += tableName
				tableName = ""
			   }
		    }   
	return tableNameList
    }
}