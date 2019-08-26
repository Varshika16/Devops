package com.tetra.airline.ddl
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.tetra.airline.appdata.AppConfig
import com.tetra.airline.appdata.SparkSessionProvider
import scala.collection.mutable.ListBuffer

object createTable {
  
  
  def getCreateSrcTables(config : AppConfig,spark : SparkSession ,tblList:ListBuffer[String]): ListBuffer[String] = {
	   var table_Name = ""
	   for (fid <- scala.io.Source.fromFile(config.getDdlSrcPath()).getLines){
		    	spark.sql(fid)  
			var tblStr = fid.split(" ")			
			if (tblStr(0).trim.toUpperCase == "DROP"){
				table_Name = tblStr(tblStr.length -1)
			}
			else
			{
				if (table_Name.trim.length > 0 ){
					tblList += table_Name
					//LoadTable.tgtTableLoad(config,spark,table_Name)
				}
			}
		    }
		 return tblList
    
    }
   
 def getCreateStgTables(config : AppConfig,spark : SparkSession ) = {

	   for (fid <- scala.io.Source.fromFile(config.getDdlStgPath()).getLines){
		    	spark.sql(fid) 			
		    }    
    }

    def getCreateTgtTables(config : AppConfig,spark : SparkSession ,tblList:ListBuffer[String]): ListBuffer[String] = {
	   var table_Name = ""
	   for (fid <- scala.io.Source.fromFile(config.getDdlTgtPath()).getLines){
		    	spark.sql(fid)
			var tblStr = fid.split(" ")			
			if (tblStr(0).trim.toUpperCase == "DROP"){
				table_Name = tblStr(tblStr.length -1)
			}
			else
			{
				if (table_Name.trim.length > 0 ){
					tblList += table_Name
					//LoadTable.tgtTableLoad(config,spark,table_Name)
				}
			}
		    }
           return tblList
    
    }
    
}