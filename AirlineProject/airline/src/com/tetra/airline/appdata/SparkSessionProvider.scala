package com.tetra.airline.appdata

import scala.beans.{ BeanProperty, BooleanBeanProperty }
import org.apache.spark.sql.SparkSession;

class SparkSessionProvider {

  private var spark: SparkSession = null

  def getInstance(config: AppConfig): SparkSession = {
    if (spark == null) {
      spark = SparkSession
        .builder()
        .appName(config.getRunApplicationName())
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.warehouse.data.skipTrash", "true")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate
    }
    spark
  }
}