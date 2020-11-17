package com.rizzutih.vehiclemetricsprocessor.spark

import com.rizzutih.vehiclemetricsprocessor.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class SparkService(val sparkSessionConfig: SparkSessionConfig) extends Logging {

  def sparkSession: SparkSession = {
    sparkSessionConfig.sparkSession()
  }

  def loadCsvDataset(location: String): DataFrame = {
    Logger.info("Loading parquet dataset...")
    sparkSession.read.format("csv")
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .load(location)
  }

  def executeSQL(sql: String): DataFrame = {
    sparkSession.sql(sql)
  }

  def write(dataFrame: DataFrame, location: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(location)
  }

  def stop(): Unit = {
    sparkSession.stop()
  }

}
