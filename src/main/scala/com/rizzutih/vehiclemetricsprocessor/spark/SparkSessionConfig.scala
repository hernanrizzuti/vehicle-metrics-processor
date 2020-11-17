package com.rizzutih.vehiclemetricsprocessor.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionConfig {

  protected def sparkConf: SparkConf

  def sparkSession(): SparkSession = {
    lazy val sparkSession: SparkSession = SparkSession.builder
      .appName("vehicle-metrics-processor")
      .config(sparkConf)
      .getOrCreate()
    sparkSession
  }
}

class LocalSparkSessionConfig() extends SparkSessionConfig {

  override protected def sparkConf: SparkConf = new SparkConf().setMaster("local[4]").set("spark.driver.host", "localhost")

  override def sparkSession(): SparkSession = {

    val sparkSession = super.sparkSession()
    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    sparkSession.sql("set spark.sql.caseSensitive=true")
    hadoopConfig.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    hadoopConfig.set("parquet.enable.summary-metadata", "false")

    sparkSession
  }
}
