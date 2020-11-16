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

    sparkSession
  }
}
