package com.rizzutih.vehiclemetricsprocessor.spark.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionConfig {

  protected def sparkConf: SparkConf

  def sparkSession(): SparkSession = {
    lazy val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
    sparkSession
  }
}

class LocalSparkSessionConfig extends SparkSessionConfig {

  override protected def sparkConf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .set("spark.driver.host", "localhost")
    .setAppName("vehicle-metrics-processor")

  override def sparkSession(): SparkSession = {

    val sparkSession = super.sparkSession()
    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    sparkSession.sql("set spark.sql.caseSensitive=true")
    hadoopConfig.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    hadoopConfig.set("parquet.enable.summary-metadata", "false")

    sparkSession
  }
}

class CTSparkSessionConfig extends SparkSessionConfig {

  val s3ServerPort: Int = 9999
  val awsEndpointUri: String = s"http://localhost:$s3ServerPort/"
  val awsAccessKey: String = "accessKey"
  val awsSecretKey: String = "secretKey"

  override protected def sparkConf: SparkConf = new SparkConf()
    .set("spark.driver.host", "127.0.0.1")
    .setMaster("local[4]")
    .setAppName("vehicle-metrics-processor")

  override def sparkSession(): SparkSession = {

    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate

    sparkSession.sql("set spark.sql.caseSensitive=true")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", awsEndpointUri)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKey)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretKey)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.attempts.maximum", "3")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.change.detection.version.required", "false")

    sparkSession
  }
}

