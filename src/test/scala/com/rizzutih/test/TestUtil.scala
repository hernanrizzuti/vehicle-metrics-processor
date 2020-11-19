
package com.rizzutih.test

import java.io.File

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.rizzutih.vehiclemetricsprocessor.spark.config.CTSparkSessionConfig
import io.findify.s3mock.S3Mock
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUtil {
  private val ctSparkSessionConfig = new CTSparkSessionConfig

  def withS3Mock(): S3Mock = {
    new S3Mock.Builder().withPort(ctSparkSessionConfig.s3ServerPort).withInMemoryBackend().build
  }

  def buildS3Client(): AmazonS3 = {

    val client = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(new EndpointConfiguration(ctSparkSessionConfig.awsEndpointUri, Regions.US_EAST_1.getName))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ctSparkSessionConfig.awsAccessKey, ctSparkSessionConfig.awsSecretKey)))
      .build()
    client
  }

  def uploadObjectToS3(s3Client: AmazonS3, testBucketName: String, location: String, fileName: String): Unit = {
    s3Client.putObject(testBucketName,
      s"$location/$fileName",
      new File(getClass.getResource(s"/inFiles/$fileName").getPath))
  }

  def readParquet(spark: SparkSession, location: String): DataFrame = {
    spark.read.format("parquet").load(location)
  }

  def readCsv(spark: SparkSession, location: String): DataFrame = {
    spark.read.format("csv").options(Map("header" -> "true", "delimiter" -> ",")).load(location)
  }
}
