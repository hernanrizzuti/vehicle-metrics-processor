package com.rizzutih.vehiclemetricsprocessor.spark

import java.time.LocalDate

import com.amazonaws.services.s3.AmazonS3
import com.rizzutih.test.TestUtil.{buildS3Client, readParquet, uploadObjectToS3, withS3Mock}
import com.rizzutih.test.{TestUtil, UnitSpec}
import com.rizzutih.vehiclemetricsprocessor.spark.config.CTSparkSessionConfig
import io.findify.s3mock.S3Mock
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace}

import scala.collection.mutable.ListBuffer

class MetricServiceTest extends UnitSpec {

  val sparkSessionConfig = new CTSparkSessionConfig
  val sparkService: SparkService = new SparkService(sparkSessionConfig)
  val metricService = new MetricService(sparkService)
  val dataFrameList = new ListBuffer[DataFrame]
  val year = "2019"
  val vehicle = "Cars"
  val source = "src/test/resources/inFiles/veh0105"

  "buildDataFrame" should "return dataFrame" in {
    val expectedDataframe = TestUtil.readCsv(sparkSessionConfig.sparkSession(), s"$source-$year.csv")
    val expectedMetric = expectedDataframe.select(regexp_replace(col(vehicle), ",", "").cast("double").as("vehicle"),
      col("Region/Local Authority").as("local_authority"))
      .withColumn("year", lit(year)).collect()

    val actualMetric = metricService.buildDataFrame(vehicle, source, List(year), dataFrameList).collect()

    actualMetric.length shouldBe 4
    actualMetric(0) shouldBe expectedMetric(0)
    actualMetric(1) shouldBe expectedMetric(1)
    actualMetric(2) shouldBe expectedMetric(2)
  }

  "findVehicleNumberPercentageChangePerYear" should "write metric" in {
    val testBucket = "test-bucket"
    val source = s"s3a://$testBucket/vehicle/statistics/in/veh0105"
    val destination = s"s3a://$testBucket/vehicle/statistics/out/$vehicle"
    val s3MockApi: S3Mock = withS3Mock()
    val s3Client: AmazonS3 = buildS3Client()
    s3MockApi.start
    s3Client.createBucket(testBucket)
    uploadObjectToS3(s3Client, "test-bucket", s"vehicle/statistics/in", "veh0105-2019.csv")
    uploadObjectToS3(s3Client, "test-bucket", s"vehicle/statistics/in", "veh0105-2018.csv")

    metricService.findVehicleNumberPercentageChangePerYear(vehicle, source, destination, List("2018", "2019"))
    val session = sparkSessionConfig.sparkSession()
    session.sqlContext.tableNames().contains("dataset") shouldBe true
    val actualMetric = readParquet(session, destination).collectAsList()

    /*
    *
+-------------------+---------------+------------+--------------------------+
|increase_percentage|local_authority|vehicle_type|calculated_at             |
+-------------------+---------------+------------+--------------------------+
|1.1435             |England        |Cars        |2020-11-19 20:34:31.492326|
|1.1765             |Great Britain  |Cars        |2020-11-19 20:34:31.492326|
|1.2033             |United Kingdom |Cars        |2020-11-19 20:34:31.492326|
|1.8098             |North East     |Cars        |2020-11-19 20:34:31.492326|
+-------------------+---------------+------------+--------------------------+
    * */

    val firstRow = actualMetric.get(0)
    val SecondRow = actualMetric.get(1)
    val thirdRow = actualMetric.get(2)
    val fourthRow = actualMetric.get(3)

    firstRow.get(0) shouldBe 1.1435
    firstRow.get(1) shouldBe "England"
    firstRow.get(2) shouldBe "Cars"
    firstRow.get(3).toString.contains(LocalDate.now.toString) shouldBe true

    SecondRow.get(0) shouldBe 1.1765
    SecondRow.get(1) shouldBe "Great Britain"
    SecondRow.get(2) shouldBe "Cars"
    SecondRow.get(3).toString.contains(LocalDate.now.toString) shouldBe true

    thirdRow.get(0) shouldBe 1.2033
    thirdRow.get(1) shouldBe "United Kingdom"
    thirdRow.get(2) shouldBe "Cars"
    thirdRow.get(3).toString.contains(LocalDate.now.toString) shouldBe true

    fourthRow.get(0) shouldBe 1.8098
    fourthRow.get(1) shouldBe "North East"
    fourthRow.get(2) shouldBe "Cars"
    fourthRow.get(3).toString.contains(LocalDate.now.toString) shouldBe true
  }

}
