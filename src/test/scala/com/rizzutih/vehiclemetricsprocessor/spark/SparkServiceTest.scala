package com.rizzutih.vehiclemetricsprocessor.spark

import com.rizzutih.test.UnitSpec
import com.rizzutih.vehiclemetricsprocessor.spark.config.SparkSessionConfig
import org.apache.spark.sql._

class SparkServiceTest extends UnitSpec {
  private val sparkSessionConfig: SparkSessionConfig = mock[SparkSessionConfig]
  private val session: SparkSession = mock[SparkSession]
  private val sparkService: SparkService = new SparkService(sparkSessionConfig)
  private val dataFrameReader: DataFrameReader = mock[DataFrameReader]
  private val dataFrame: DataFrame = mock[DataFrame]
  private val dataSet: Dataset[Row] = mock[Dataset[Row]]
  private val dataFrameWriter: DataFrameWriter[Row] = mock[DataFrameWriter[Row]]
  val location = "some/location"

  before {
    sparkSessionConfig.sparkSession() shouldReturn session
  }

  "sparkSession" should "call SparkSession" in {
    sparkService.sparkSession

    sparkSessionConfig.sparkSession() was called
  }

  "loadCsvDataset" should "call load" in {
    // Given
    session.read shouldReturn dataFrameReader
    dataFrameReader.format("csv") shouldReturn dataFrameReader
    dataFrameReader.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")) shouldReturn dataFrameReader
    // When
    sparkService.loadCsvDataset(location)

    // Then
    dataFrameReader.load(location) was called
  }

  "executeSQL" should "call sql" in {
    val sql = "select * from table"
    sparkService.executeSQL(sql)

    session.sql(sql)
  }

  "write" should "call parquet" in {
    dataFrame.repartition(1) shouldReturn dataSet
    dataSet.write shouldReturn dataFrameWriter
    dataFrameWriter.mode(SaveMode.Overwrite) shouldReturn dataFrameWriter

    sparkService.write(dataFrame, location)

    dataFrameWriter.parquet(location) was called
  }

  "stop" should "call stop" in {
    sparkService.stop()

    session.stop() was called
  }
}
