package com.rizzutih.vehiclemetricsprocessor.spark

import com.rizzutih.vehiclemetricsprocessor.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

class MetricService(val sparkService: SparkService) extends Logging {

  def findVehicleNumberPercentageChangePerYear(vehicleType: String,
                                               source: String,
                                               destination: String,
                                               years: List[String]): Unit = {

    val dataFrame: DataFrame = buildDataFrame(vehicleType, source, years, new ListBuffer[DataFrame])

    dataFrame.createOrReplaceTempView("dataset")

    val sql = "select round(((d1.vehicle - d2.vehicle) / d2.vehicle) * 100, 4) as increase_percentage, d1.local_authority from dataset d1 join dataset d2 on (d1.year='2019' and d1.local_authority=d2.local_authority) and (d2.year='2018' and d2.local_authority==d1.local_authority)"

    val dataFrameMetric = sparkService.executeSQL(sql)
      .filter(col("local_authority") =!= "Local Authority unknown 3")
      .filter(col("local_authority") =!= "Local Authority District unknown 3")
      .filter(col("local_authority") =!= "Region/Country unknown 3")
      .orderBy(asc("increase_percentage"))
      .withColumn("vehicle_type", lit(vehicleType))
      .withColumn("calculated_at", lit(current_timestamp()))

    sparkService.write(dataFrameMetric, destination)
    Logger.info(s"Successfully wrote parquet dataset to $destination...")
  }

  def buildDataFrame(vehicleType: String,
                     location: String,
                     years: List[String],
                     dataframes: ListBuffer[DataFrame]): DataFrame = {


    for (year <- years) {
      dataframes += sparkService.loadCsvDataset(s"$location-$year.csv")
        .select(regexp_replace(col(vehicleType), ",", "").cast("double").as("vehicle"),
          col("Region/Local Authority").as("local_authority"))
        .withColumn("year", lit(year))
    }

    dataframes.reduce(_ union _)
  }

  sparkService.stop()

}
