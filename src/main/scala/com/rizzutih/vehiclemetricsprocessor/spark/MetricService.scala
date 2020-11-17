package com.rizzutih.vehiclemetricsprocessor.spark

import com.rizzutih.vehiclemetricsprocessor.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, lit, regexp_replace}

import scala.collection.mutable.ListBuffer

class MetricService(val sparkService: SparkService) extends Logging {

  def calculateMetric(): Unit = {
    val years = List("2018", "2019")
    val dataframes = new ListBuffer[DataFrame]
    for (year <- years) {
      dataframes += sparkService.loadCsvDataset(s"src/main/scala/resources/veh0105-$year.csv")
        .select(regexp_replace(col("Cars"), ",", "")
          .cast("double").as("cars"), col("Region/Local Authority").as("local_authority"))
        .withColumn("year", lit(year))
    }
    dataframes.reduce(_ union _).createOrReplaceTempView("dataset")

    val sql = "select round(((d1.cars - d2.cars) / d2.cars) * 100, 4) as increase_percentage, d1.local_authority from dataset d1 join dataset d2 on (d1.year='2019' and d1.local_authority=d2.local_authority) and (d2.year='2018' and d2.local_authority==d1.local_authority)"

    val df = sparkService.executeSQL(sql)
      .filter(col("local_authority") =!= "Local Authority unknown 3")
      .filter(col("local_authority") =!= "Local Authority District unknown 3")
      .filter(col("local_authority") =!= "Region/Country unknown 3")
      .orderBy(asc("increase_percentage"))

    sparkService.write(df, "src/main/scala/resources/outputFiles")
    Logger.info(s"Successfully wrote parquet dataset to ...")
  }

  sparkService.stop()
}
