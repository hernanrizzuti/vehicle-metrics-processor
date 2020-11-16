package com.rizzutih.vehiclemetricsprocessor.spark

import com.rizzutih.vehiclemetricsprocessor.utils.Logging
import org.apache.spark.sql.functions.col

class SparkProcessorService(val sparkService: SparkService) extends Logging {

  def harvestDataset(): Unit = {
    val df2018 = sparkService.loadCsvDataset("src/main/scala/resources/veh0105-2018.csv")
      .filter(col("Region/Local Authority") === "United Kingdom")
      .select(col("Cars"))
      .withColumn("Cars2018", col("Cars"))
      .drop(col("Cars"))

    val df2019 = sparkService.loadCsvDataset("src/main/scala/resources/veh0105-2019.csv")
      .filter(col("Region/Local Authority") === "United Kingdom")
      .select(col("Cars"))
      .withColumn("Cars2019", col("Cars"))
      .drop(col("Cars"))

    df2018.join(df2019).printSchema() //.withColumn("Total", expr("Cars2019 - Cars2018")).show()//.drop("Cars2018").drop("Cars2019").show()

    //sparkService.write(df, "")
    Logger.info(s"Successfully wrote parquet dataset to ...")
  }

  sparkService.stop()
}
