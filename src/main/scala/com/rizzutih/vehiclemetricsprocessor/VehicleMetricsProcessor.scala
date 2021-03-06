package com.rizzutih.vehiclemetricsprocessor

import com.rizzutih.vehiclemetricsprocessor.spark.{LocalSparkSessionConfig, SparkProcessorService, SparkService}

object VehicleMetricsProcessor extends App {

  run(args)

  def run(args: Array[String]): Unit = {
    print(classOf[org.apache.commons.lang3.SystemUtils].getResource("SystemUtils.class"))
    val sparkService: SparkService = new SparkService(new LocalSparkSessionConfig())
    val sparkProcessorService: SparkProcessorService = new SparkProcessorService(sparkService)
    sparkProcessorService.harvestDataset()
  }
}
