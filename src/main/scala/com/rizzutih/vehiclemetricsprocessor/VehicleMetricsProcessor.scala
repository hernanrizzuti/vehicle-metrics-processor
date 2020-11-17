package com.rizzutih.vehiclemetricsprocessor

import com.rizzutih.vehiclemetricsprocessor.enumerator.VehicleType
import com.rizzutih.vehiclemetricsprocessor.spark.{LocalSparkSessionConfig, MetricService, SparkService}

object VehicleMetricsProcessor extends App {

  run(args)

  def run(args: Array[String]): Unit = {
    val sparkService: SparkService = new SparkService(new LocalSparkSessionConfig())
    val sparkProcessorService: MetricService = new MetricService(sparkService)
    for(vehicle<- VehicleType.values)
    sparkProcessorService.calculateMetric(vehicle.toString)
  }
}
