package com.rizzutih.vehiclemetricsprocessor.factory

import com.rizzutih.vehiclemetricsprocessor.enumerator.Environment
import com.rizzutih.vehiclemetricsprocessor.spark.config.{CTSparkSessionConfig, LocalSparkSessionConfig}
import com.rizzutih.vehiclemetricsprocessor.spark.{MetricService, SparkService}
import com.rizzutih.vehiclemetricsprocessor.utils.Logging

object MetricServiceFactory extends Logging {

  def getInstance(env: Environment.Value): MetricService = {
    val sparkSessionConfig = env match {
      case Environment.local =>
        Logger.info("Creating MetricService with LocalSparkSessionConfig...")
        new LocalSparkSessionConfig
      case Environment.ct =>
        Logger.info("Creating MetricService with CTSparkSessionConfig...")
        new CTSparkSessionConfig
    }
    new MetricService(new SparkService(sparkSessionConfig))
  }

}
