package com.rizzutih.vehiclemetricsprocessor

import cats.data.Validated.{Invalid, Valid}
import com.rizzutih.vehiclemetricsprocessor.enumerator.VehicleType
import com.rizzutih.vehiclemetricsprocessor.factory.MetricServiceFactory
import com.rizzutih.vehiclemetricsprocessor.option.OptionParser
import com.rizzutih.vehiclemetricsprocessor.spark.MetricService
import com.rizzutih.vehiclemetricsprocessor.utils.Logging
import com.rizzutih.vehiclemetricsprocessor.validation.OptionValidator.validateEnv

object VehicleMetricsProcessor extends App with Logging {

  run(args)

  def run(args: Array[String]): Unit = {
    val options = OptionParser.nextOption(Map(), args.toList)
    val env = options.getOrElse('env, "local")

    validateEnv("env", env) match {
      case Valid(validEnv) =>
        val sparkProcessorService: MetricService = MetricServiceFactory.getInstance(validEnv)
        for (vehicle <- VehicleType.values)
          sparkProcessorService.findVehicleNumberPercentageChangePerYear(vehicle.toString,
            "src/main/scala/resources/veh0105",
            s"src/main/scala/resources/outputFiles/$vehicle",
            List("2018", "2019"))
      case Invalid(errors) => throw new IllegalArgumentException(errors.toString)
    }

  }
}
