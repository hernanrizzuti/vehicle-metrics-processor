package com.rizzutih.vehiclemetricsprocessor.factory

import com.rizzutih.test.UnitSpec
import com.rizzutih.vehiclemetricsprocessor.enumerator.Environment
import com.rizzutih.vehiclemetricsprocessor.spark.MetricService
import com.rizzutih.vehiclemetricsprocessor.spark.config.{CTSparkSessionConfig, LocalSparkSessionConfig}

class MetricServiceFactoryTest extends UnitSpec {

  "getInstance" should "return a MetricService instance with LocalSparkSessionConfig when env is local" in {
    val metricService = MetricServiceFactory.getInstance(Environment.local)
    metricService shouldBe a[MetricService]
    metricService.sparkService.sparkSessionConfig shouldBe a[LocalSparkSessionConfig]
  }

  it should "return a MetricService instance with CTSparkSessionConfig when env is ct" in {
    val metricService = MetricServiceFactory.getInstance(Environment.ct)
    metricService shouldBe a[MetricService]
    metricService.sparkService.sparkSessionConfig shouldBe a[CTSparkSessionConfig]
  }

}
