package com.rizzutih.vehiclemetricsprocessor.spark

import com.rizzutih.test.UnitSpec
import com.rizzutih.vehiclemetricsprocessor.spark.config.LocalSparkSessionConfig

class SparkSessionConfigTest extends UnitSpec {

  "LocalSparkSessionConfig" should "be configured with local[4] master value" in {
    val session = new LocalSparkSessionConfig().sparkSession()
    val conf = session.conf

    conf.get("spark.master") shouldBe "local[4]"
    conf.get("spark.driver.host") shouldBe "localhost"
    conf.get("spark.app.name") shouldBe "vehicle-metrics-processor"
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    conf.set("parquet.enable.summary-metadata", "false")
  }

}
