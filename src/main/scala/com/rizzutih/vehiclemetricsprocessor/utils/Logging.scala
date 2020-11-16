package com.rizzutih.vehiclemetricsprocessor.utils

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  val Logger: Logger = LoggerFactory.getLogger(this.getClass)

}
