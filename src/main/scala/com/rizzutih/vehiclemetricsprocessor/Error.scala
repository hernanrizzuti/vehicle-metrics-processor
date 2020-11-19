package com.rizzutih.vehiclemetricsprocessor

sealed trait Error extends Product with Serializable {
  val message: String
}

case class InvalidParameter(message: String) extends Error


