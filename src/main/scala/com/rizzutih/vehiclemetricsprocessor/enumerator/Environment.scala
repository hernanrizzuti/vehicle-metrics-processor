package com.rizzutih.vehiclemetricsprocessor.enumerator

object Environment extends Enumeration {

  val local, ct = Value

  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)

}
