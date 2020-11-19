package com.rizzutih.vehiclemetricsprocessor.option

import scala.annotation.tailrec

object OptionParser {
  val usage =
    """
       Usage:
      |  --env <optional: environment to be ran, if no provided it will be set as 'local'>
    """
  type OptionMap = Map[Symbol, String]

  @tailrec
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--env" :: value :: tail =>
        nextOption(map ++ Map('env -> value), tail)
      case option :: _ =>
        println(usage)
        throw new IllegalArgumentException("Unknown option " + option)
    }
  }

}
