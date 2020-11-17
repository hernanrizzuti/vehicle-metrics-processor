package com.rizzutih.vehiclemetricsprocessor.enumerator

object VehicleType extends Enumeration {

  type VehicleType = Value

  val Cars = Value("Cars")
  val Motorcycles = Value("Motorcycles")
  val LGV = Value("Light Goods Vehicles")
  val HGV = Value("Heavy Goods Vehicles")
  val BusesAndCouches = Value("Buses and coaches")
  val Other = Value("Other vehicles 2")
  val Total= Value("Total")
  val DieselCars = Value("Diesel Cars")
  val DieselVans = Value("Diesel Vans")

}
