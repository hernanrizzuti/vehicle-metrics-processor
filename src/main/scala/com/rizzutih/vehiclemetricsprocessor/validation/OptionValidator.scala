package com.rizzutih.vehiclemetricsprocessor.validation

import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import com.rizzutih.vehiclemetricsprocessor.InvalidParameter
import com.rizzutih.vehiclemetricsprocessor.enumerator.Environment

object OptionValidator {

  private val errorMsg = "Incorrect format for option ="

  def validateEnv(key: String,
                  value: String): ValidatedNel[InvalidParameter, Environment.Value] = {

    if (value.isEmpty) {
      return InvalidParameter(s"$key is empty").invalidNel
    }
    val option: Option[Environment.Value] = Environment.withNameOpt(value)
    if (option.isDefined) {
      option.get.validNel
    } else {
      InvalidParameter(s"$errorMsg $key").invalidNel
    }
  }

}
