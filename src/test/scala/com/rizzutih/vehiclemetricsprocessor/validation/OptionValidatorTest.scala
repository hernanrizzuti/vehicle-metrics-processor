package com.rizzutih.vehiclemetricsprocessor.validation

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import com.rizzutih.test.UnitSpec
import com.rizzutih.vehiclemetricsprocessor.InvalidParameter
import com.rizzutih.vehiclemetricsprocessor.enumerator.Environment

class OptionValidatorTest extends UnitSpec {

  "validateEnv" should "return a valid local env" in {
    val env = Environment.local
    val validated = OptionValidator.validateEnv("env", env.toString)
    validated.isValid shouldBe true
    validated shouldBe Valid(env)
  }

  it should "return a valid ct env" in {
    val env = Environment.ct
    val validated = OptionValidator.validateEnv("env", env.toString)
    validated.isValid shouldBe true
    validated shouldBe Valid(env)
  }

  it should "return InvalidParameter when invalid env is passed" in {
    val validated = OptionValidator.validateEnv("env", "staging")
    validated.isInvalid shouldBe true
    validated shouldBe Invalid(NonEmptyList(InvalidParameter("Incorrect format for option = env"), Nil))
  }

  it should "return InvalidParameter when env is empty" in {
    val validated = OptionValidator.validateEnv("env", "")
    validated.isInvalid shouldBe true
    validated shouldBe Invalid(NonEmptyList(InvalidParameter("env is empty"), Nil))
  }

}
