package com.rizzutih.vehiclemetricsprocessor.option

import com.rizzutih.test.UnitSpec

class OptionParserTest extends UnitSpec {

  "nextOption" should "return an empty list when no args passed" in {
    OptionParser.nextOption(Map(), List.empty).isEmpty shouldBe true
  }

  it should "return env ct" in {
    val env = "ct"
    OptionParser.nextOption(Map(), List("--env", env)).get('env) shouldBe Some(env)
  }

  it should "throw exception when arg is invalid" in {
    val env = "ct"
    val exception = intercept[IllegalArgumentException] {
      OptionParser.nextOption(Map(), List("--environment", env))
    }
    assert(exception.getMessage == "Unknown option --environment")
  }

}
