package com.rizzutih.test

import org.mockito.scalatest.ResetMocksAfterEachTest
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Inspectors, OptionValues}

abstract class UnitSpec extends AnyFlatSpec
  with Matchers
  with OptionValues
  with Inspectors
  with IdiomaticMockito
  with ArgumentMatchersSugar
  with ResetMocksAfterEachTest
  with BeforeAndAfter
  with BeforeAndAfterAll {
}
