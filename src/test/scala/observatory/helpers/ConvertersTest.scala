package observatory.helpers

import org.scalatest.FunSuite

class ConvertersTest extends FunSuite {

  test("testToCelsius") {
    assert(Converters.toCelsius(50) == 10)
  }

}
