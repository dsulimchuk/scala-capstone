package observatory

import java.time.LocalDate

import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {

  test("locationYearlyAverageRecords") {
    val now = LocalDate.now()
    val x = List((now, Location(1, 2), 5.0), (now, Location(1, 2), 10.0))
    val result = Extraction.locationYearlyAverageRecords(x)
    assert(result.size === 1)
    assert(result === Map(Location(1, 2) -> 7.5))
  }

  test("locateTemperatures") {
    val result = Extraction.locateTemperatures(1975, "/stations.csv", "/test.csv")
    assert(result.size === 7)
    val first = result.iterator.next()
    assert(first._1 === LocalDate.of(1975, 8, 11))
    assert(first._2 === Location(37.35,-78.433))
    assert(first._3 === 27.299999999999997)
    println(result)
  }

}