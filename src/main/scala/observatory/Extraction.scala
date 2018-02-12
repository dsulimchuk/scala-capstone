package observatory

import java.nio.file.{Files, Paths}
import java.time.LocalDate

import observatory.helpers.Converters

import scala.io.Source


/**
  * 1st milestone: data extraction
  */
object Extraction {

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationToLocation = using(Source.fromFile(fileNameToUri(stationsFile))) { file =>
      file.getLines()
        .map(line => parseStationLine(line))
        .map(t => Station(t._1, t._2) -> t._3.orNull)
        .toMap
    }
    val result = using(Source.fromFile(fileNameToUri(temperaturesFile))) { file =>
      file.getLines()
        .map(line => {
          val (date, maybeStdId, maybeWbanId, temp) = parseTemperaturesFile(line, year)
          val location = stationToLocation.get(Station(maybeStdId, maybeWbanId)).orNull
          (date, location, temp)
        })
        .filter(_._2 != null)
        .filter(_._3 != 9999.9)
        .toList
    }

    result
  }

  private def fileNameToUri(stationsFile: String) = {
    this.getClass.getResource(stationsFile).toURI
  }

  private def fileNameToStreamOfLines(stf: String) = {
    Files.lines(Paths.get(Extraction.getClass.getResource(stf).toURI))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.groupBy(records => records._2).mapValues(v => avg(v.map(_._3)))
  }

  private def avg(reco: Iterable[Temperature]): Temperature = {
    if (reco.isEmpty) 0
    else reco.sum / reco.size
  }

  private def parseStationLine(line: String): (Option[Long], Option[Long], Option[Location]) = {
    val parsed = line.split(",").map(_.trim).toList
    //println(parsed)
    val stdId: Option[Long] = extractValue(parsed, 0).map(_.toLong)
    val wbanId: Option[Long] = extractValue(parsed, 1).map(_.toLong)
    val lat: Option[Double] = extractValue(parsed, 2).map(_.toDouble)
    val lon: Option[Double] = extractValue(parsed, 3).map(_.toDouble)
    val location: Option[Location] = for (l <- lat; ll <- lon) yield Location(l, ll)
    Tuple3(stdId, wbanId, location)
  }

  private def parseTemperaturesFile(line: String,
                                    year: Int): (LocalDate, Option[Long], Option[Long], Temperature) = {
    val parsed = line.split(",").map(_.trim).toList
    //println(parsed)
    val stdId: Option[Long] = extractValue(parsed, 0).map(_.toLong)
    val wbanId: Option[Long] = extractValue(parsed, 1).map(_.toLong)
    val month: Option[Int] = extractValue(parsed, 2).map(_.toInt)
    val day: Option[Int] = extractValue(parsed, 3).map(_.toInt)
    val temp: Option[Temperature] = extractValue(parsed, 4).map(_.toDouble).map(Converters.toCelsius(_))

    val date = LocalDate.of(year, month.getOrElse(1), day.getOrElse(1))
    (date, stdId, wbanId, temp.get)
  }

  private def extractValue(parsedList: List[String], postion: Int): Option[String] = {
    if (parsedList.lengthCompare(postion) <= 0) return Option.empty
    val value = parsedList(postion)
    return if (value.isEmpty) Option.empty else Option(value)
  }

  private case class Station(val stn: Option[Long], val wban: Option[Long])

  private def using[A, B <: {def close() : Unit}](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    } finally {
      closeable.close()
    }
}

