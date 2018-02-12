package observatory.helpers

object Converters {
  def toCelsius(farenheit: Double) = ((farenheit - 32) * 5) / 9
}
