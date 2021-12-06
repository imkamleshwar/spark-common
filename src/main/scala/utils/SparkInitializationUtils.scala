package utils

object SparkInitializationUtils {

  def argumentParser(args: String): Map[String, String] = {
    args
      .split("\\s*-D")
      .tail
      .map(kv => kv.split("=")(0).trim -> kv.split("=")(1).trim)
      .toMap
  }

}
