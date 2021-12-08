package utils

import com.typesafe.config.ConfigFactory
import testSpec.TestSpecWithFunSuite
import utils.ConfigUtils.addSparkArgsToConfig

class ConfigUtilsTest extends TestSpecWithFunSuite {

  test("ConfigUtilsTest 1: Config should get updated when appropriate config value is passed") {

    val config = ConfigFactory.load("spark.conf")

    val sparkArgsMap = Map("spark.appName" -> "NEW UNIT TEST SPARK APP",
      "spark.shufflePartition" -> "100",
      "spark.logLevel" -> "ERROR",
      "inputPath" -> "hdfs/path/of/input",
      "outputPath" -> "output/path")

    val newConfig = addSparkArgsToConfig(config, sparkArgsMap)

    assert(
      config.getConfig("spark").entrySet().size() + 1 == newConfig.getConfig("spark").entrySet().size()
        && config.getString("spark.appName") != newConfig.getString("spark.appName")
        && config.getString("spark.logLevel") != newConfig.getString("spark.logLevel")
        && config.getString("spark.appName") == "Unit Test Spark Job"
        && config.getString("spark.logLevel") == "WARN"
        && newConfig.getString("spark.appName") == "NEW UNIT TEST SPARK APP"
        && newConfig.getString("spark.logLevel") == "ERROR"
        && newConfig.getString("outputPath") == "output/path"
    )

  }

}
