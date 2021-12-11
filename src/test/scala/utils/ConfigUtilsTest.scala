package utils

import com.typesafe.config.{ConfigException, ConfigFactory, ConfigValueFactory}
import constants.ConfigConstants.MANDATORY_CONFIGS
import testSpec.TestSpecWithFunSuite
import utils.ConfigUtils.{addSparkArgsToConfig, checkMandatoryConfig}

class ConfigUtilsTest extends TestSpecWithFunSuite {

  test("ConfigUtilsTest.addSparkArgsToConfig 1: Config should get updated when appropriate config value is passed") {

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

  test("ConfigUtilsTest.checkMandatoryConfig 1: `Either` Left should have true") {

    val config = ConfigFactory.load("spark.conf")
      .withValue("spark.logLevel", ConfigValueFactory.fromAnyRef("INFO"))
      .withValue("jobConfigs", ConfigValueFactory.fromAnyRef("jobsConf.testSDC2Job.conf"))

    val eitherBoolOrException = checkMandatoryConfig(MANDATORY_CONFIGS, config)

    assert(
      eitherBoolOrException.isLeft
        && !eitherBoolOrException.isRight
    )
  }

  test("ConfigUtilsTest.checkMandatoryConfig 2: `Either` Right should be have ConfigException") {

    val config = ConfigFactory.load("spark.conf")
      .withValue("spark.logLevel", ConfigValueFactory.fromAnyRef("INFO"))

    val eitherBoolOrException = checkMandatoryConfig(MANDATORY_CONFIGS, config)

    assert(
      !eitherBoolOrException.isLeft
        && eitherBoolOrException.isRight
    )

    an[ConfigException] should be thrownBy (throw eitherBoolOrException.right.get)
  }

  test("ConfigUtilsTest.checkMandatoryConfig 3: `Either` Right should be true if MANDATORY_CONFIGS is not same") {

    val config = ConfigFactory.load("spark.conf")
      .withValue("spark.logLevel", ConfigValueFactory.fromAnyRef("INFO"))
      .withValue("jobConfigs", ConfigValueFactory.fromAnyRef("jobsConf.testSDC2Job.conf"))

    val eitherBoolOrException = checkMandatoryConfig(MANDATORY_CONFIGS + "aDummyConfig", config)

    assert(
      !eitherBoolOrException.isLeft
        && eitherBoolOrException.isRight
    )
  }

}
