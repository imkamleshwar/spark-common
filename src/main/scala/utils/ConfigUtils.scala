package utils

import com.typesafe.config.{Config, ConfigException, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging

object ConfigUtils extends LazyLogging {

  def addSparkArgsToConfig(config: Config, sparkArgsMap: Map[String, String]): Config = {
    sparkArgsMap
      .foldLeft(config) { case (conf, (key, value)) => conf.withValue(key, ConfigValueFactory.fromAnyRef(value)) }
  }

  def checkMandatoryConfig(mandatoryConfigs: Set[String], config: Config): Either[Boolean, Exception] = {
    import scala.collection.JavaConverters._
    val configKeySet = config.entrySet().asScala.map(KeyAndConfigValue => KeyAndConfigValue.getKey)
    val diffSet = mandatoryConfigs.diff(configKeySet)
    if (diffSet.isEmpty) {
      logger.info(s"All mandatory configs are passed: ${mandatoryConfigs.mkString(", ")}")
      Left(true)
    } else {
      logger.error(s"All mandatory configs are NOT passed:")
      logger.error(s"Mandatory Configs required to be passed: ${mandatoryConfigs.mkString(", ")}")
      val configMissingException = new ConfigException.Missing(diffSet.mkString(", "))
      logger.error(s"Mandatory Configs which are NOT passed: ${diffSet.mkString(", ")}", configMissingException)
      Right(configMissingException)
    }

  }

}
