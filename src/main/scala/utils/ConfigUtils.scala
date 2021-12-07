package utils

import com.typesafe.config.{Config, ConfigValueFactory}

object ConfigUtils {

  def addSparkArgsToConfig(config: Config, sparkArgsMap: Map[String, String]): Config = {
    sparkArgsMap
      .foldLeft(config) { case (conf, (key, value)) => conf.withValue(key, ConfigValueFactory.fromAnyRef(value)) }
  }

}
