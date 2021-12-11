package common

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import constants.ConfigConstants.MANDATORY_CONFIGS
import org.apache.spark.sql.SparkSession
import utils.ConfigUtils.{addSparkArgsToConfig, checkMandatoryConfig}
import utils.SparkInitializationUtils.argumentParser

import java.util.Date

trait JobHelper extends SparkInitializer with LazyLogging {

  def main(args: Array[String]): Unit = {
    val sparkSubmitArgs = argumentParser(args.mkString)
    val initialConf = getConfig(sparkSubmitArgs.getOrElse("jobConfigs", "spark.conf"))
    val config = addSparkArgsToConfig(initialConf, sparkSubmitArgs)
    checkMandatoryConfig(MANDATORY_CONFIGS, config) match {
      case Left(_) =>
        logger.info("All mandatory configs are present")

      case Right(configMissingException) =>
        throw configMissingException
    }
    
    val sparkConf = getSparkConf(config)
    val spark = initializeSpark(sparkConf)

    setSparkLoggingLevel(spark, sparkSubmitArgs.getOrElse("logLevel", "WARN"))

    logger.info("=====>>>>> Arguments passed:" + args.mkString("\n"))
    logger.info("=====>>>>> Parsed Argument:" + sparkSubmitArgs.mkString("\n"))
    logger.info(s"=====>>>>> Spark Session initialised successfully at: ${new Date(spark.sparkContext.startTime)}")

    execute(spark, config)

    spark.stop()
    logger.info(s"=====>>>>> Spark Session stopped at: ${new Date(System.currentTimeMillis())}")
  }

  def execute(spark: SparkSession, config: Config): Unit

}
