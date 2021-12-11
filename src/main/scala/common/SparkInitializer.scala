package common

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkInitializer extends LazyLogging {

  def initializeSpark(sparkConf: SparkConf): SparkSession = {

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    setSparkLoggingLevel(spark)

    spark
  }

  def setSparkLoggingLevel(spark: SparkSession, logLevel: String = "WARN"): Unit = {
    spark.sparkContext.setLogLevel(logLevel)
    logger.info(s"Spark Log Level is set to : $logLevel")
  }

  def getSparkConf(config: Config): SparkConf = {
    new SparkConf()
      .setMaster(config.getString("spark.master"))
      .setAppName(config.getString("spark.appName"))
  }

  def getConfig(jobConfigs: String): Config = ConfigFactory.load(jobConfigs)

}
