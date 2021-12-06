package common

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

trait SparkInitializer extends LazyLogging {

  def initializeSpark(initMapping: Map[String, String]): SparkSession = {

    SparkSession
      .builder()
      .master(initMapping.getOrElse("master", "local[2]"))
      .appName(initMapping.getOrElse("appName", "Spark Job"))
      .enableHiveSupport()
      .getOrCreate()
  }

  def setSparkLoggingLevel(spark: SparkSession, logLevel: String = "WARN"): Unit = {
    spark.sparkContext.setLogLevel(logLevel)
  }

}
