package common

import org.apache.spark.sql.SparkSession
import utils.SparkInitializationUtils.argumentParser

import java.util.Date

trait JobHelper extends SparkInitializer {

  def main(args: Array[String]): Unit = {
    val sparkSubmitArgs = argumentParser(args.mkString)
    val spark = initializeSpark(sparkSubmitArgs)

    setSparkLoggingLevel(spark, sparkSubmitArgs.getOrElse("logLevel", "WARN"))

    logger.info("=====>>>>> Arguments passed:" + args.mkString("\n"))
    logger.info("=====>>>>> Parsed Argument:" + sparkSubmitArgs.mkString("\n"))
    logger.info(s"=====>>>>> Spark Session initialised successfully at: ${new Date(spark.sparkContext.startTime)}")

    execute(spark)

    spark.stop()
    logger.info(s"=====>>>>> Spark Session stopped at: ${new Date(System.currentTimeMillis())}")
  }


  def execute(spark: SparkSession): Unit

}
