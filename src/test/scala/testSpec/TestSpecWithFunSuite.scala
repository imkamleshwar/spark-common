package testSpec

import com.typesafe.config.Config
import common.SparkInitializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

trait TestSpecWithFunSuite extends AnyFunSuite with SparkInitializer {

  val config: Config = getConfig("spark.conf")
  val sparkConf: SparkConf = getSparkConf(config)
  val spark: SparkSession = initializeSpark(sparkConf)

  setSparkLoggingLevel(spark)

}
