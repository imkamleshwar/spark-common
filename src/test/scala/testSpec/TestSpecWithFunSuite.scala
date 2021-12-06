package testSpec

import common.SparkInitializer
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

trait TestSpecWithFunSuite extends AnyFunSuite with SparkInitializer {

  val spark: SparkSession = initializeSpark(Map())
  setSparkLoggingLevel(spark)

}
