package testSpec

import com.typesafe.config.Config
import common.SparkInitializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait TestSpecWithFunSuite extends AnyFunSuite with SparkInitializer with Matchers {

  lazy val config: Config = getConfig("spark.conf")
  lazy val sparkConf: SparkConf = getSparkConf(config)
  lazy val spark: SparkSession = initializeSpark(sparkConf)

}
