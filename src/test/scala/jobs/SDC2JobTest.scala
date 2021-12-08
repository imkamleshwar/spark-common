package jobs

import com.typesafe.config.ConfigFactory
import testSpec.TestSpecWithFunSuite

class SDC2JobTest extends TestSpecWithFunSuite {

  test("test 1") {
    val inDF = spark.range(5)
    val outDF = spark.range(5)

    assert(inDF.collect().sorted.deep == outDF.collect().sorted.deep)
  }

  test("config test") {
    val config = ConfigFactory.load("spark.conf")
    assert(config.getString("spark.appName") == "Unit Test Spark Job")
  }

}
