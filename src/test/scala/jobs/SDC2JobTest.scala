package jobs

import testSpec.TestSpecWithFunSuite

class SDC2JobTest extends TestSpecWithFunSuite {

  test("test 1") {
    val inDF = spark.range(5)
    val outDF = spark.range(5)

    assert(inDF.collect().sorted.deep == outDF.collect().sorted.deep)
  }

}
