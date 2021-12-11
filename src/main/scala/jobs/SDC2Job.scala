package jobs

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import common.JobHelper
import org.apache.spark.sql.SparkSession
import sdc.SCD2Impl

object SDC2Job extends JobHelper with LazyLogging {

  override def execute(spark: SparkSession, config: Config): Unit = {
    //    val configIterator = config.entrySet().iterator()
    //    while (configIterator.hasNext) {
    //      println(configIterator.next())
    //    }
    SCD2Impl(spark)
  }
}
