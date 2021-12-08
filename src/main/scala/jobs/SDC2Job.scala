package jobs

import com.typesafe.scalalogging.LazyLogging
import common.JobHelper
import org.apache.spark.sql.SparkSession
import sdc.SCD2Impl

object SDC2Job extends JobHelper with LazyLogging {

  override def execute(spark: SparkSession): Unit = {
    SCD2Impl(spark)
  }
}
