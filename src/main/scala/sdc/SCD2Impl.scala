package sdc

import models.{Customer, CustomerUpdate}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date

object SCD2Impl {

  def apply(spark: SparkSession): DataFrame = {

    import spark.implicits._

    val customers = Seq(
      Customer(1, "Denver", false, null, Date.valueOf("2018-02-01"))
      , Customer(1, "New York", true, Date.valueOf("2018-02-01"), null)
      , Customer(2, "Budapest", true, Date.valueOf("2018-02-01"), null)
      , Customer(3, "London", true, Date.valueOf("2018-02-01"), null)
    ).toDF()

    val customerUpdates = Seq(
      CustomerUpdate(1, "San Francisco", Date.valueOf("2020-01-20"))
      , CustomerUpdate(3, "Landon", Date.valueOf("2018-02-01"))
      , CustomerUpdate(4, "Berlin", Date.valueOf("2020-01-20"))
    ).toDF()

    customers.show(false)
    customers.printSchema()

    customerUpdates.show(false)
    customerUpdates.printSchema()

    val df = customers
      .join(customerUpdates
        .withColumnRenamed("address", "newAddress")
        .withColumnRenamed("effectiveDate", "newEffectiveDate")
        .as("updates"), Seq("customerId"), "left")
      .filter(col("address").isNotNull)

    df.show(false)

    val endDateUpdated = df
      .withColumn("endDate",
        when(col("current") === true
          && col("newEffectiveDate").isNotNull
          && col("newEffectiveDate") =!= col("effectiveDate"), col("newEffectiveDate"))
          .otherwise(col("endDate")))
      .withColumn("current",
        when(col("current") === true
          && col("newEffectiveDate").isNotNull
          && col("newEffectiveDate") =!= col("effectiveDate"), lit(false))
          .otherwise(col("current")))
      .select(customers.columns.map(col): _*)

    endDateUpdated
      .show(false)

    val updateCustomers = df
      .filter(col("address") =!= col("newAddress")
        && col("current") === true
        && col("newAddress") =!= col("address")
        && col("newEffectiveDate") =!= col("effectiveDate"))
      .drop("effectiveDate", "address")
      .withColumnRenamed("newAddress", "address")
      .withColumnRenamed("newEffectiveDate", "effectiveDate")
      .unionByName(endDateUpdated)
      .select(endDateUpdated.columns.map(col): _*)

    updateCustomers.show(false)

    val finalDF = customerUpdates.select("customerId")
      .except(customers.select("customerId"))
      .distinct()
      .join(customerUpdates, Seq("customerId"))
      .withColumn("current", lit(true))
      .withColumn("endDate", lit(null))
      .unionByName(updateCustomers)
      .sort("customerId", "effectiveDate")

    finalDF.show(false)

    finalDF

  }

}
