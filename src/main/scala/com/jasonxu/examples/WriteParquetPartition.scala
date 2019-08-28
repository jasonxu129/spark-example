package com.jasonxu.examples

import org.apache.spark.sql.SparkSession

object WriteParquetPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateParquetPartition")
      .enableHiveSupport()
      .getOrCreate()

    println("Loading dataframe from csv")
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("employee.csv")

    df.createOrReplaceTempView("sales_people")

    println("Print dataframe")
    df.show()

    // List tables and query one table.
    spark.catalog.listTables().show()

    spark.sql("select * from employee").show()


    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS spark_db.people (firstName String, lastName String, country String, age Int, dept String)
         | USING PARQUET PARTITIONED BY (dept)
         |""".stripMargin
    )

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE spark_db.people PARTITION (dept='sales')
         | SELECT * from sales_people
         |""".stripMargin
    )

    spark.sql(
      s"""
         |SELECT * from spark_db.people where dept='sales'
         |""".stripMargin
    ).show()


    spark.stop()
  }
}
