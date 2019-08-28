package com.jasonxu

import org.apache.spark.sql.SparkSession

object SparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Test")
      .getOrCreate()

    println("Loading dataframe from csv")
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("employee.csv")

    df.printSchema()

    df.show()

    spark.stop()
  }
}
