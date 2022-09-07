package com.example

import org.apache.spark.sql.SparkSession

object MainApp extends App {
    final val READ_FILE_PATH = "gs://qori-hadoop-test/film.csv"

    val spark = SparkSession
        .builder()
        .appName("gcs_test")
        .getOrCreate()

    val df = spark.read
        .option("header", true)
        .option("delimiter", ";")
        .csv(READ_FILE_PATH)

    df.show(truncate = false, numRows = 20)
}
