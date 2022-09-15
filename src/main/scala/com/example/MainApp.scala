package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MainApp extends App {
    final val READ_FILE_PATH = "gs://qori-hadoop-test/film.csv"
    final val WRITE_FILE_PATH = "gs://qori-hadoop-test/processed_films"

    val spark = SparkSession
        .builder()
        .appName("gcs_test")
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    var df = spark.read
        .option("header", true)
        .option("delimiter", ";")
        .csv(READ_FILE_PATH)

    df.show(truncate = false, numRows = 20)

    df = df
        .withColumn("actor_split", split(col("Actor"), ","))
        .withColumn("actress_split", split(col("Actress"), ","))
        .withColumn("director_split", split(col("Director"), ","))

    df = df
        .withColumn("Actor", concat(col("actor_split")(1), lit(" "), col("actor_split")(0)))
        .withColumn("Actress", concat(col("actress_split")(1), lit(" "), col("actress_split")(0)))
        .withColumn("Director", concat(col("director_split")(1), lit(" "), col("director_split")(0)))
        .drop("*Image", "actor_split", "actress_split", "director_split")
        .na.drop()
        .orderBy("Actor")

    df.show(truncate = false, numRows = 20)

    df
        .coalesce(1)
        .write
        .option("header", true)
        .option("delimiter", "|")
        .mode("overwrite")
        .csv(WRITE_FILE_PATH)
}
