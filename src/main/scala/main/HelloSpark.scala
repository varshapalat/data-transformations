package main

import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()
    println("Hello Spark")

    //Reading text file
    val textFile = spark.read.text("/Users/varshapa/Downloads/sampletext.txt")

    //Showing full file
    //    textFile.show(textFile.count().toInt, false)
    textFile.show()

    println("SCHEMA: ")
    textFile.printSchema()
    println("COLUMNS: ")
    val columns = textFile.columns
    println(columns)


    //Reading csv file
    val csvFile = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/Users/varshapa/Downloads/samplecsv.csv")
    csvFile.show()

    csvFile.printSchema()

    //Reading Parquet file
    val parquetFile = spark.read.parquet("/Users/varshapa/Downloads/parquetFile.parquet")
    parquetFile.show()

    spark.stop()
  }

}
