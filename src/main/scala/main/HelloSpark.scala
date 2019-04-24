package main

import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()
    println("Hello Spark")

    //Reading text file
    val textFile = spark.read.text("/Users/varshapa/Downloads/sampletext.txt")
    textFile.show(false)
    println("SCHEMA: ")
    textFile.printSchema()
    println("COLUMNS: ")
    println(textFile.columns)

    //Reading csv file
    spark.stop()
  }

}
