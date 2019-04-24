package readingData

import org.apache.spark.sql.SparkSession

class ReadingText {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Reading text App").getOrCreate()

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
  }
}
