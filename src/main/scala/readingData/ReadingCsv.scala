package readingData

import org.apache.spark.sql.SparkSession

class ReadingCsv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Reading csv App").getOrCreate()

    val csvFile = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/Users/varshapa/Downloads/samplecsv.csv")
    csvFile.show()

    csvFile.printSchema()

  }
}
