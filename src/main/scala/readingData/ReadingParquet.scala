package readingData

import org.apache.spark.sql.SparkSession

class ReadingParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()
    
    val parquetFile = spark.read.parquet("/Users/varshapa/Downloads/parquetFile.parquet")
    parquetFile.show()
  }

}
