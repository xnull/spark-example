package bynull.spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by null on 19.02.17.
  */
class DataManager(spark: SparkSession, inputDirectory: String, val outputDirectory: String) {

  def loadCsv(fileName: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(s"$inputDirectory/$fileName")
  }
}
