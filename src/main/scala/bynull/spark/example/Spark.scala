package bynull.spark.example

import java.io.File

import bynull.spark.example.service.BankAnalyticsService
import org.apache.commons.io.FileUtils

/**
  * Created by null on 18.02.17.
  */
object Spark extends App {
  val inputDirectory = "src/main/resources"
  val outputDirectory = "./data"

  FileUtils.deleteQuietly(new File(outputDirectory))

  val app = SparkApplication(master = "local[*]")

  try {
    app.run { spark =>
      val dataLoader = new DataManager(spark, inputDirectory, outputDirectory)
      val bankAnalytics = BankAnalyticsService(dataLoader, spark)

      bankAnalytics.accountsYoungerThanThirty()
      bankAnalytics.topAccounts()
      bankAnalytics.accountsHigherThanAverage()
      bankAnalytics.richestAccountsByJob()
    }
  } finally {
    app.stop()
  }
}