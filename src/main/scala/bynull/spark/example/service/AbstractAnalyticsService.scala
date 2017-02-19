package bynull.spark.example.service

import bynull.spark.example.DataManager
import org.apache.spark.sql.SparkSession

/**
  * Created by null on 19.02.17.
  */
abstract class AbstractAnalyticsService(protected val dataManager: DataManager, protected val spark: SparkSession)
