package bynull.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, _}

/**
  * Created by null on 18.02.17.
  */
object SparkApplication {

  def apply(master: String): SparkApplication = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark application")
      .master(master)
      .getOrCreate()

    new SparkApplication(
      spark,
      new StreamingContext(spark.sparkContext, Seconds(1))
    )
  }
}

class SparkApplication(spark: SparkSession, ssc: StreamingContext) {

  def run(task: SparkSession => Unit): Unit = task(spark)

  def runStreaming(task: StreamingContext => Unit): Unit = task(ssc)

  def stop(): Unit = spark.stop()
}
